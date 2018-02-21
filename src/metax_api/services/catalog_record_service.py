import logging
from collections import defaultdict
from os.path import dirname, join

import simplexquery as sxq
from dicttoxml import dicttoxml
from django.db import connection
from rest_framework import status
from rest_framework.serializers import ValidationError

from metax_api.exceptions import Http400, Http403, Http503
from metax_api.models import Contract, Directory, File
from metax_api.utils import RabbitMQ
from .common_service import CommonService
from .reference_data_mixin import ReferenceDataMixin

_logger = logging.getLogger(__name__)
d = logging.getLogger(__name__).debug


# avoiding circular imports
def DirectorySerializer(*args, **kwargs):
    from metax_api.api.rest.base.serializers import DirectorySerializer as DS
    DirectorySerializer = DS
    return DirectorySerializer(*args, **kwargs)

def FileSerializer(*args, **kwargs):
    from metax_api.api.rest.base.serializers import FileSerializer as FS
    FileSerializer = FS
    return FileSerializer(*args, **kwargs)


class CatalogRecordService(CommonService, ReferenceDataMixin):

    @staticmethod
    def get_queryset_search_params(request):
        """
        Get and validate parameters from request.query_params that will be used for filtering
        in view.get_queryset()
        """

        if not request.query_params:
            return {}

        queryset_search_params = {}

        if request.query_params.get('state', False):
            state_vals = request.query_params['state'].split(',')
            for val in state_vals:
                try:
                    int(val)
                except ValueError:
                    raise Http400({ 'state': ['Value \'%s\' is not an integer' % val] })
            queryset_search_params['preservation_state__in'] = state_vals

        if CommonService.get_boolean_query_param(request, 'latest'):
            queryset_search_params['next_version_id'] = None

        if request.query_params.get('curator', False):
            queryset_search_params['research_dataset__contains'] = \
                {'curator': [{ 'identifier': request.query_params['curator']}]}

        if request.query_params.get('owner_id', False):
            queryset_search_params['editor__contains'] = { 'owner_id': request.query_params['owner_id'] }

        if request.query_params.get('user_created', False):
            queryset_search_params['user_created'] = request.query_params['user_created']

        return queryset_search_params

    @staticmethod
    def populate_file_details(catalog_record):
        """
        Populate individual research_dataset.file and directory objects with their
        corresponding objects from their db tables.

        Additionally, for directories, include two other calculated fields:
        - byte_size, for total size of files
        - file_count, for total number of files

        Note: Some of these results may be very useful to cache, or cache the entire dataset
        if feasible.
        """
        rd = catalog_record['research_dataset']

        file_identifiers = [ f['identifier'] for f in rd.get('files', [])]

        for file in File.objects.filter(identifier__in=file_identifiers):
            for f in rd['files']:
                if f['identifier'] == file.identifier:
                    f['details'] = FileSerializer(file).data

        dir_identifiers = [ dr['identifier'] for dr in rd.get('directories', []) ]

        for directory in Directory.objects.filter(identifier__in=dir_identifiers):
            for dr in rd['directories']:
                if dr['identifier'] == directory.identifier:
                    dr['details'] = DirectorySerializer(directory).data

        if not dir_identifiers:
            return

        sql = '''
            select sum(f.byte_size) as byte_size, count(*) as file_count
            from metax_api_file f
            inner join metax_api_catalogrecord_files cr_f on cr_f.file_id = f.id
            where cr_f.catalogrecord_id = %s
            and f.project_identifier = %s
            and f.file_path like (%s || '%%')
            and f.active = true and f.removed = false
        '''

        with connection.cursor() as cr:
            for dr in rd['directories']:
                cr.execute(
                    sql,
                    [
                        catalog_record['id'],
                        dr['details']['project_identifier'],
                        dr['details']['directory_path']
                    ]
                )
                for row in cr.fetchall():
                    dr['details']['byte_size'] = row[0]
                    dr['details']['file_count'] = row[1]

    @staticmethod
    def propose_to_pas(request, catalog_record):
        """
        Set catalog record status to 'proposed to pas <midterm or longterm>'
        """

        if not request.query_params.get('state', False):
            raise Http400({ 'state': ['Query parameter \'state\' is a required parameter.'] })

        if request.query_params.get('state') not in ('1', '2'):
            raise Http400({ 'state': ['Query parameter \'state\' value must be 1 or 2.'] })

        if not request.query_params.get('contract', False):
            raise Http400({ 'contract': ['Query parameter \'contract\' is a required parameter.'] })

        if not catalog_record.can_be_proposed_to_pas():
            raise Http403({ 'preservation_state': ['Value must be 0 (not proposed to PAS), 7 (longterm PAS rejected), '
                                                   'or 8 (midterm PAS rejected), when proposing to PAS. Current state '
                                                   'is %d.' % catalog_record.preservation_state]})

        try:
            contract = Contract.objects.get(contract_json__identifier=request.query_params.get('contract'))
        except Contract.DoesNotExist:
            raise Http400({ 'contract': ['Contract not found']})

        catalog_record.preservation_state = request.query_params.get('state')
        catalog_record.save()
        contract.records.add(catalog_record)
        contract.save()

    @classmethod
    def publish_updated_datasets(cls, response):
        """
        Publish updated datasets to RabbitMQ.

        If the update operation resulted in a new dataset version being created,
        publish those new versions as well.
        """
        if response.status_code != status.HTTP_200_OK:
            return

        next_versions = []

        if 'success' in response.data:
            updated_request_data = [ r['object'] for r in response.data['success'] ]
            for cr in updated_request_data:
                if cls._new_version_created(cr):
                    next_versions.append(cls._extract_next_version_data(cr))
        else:
            updated_request_data = response.data
            if cls._new_version_created(updated_request_data):
                next_versions.append(cls._extract_next_version_data(updated_request_data))

        count = len(updated_request_data) if isinstance(updated_request_data, list) else 1
        _logger.info('Publishing updated datasets (%d items)' % count)

        try:
            rabbitmq = RabbitMQ()
            rabbitmq.publish(updated_request_data, routing_key='update', exchange='datasets')

            if next_versions:
                _logger.info('Publishing new dataset versions (%d items)' % len(next_versions))
                rabbitmq.publish(next_versions, routing_key='create', exchange='datasets')
        except Exception as e:
            _logger.exception('Publishing rabbitmq messages failed')
            raise Http503({ 'detail': [
                'failed to publish updates to rabbitmq, all updates are aborted. details: %s' % str(e)
            ]})
        _logger.info('RabbitMQ dataset messages published')

    @staticmethod
    def _new_version_created(cr):
        return '__actions' in cr and 'publish_next_version' in cr['__actions']

    @staticmethod
    def _extract_next_version_data(cr):
        data = cr['__actions']['publish_next_version']['next_version']
        cr.pop('__actions')
        return data

    @staticmethod
    def transform_datasets_to_format(catalog_records, target_format, include_xml_declaration=True):
        """
        params:
        catalog_records: a list of catalog record dicts, or a single dict
        """

        def item_func(parent_name):
            """
            Enable using other element names than 'item', depending on parent element name
            However, since many one2many relation element names are already in singular form,
            coming up with nice singular element names for childre is difficult.
            """
            return {
                'researchdatasets': 'researchdataset'
            }.get(parent_name, 'item')

        if isinstance(catalog_records, dict):
            is_list = False
            content_to_transform = catalog_records['research_dataset']
        else:
            is_list = True
            content_to_transform = (cr['research_dataset'] for cr in catalog_records)

        xml_str = dicttoxml(
            content_to_transform,
            custom_root='researchdatasets' if is_list else 'researchdataset',
            attr_type=False,
            item_func=item_func
        ).decode('utf-8')
        # This is a bit ugly way to put the metax data to the datacite namespace,
        # which allows us to use the default namespace in xquery files.
        xml_str = xml_str.replace('<researchdataset>',
            '<researchdataset xmlns="http://uri.suomi.fi/datamodel/ns/mrd#">')
        if target_format == 'metax':
            # mostly for debugging purposes, the 'metax xml' can be returned as well
            return xml_str

        target_xslt_file_path = join(dirname(dirname(__file__)), 'api/rest/base/xslt/%s.xslt' % target_format)

        try:
            with open(target_xslt_file_path) as f:
                xslt = f.read()
        except OSError:
            raise Http400('Requested format \'%s\' is not available' % target_format)

        try:
            transformed_xml = sxq.execute(xslt, xml_str)
        except:
            _logger.exception('Something is wrong with the xslt file at %s:' % target_xslt_file_path)
            raise Http503('Requested format \'%s\' is currently unavailable' % target_format)

        if include_xml_declaration:
            return '<?xml version="1.0" encoding="UTF-8" ?>%s' % transformed_xml
        else:
            return transformed_xml

    @classmethod
    def validate_reference_data(cls, research_dataset, cache):
        """
        Validate certain fields from the received dataset against reference data, which contains
        the allowed values for these fields.

        If a field value is valid, some of the object's fields will also be populated from the cached
        reference data, overwriting possible values already entered. The fields that will be populated
        from the reference data are:

        - uri (usually to object's field 'identifier')
        - label (usually to object's field 'pref_label')

        """
        reference_data = cls.get_reference_data(cache)
        refdata = reference_data['reference_data']
        orgdata = reference_data['organization_data']['organization']
        errors = defaultdict(list)

        for theme in research_dataset.get('theme', []):
            ref_entry = cls.check_ref_data(refdata['keyword'], theme['identifier'],
                                           'research_dataset.theme.identifier', errors)
            if ref_entry:
                cls.populate_from_ref_data(ref_entry, theme, label_field='pref_label')

        for fos in research_dataset.get('field_of_science', []):
            ref_entry = cls.check_ref_data(refdata['field_of_science'], fos['identifier'],
                                           'research_dataset.field_of_science.identifier', errors)
            if ref_entry:
                cls.populate_from_ref_data(ref_entry, fos, label_field='pref_label')

        for remote_resource in research_dataset.get('remote_resources', []):

            # Since checksum is a plain string field, it should not be changed to a reference data uri in case
            # it is recognized as one of the checksum_algorithm reference data items.
            # TODO: Find out whether a non-reference data value for the checksum_algorithm should even throw an error
            if 'checksum' in remote_resource:
                cls.check_ref_data(refdata['checksum_algorithm'], remote_resource['checksum']['algorithm'],
                                   'research_dataset.remote_resources.checksum.algorithm', errors)

            for license in remote_resource.get('license', []):
                ref_entry = cls.check_ref_data(refdata['license'], license['identifier'],
                                               'research_dataset.remote_resources.license.identifier', errors)
                if ref_entry:
                    cls.populate_from_ref_data(ref_entry, license, label_field='title')

            if remote_resource.get('resource_type', False):
                ref_entry = cls.check_ref_data(refdata['resource_type'], remote_resource['resource_type']['identifier'],
                                               'research_dataset.remote_resources.resource_type.identifier', errors)
                if ref_entry:
                    cls.populate_from_ref_data(ref_entry, remote_resource['resource_type'], label_field='pref_label')

            if remote_resource.get('file_type', False):
                ref_entry = cls.check_ref_data(refdata['file_type'], remote_resource['file_type']['identifier'],
                                               'research_dataset.remote_resources.file_type.identifier', errors)
                if ref_entry:
                    cls.populate_from_ref_data(ref_entry, remote_resource['file_type'], label_field='pref_label')

            if remote_resource.get('use_category', False):
                ref_entry = cls.check_ref_data(refdata['use_category'], remote_resource['use_category']['identifier'],
                                               'research_dataset.remote_resources.use_category.identifier', errors)
                if ref_entry:
                    cls.populate_from_ref_data(ref_entry, remote_resource['use_category'], label_field='pref_label')

        for language in research_dataset.get('language', []):
            ref_entry = cls.check_ref_data(refdata['language'], language['identifier'],
                                           'research_dataset.language.identifier', errors)
            if ref_entry:
                label_field = 'title'
                cls.populate_from_ref_data(ref_entry, language, label_field=label_field)
                cls.remove_language_obj_irrelevant_titles(language, label_field)

        access_rights = research_dataset.get('access_rights', None)
        if access_rights:
            if 'access_type' in access_rights:
                ref_entry = cls.check_ref_data(refdata['access_type'], access_rights['access_type']['identifier'],
                                               'research_dataset.access_rights.access_type.identifier', errors)
                if ref_entry:
                    cls.populate_from_ref_data(ref_entry, access_rights['access_type'], label_field='pref_label')

            if 'restriction_grounds' in access_rights:
                ref_entry = cls.check_ref_data(refdata['restriction_grounds'],
                                               access_rights['restriction_grounds']['identifier'],
                                               'research_dataset.access_rights.restriction_grounds.identifier', errors)
                if ref_entry:
                    cls.populate_from_ref_data(ref_entry, access_rights['restriction_grounds'],
                                               label_field='pref_label')

            for rights_statement_license in access_rights.get('license', []):
                ref_entry = cls.check_ref_data(refdata['license'], rights_statement_license['identifier'],
                                               'research_dataset.access_rights.license.identifier', errors)
                if ref_entry:
                    cls.populate_from_ref_data(ref_entry, rights_statement_license, label_field='title')

            for rra in access_rights.get('has_rights_related_agent', []):
                cls.process_research_agent_obj_with_type(orgdata, refdata, errors, rra,
                                                         'research_dataset.access_rights.has_rights_related_agent')

        for project in research_dataset.get('is_output_of', []):
            for org_obj in project.get('source_organization', []):
                cls.process_org_obj_against_ref_data(orgdata, org_obj,
                                                     'research_dataset.is_output_of.source_organization')

            for org_obj in project.get('has_funding_agency', []):
                cls.process_org_obj_against_ref_data(orgdata, org_obj,
                                                     'research_dataset.is_output_of.has_funding_agency')

            if project.get('funder_type', False):
                ref_entry = cls.check_ref_data(refdata['funder_type'], project['funder_type']['identifier'],
                                               'research_dataset.is_output_of.funder_type.identifier', errors)
                if ref_entry:
                    cls.populate_from_ref_data(ref_entry, project['funder_type'], label_field='pref_label')

        for other_identifier in research_dataset.get('other_identifier', []):
            if other_identifier.get('type', False):
                ref_entry = cls.check_ref_data(refdata['identifier_type'], other_identifier['type']['identifier'],
                                               'research_dataset.other_identifier.type.identifier', errors)
                if ref_entry:
                    cls.populate_from_ref_data(ref_entry, other_identifier['type'], label_field='pref_label')

            if other_identifier.get('provider', False):
                cls.process_org_obj_against_ref_data(orgdata, other_identifier['provider'],
                                                     'research_dataset.other_identifier.provider')

        for spatial in research_dataset.get('spatial', []):
            as_wkt = spatial.get('as_wkt', [])

            if spatial.get('place_uri', False):
                place_uri = spatial.get('place_uri')
                ref_entry = cls.check_ref_data(refdata['location'], place_uri['identifier'],
                                               'research_dataset.spatial.place_uri.identifier', errors)
                if ref_entry:
                    cls.populate_from_ref_data(ref_entry, place_uri, label_field='pref_label')

                    # Populate as_wkt field from reference data only if it is empty, i.e. not provided by the user
                    # and when the coordinates are available in the reference data
                    if len(as_wkt) == 0 and ref_entry.get('wkt', False):
                        as_wkt.append(ref_entry.get('wkt'))

            spatial['as_wkt'] = as_wkt

        for file in research_dataset.get('files', []):
            if file.get('file_type', False):
                ref_entry = cls.check_ref_data(refdata['file_type'], file['file_type']['identifier'],
                                               'research_dataset.files.file_type.identifier', errors)
                if ref_entry:
                    cls.populate_from_ref_data(ref_entry, file['file_type'], label_field='pref_label')

            if file.get('use_category', False):
                ref_entry = cls.check_ref_data(refdata['use_category'], file['use_category']['identifier'],
                                               'research_dataset.files.use_category.identifier', errors)
                if ref_entry:
                    cls.populate_from_ref_data(ref_entry, file['use_category'], label_field='pref_label')

        for directory in research_dataset.get('directories', []):
            if directory.get('use_category', False):
                ref_entry = cls.check_ref_data(refdata['use_category'], directory['use_category']['identifier'],
                                               'research_dataset.directories.use_category.identifier', errors)
                if ref_entry:
                    cls.populate_from_ref_data(ref_entry, directory['use_category'], label_field='pref_label')

        for contributor in research_dataset.get('contributor', []):
            cls.process_research_agent_obj_with_type(orgdata, refdata, errors, contributor,
                                                     'research_dataset.contributor')

        if research_dataset.get('publisher', False):
            cls.process_research_agent_obj_with_type(orgdata, refdata, errors, research_dataset['publisher'],
                                                     'research_dataset.publisher')

        for curator in research_dataset.get('curator', []):
            cls.process_research_agent_obj_with_type(orgdata, refdata, errors, curator, 'research_dataset.curator')

        for creator in research_dataset.get('creator', []):
            cls.process_research_agent_obj_with_type(orgdata, refdata, errors, creator, 'research_dataset.creator')

        if research_dataset.get('rights_holder', False):
            cls.process_research_agent_obj_with_type(orgdata, refdata, errors, research_dataset['rights_holder'],
                                                     'research_dataset.rights_holder')

        for activity in research_dataset.get('provenance', []):
            for was_associated_with in activity.get('was_associated_with', []):
                cls.process_research_agent_obj_with_type(orgdata, refdata, errors, was_associated_with,
                                                         'research_dataset.provenance.was_associated_with')

            if activity.get('spatial', False):
                spatial = activity['spatial']
                as_wkt = spatial.get('as_wkt', [])

                if spatial.get('place_uri', False):
                    place_uri = spatial.get('place_uri')
                    ref_entry = cls.check_ref_data(refdata['location'], place_uri['identifier'],
                                                   'research_dataset.provenance.spatial.place_uri.identifier', errors)
                    if ref_entry:
                        cls.populate_from_ref_data(ref_entry, place_uri, label_field='pref_label')

                        # Populate as_wkt field from reference data only if it is empty, i.e. not provided by the user
                        # and when the coordinates are available in the reference data
                        if len(as_wkt) == 0 and ref_entry.get('wkt', False):
                            as_wkt.append(ref_entry.get('wkt'))

                spatial['as_wkt'] = as_wkt

            if activity.get('type', False):
                ref_entry = cls.check_ref_data(refdata['lifecycle_event'],
                                               activity['type']['identifier'],
                                               'research_dataset.activity.type.identifier',
                                               value_not_found_is_error=False)

                if not ref_entry:
                    ref_entry = cls.check_ref_data(refdata['preservation_event'],
                                                   activity['type']['identifier'],
                                                   'research_dataset.activity.type.identifier', errors)

                if ref_entry:
                    cls.populate_from_ref_data(ref_entry, activity['type'], label_field='pref_label')

        for infra in research_dataset.get('infrastructure', []):
            ref_entry = cls.check_ref_data(refdata['research_infra'], infra['identifier'],
                                           'research_dataset.infrastructure.identifier', errors)
            if ref_entry:
                cls.populate_from_ref_data(ref_entry, infra, label_field='pref_label')

        for relation in research_dataset.get('relation', []):
            if relation.get('relation_type', False):
                ref_entry = cls.check_ref_data(refdata['relation_type'], relation['relation_type']['identifier'],
                                               'research_dataset.relation.relation_type.identifier', errors)
                if ref_entry:
                    cls.populate_from_ref_data(ref_entry, relation['relation_type'], label_field='pref_label')

        if errors:
            raise ValidationError(errors)
