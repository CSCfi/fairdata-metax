import logging
import urllib.parse
from collections import defaultdict
from os.path import dirname, join

import simplexquery as sxq
from dicttoxml import dicttoxml
from django.db.models import Q
from rest_framework import status
from rest_framework.serializers import ValidationError

from metax_api.exceptions import Http400, Http403, Http503
from metax_api.models import Directory, File
from metax_api.utils import RabbitMQ
from .common_service import CommonService
from .file_service import FileService
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

    @classmethod
    def get_queryset_search_params(cls, request):
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
            queryset_search_params['next_dataset_version_id'] = None

        if request.query_params.get('curator', False):
            queryset_search_params['research_dataset__contains'] = \
                {'curator': [{ 'identifier': request.query_params['curator']}]}

        if request.query_params.get('owner_id', False):
            queryset_search_params['editor__contains'] = { 'owner_id': request.query_params['owner_id'] }

        if request.query_params.get('user_created', False):
            queryset_search_params['user_created'] = request.query_params['user_created']

        if request.query_params.get('editor', False):
            queryset_search_params['editor__contains'] = { 'identifier': request.query_params['editor'] }

        if request.query_params.get('contract_org_identifier', False):
            if request.user.username not in ('metax', 'tpas'):
                raise Http403({ 'detail': ['query parameter pas_filter is restricted']})
            queryset_search_params['contract__contract_json__organization__organization_identifier__iregex'] = \
                request.query_params['contract_org_identifier']

        if request.query_params.get('pas_filter', False):
            cls.set_pas_filter(queryset_search_params, request)

        return queryset_search_params

    @staticmethod
    def set_pas_filter(queryset_search_params, request):
        """
        A somewhat specific filter for PAS needs... The below OR query is AND'ed with any
        other possible filters from other query parameters.
        """
        if request.user.username not in ('metax', 'tpas'):
            raise Http403({ 'detail': ['query parameter pas_filter is restricted']})

        search_string = urllib.parse.unquote(request.query_params.get('pas_filter', ''))

        # dataset title, from various languages...
        q1 = Q(research_dataset__title__en__iregex=search_string)
        q2 = Q(research_dataset__title__fi__iregex=search_string)

        # contract...
        q3 = Q(contract__contract_json__title__iregex=search_string)

        # a limitation of jsonb-field queries...
        # unable to use regex search directly (wildcard) since curator is an array... and __contains only
        # matches whole string. cheating here to search from first three indexes using regex. who
        # knows how many curators datasets will actually have, but probably most cases will produce
        # a match with this approach... if not, the user will be more and more accurate and finally
        # type the whole name and get a result while cursing shitty software
        q4 = Q(research_dataset__curator__0__name__iregex=search_string)
        q5 = Q(research_dataset__curator__1__name__iregex=search_string)
        q6 = Q(research_dataset__curator__2__name__iregex=search_string)
        q7 = Q(research_dataset__curator__contains=[{ 'name': search_string }])

        q_filter = q1 | q2 | q3 | q4 | q5 | q6 | q7

        if 'q_filters' in queryset_search_params: # pragma: no cover
            # no usecase yet but leaving comment for future reference... if the need arises to
            # include Q-filters from multiple sources (query params), probably AND them together
            # by appending to list
            queryset_search_params['q_filters'].append(q_filter)
        else:
            queryset_search_params['q_filters'] = [q_filter]

    @staticmethod
    def populate_file_details(catalog_record, request):
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

        directory_fields, file_fields, discard_fields = \
            FileService._get_requested_file_browsing_fields(request, catalog_record['id'])

        for file in File.objects.filter(identifier__in=file_identifiers).only(*file_fields):
            for f in rd['files']:
                if f['identifier'] == file.identifier:
                    f['details'] = FileSerializer(file, only_fields=file_fields).data
                    continue

        dir_identifiers = [ dr['identifier'] for dr in rd.get('directories', []) ]

        for directory in Directory.objects.filter(identifier__in=dir_identifiers).only(*directory_fields):
            for dr in rd['directories']:
                if dr['identifier'] == directory.identifier:
                    dr['details'] = DirectorySerializer(directory, only_fields=directory_fields).data
                    continue

        if not dir_identifiers:
            return

        if not directory_fields or ('byte_size' in directory_fields or 'file_count' in directory_fields):
            # no specific fields requested -> calculate,
            # OR byte_size or file_count among requested fields -> calculate
            for dr in rd['directories']:
                FileService.calculate_directory_byte_sizes_and_file_counts_for_cr(
                    dr['details'], catalog_record['id'], directory_fields=directory_fields,
                    discard_fields=discard_fields)

    @classmethod
    def publish_updated_datasets(cls, response):
        """
        Publish updated datasets to RabbitMQ.

        If the update operation resulted in a new dataset version being created,
        publish those new versions as well.
        """
        if response.status_code != status.HTTP_200_OK:
            return

        new_versions = []

        if 'success' in response.data:
            # bulk update
            updated_request_data = [ r['object'] for r in response.data['success'] ]
            for cr in updated_request_data:
                if cls._new_version_created(cr):
                    new_versions.append(cls._extract_new_version_data(cr))
        else:
            # single update
            updated_request_data = response.data
            if cls._new_version_created(updated_request_data):
                new_versions.append(cls._extract_new_version_data(updated_request_data))

        count = len(updated_request_data) if isinstance(updated_request_data, list) else 1
        _logger.info('Publishing updated datasets (%d items)' % count)

        try:
            rabbitmq = RabbitMQ()
            rabbitmq.publish(updated_request_data, routing_key='update', exchange='datasets')

            if new_versions:
                _logger.info('Publishing new versions (%d items)' % len(new_versions))
                rabbitmq.publish(new_versions, routing_key='create', exchange='datasets')
        except Exception as e:
            _logger.exception('Publishing rabbitmq messages failed')
            raise Http503({ 'detail': [
                'failed to publish updates to rabbitmq, all updates are aborted. details: %s' % str(e)
            ]})
        _logger.info('RabbitMQ dataset messages published')

    @staticmethod
    def _new_version_created(cr):
        return '__actions' in cr and 'publish_new_version' in cr['__actions']

    @staticmethod
    def _extract_new_version_data(cr):
        data = cr['__actions']['publish_new_version']['dataset']
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
                license_url = license.get('license', None)

                ref_entry = cls.check_ref_data(refdata['license'], license['identifier'],
                                               'research_dataset.remote_resources.license.identifier', errors)
                if ref_entry:
                    cls.populate_from_ref_data(ref_entry, license, label_field='title')

                    # Populate license field from reference data only if it is empty, i.e. not provided by the user
                    # and when the reference data uri does not contain purl.org/att
                    if not license_url and ref_entry.get('uri', False):
                        license_url = ref_entry['uri'] if 'purl.org/att' not in ref_entry['uri'] else None

                if license_url:
                    license['license'] = license_url

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

            for license in access_rights.get('license', []):
                license_url = license.get('license', None)

                ref_entry = cls.check_ref_data(refdata['license'], license['identifier'],
                                               'research_dataset.access_rights.license.identifier', errors)
                if ref_entry:
                    cls.populate_from_ref_data(ref_entry, license, label_field='title')

                    # Populate license field from reference data only if it is empty, i.e. not provided by the user
                    # and when the reference data uri does not contain purl.org/att
                    if not license_url and ref_entry.get('uri', False):
                        license_url = ref_entry['uri'] if 'purl.org/att' not in ref_entry['uri'] else None

                if license_url:
                    license['license'] = license_url

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
                    if not as_wkt and ref_entry.get('wkt', False):
                        as_wkt.append(ref_entry.get('wkt'))

            if as_wkt:
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

        for rights_holder in research_dataset.get('rights_holder', []):
            cls.process_research_agent_obj_with_type(orgdata, refdata, errors, rights_holder,
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
                        if not as_wkt and ref_entry.get('wkt', False):
                            as_wkt.append(ref_entry.get('wkt'))

                if as_wkt:
                    spatial['as_wkt'] = as_wkt

            if activity.get('lifecycle_event', False):
                ref_entry = cls.check_ref_data(refdata['lifecycle_event'],
                                               activity['lifecycle_event']['identifier'],
                                               'research_dataset.provenance.lifecycle_event.identifier', errors)

                if ref_entry:
                    cls.populate_from_ref_data(ref_entry, activity['lifecycle_event'], label_field='pref_label')

            if activity.get('preservation_event', False):
                ref_entry = cls.check_ref_data(refdata['preservation_event'],
                                               activity['preservation_event']['identifier'],
                                               'research_dataset.provenance.preservation_event.identifier', errors)

                if ref_entry:
                    cls.populate_from_ref_data(ref_entry, activity['preservation_event'], label_field='pref_label')

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
