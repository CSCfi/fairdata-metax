from collections import defaultdict
from copy import deepcopy
from datetime import datetime
from os.path import dirname, join

import simplexquery as sxq
from dicttoxml import dicttoxml
from rest_framework import status
from rest_framework.serializers import ValidationError

from metax_api.exceptions import Http400, Http403, Http503
from metax_api.models import CatalogRecord, Contract
from .common_service import CommonService
from .reference_data_mixin import ReferenceDataMixin

import logging
_logger = logging.getLogger(__name__)
d = logging.getLogger(__name__).debug

class CatalogRecordService(CommonService, ReferenceDataMixin):

    @staticmethod
    def create_new_dataset_version(request, catalog_record_current, **kwargs):
        """
        Note: no tests yet before further spekking
        """

        if catalog_record_current.next_version_identifier:
            raise Http403({ 'next_version_identifier': ['A newer version already exists. You can not create new versions from archived versions.'] })

        # import here instead of beginning of the file to avoid circular import in CR serializer
        from metax_api.api.base.serializers import CatalogRecordSerializer

        serializer_current = CatalogRecordSerializer(catalog_record_current, **kwargs)
        current_time = datetime.now()

        catalog_record_new = deepcopy(serializer_current.data)
        catalog_record_new.pop('id', None)
        catalog_record_new.pop('modified_by_api', None)
        catalog_record_new.pop('modified_by_user_id', None)
        catalog_record_new['identifier'] = 'urn:nice:generated:identifier' # TODO
        catalog_record_new['research_dataset']['identifier'] = 'urn:nice:generated:identifier' # TODO
        catalog_record_new['research_dataset']['preferred_identifier'] = request.query_params.get('preferred_identifier', None)
        catalog_record_new['previous_version_identifier'] = catalog_record_current.identifier
        catalog_record_new['previous_version_id'] = catalog_record_current.id
        catalog_record_new['version_created'] = current_time
        catalog_record_new['created_by_api'] = current_time
        catalog_record_new['created_by_user_id'] = request.user.id or None

        serializer_new = CatalogRecordSerializer(data=catalog_record_new, **kwargs)
        serializer_new.is_valid()
        serializer_new.save()

        catalog_record_current.next_version_id = CatalogRecord.objects.get(pk=serializer_new.data['id'])
        catalog_record_current.next_version_identifier = serializer_new.data['identifier']
        catalog_record_current.modified_by_api = current_time
        catalog_record_current.modified_by_user = request.user.id or None
        catalog_record_current.save()

        return serializer_new.data, status.HTTP_201_CREATED

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

        if request.query_params.get('curator', False):
            queryset_search_params['research_dataset__contains'] = { 'curator': [{ 'identifier': request.query_params['curator'] }]}

        if request.query_params.get('owner_id', False):
            queryset_search_params['owner_id'] = request.query_params['owner_id']

        if request.query_params.get('created_by_user_id', False):
            queryset_search_params['created_by_user_id'] = request.query_params['created_by_user_id']

        return queryset_search_params

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
        contract.catalogrecord_set.add(catalog_record)
        contract.save()

    @staticmethod
    def transform_datasets_to_format(catalog_records, target_format):
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

        if target_format == 'metax':
            # mostly for debugging purposes, the 'metax xml' can be returned as well
            return xml_str

        target_xslt_file_path = join(dirname(dirname(__file__)), 'api/base/xslt/%s.xslt' % target_format)

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

        return '<?xml version="1.0" encoding="UTF-8" ?>%s' % transformed_xml

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

            if remote_resource.get('type', False):
                ref_entry = cls.check_ref_data(refdata['resource_type'], remote_resource['type']['identifier'],
                                               'research_dataset.remote_resources.type.identifier', errors)
                if ref_entry:
                    cls.populate_from_ref_data(ref_entry, remote_resource['type'], label_field='pref_label')

        for language in research_dataset.get('language', []):
            ref_entry = cls.check_ref_data(refdata['language'], language['identifier'],
                                           'research_dataset.language.identifier', errors)
            if ref_entry:
                cls.populate_from_ref_data(ref_entry, language, label_field='title')

        access_rights = research_dataset.get('access_rights', None)
        if access_rights:
            for rights_statement_type in access_rights.get('type', []):
                ref_entry = cls.check_ref_data(refdata['access_type'], rights_statement_type['identifier'],
                                               'research_dataset.access_rights.type.identifier', errors)
                if ref_entry:
                    cls.populate_from_ref_data(ref_entry, rights_statement_type, label_field='pref_label')

            for rights_statement_license in access_rights.get('license', []):
                ref_entry = cls.check_ref_data(refdata['license'], rights_statement_license['identifier'],
                                               'research_dataset.access_rights.license.identifier', errors)
                if ref_entry:
                    cls.populate_from_ref_data(ref_entry, rights_statement_license, label_field='title')

            for rra in access_rights.get('has_rights_related_agent', []):
                cls.process_research_agent_obj_with_type(orgdata, rra, 'research_dataset.access_rights.has_rights_related_agent')

        for project in research_dataset.get('is_output_of', []):
            for org_obj in project.get('source_organization', []):
                cls.process_org_obj_against_ref_data(orgdata, org_obj,
                                                     'research_dataset.is_output_of.source_organization')

            for org_obj in project.get('has_funding_agency', []):
                cls.process_org_obj_against_ref_data(orgdata, org_obj,
                                                     'research_dataset.is_output_of.has_funding_agency')

        for other_identifier in research_dataset.get('other_identifier', []):
            if 'type' in other_identifier:
                ref_entry = cls.check_ref_data(refdata['identifier_type'], other_identifier['type']['identifier'],
                                               'research_dataset.other_identifier.type.identifier', errors)
                if ref_entry:
                    cls.populate_from_ref_data(ref_entry, other_identifier['type'], label_field='pref_label')

            if 'provider' in other_identifier:
                cls.process_org_obj_against_ref_data(orgdata, other_identifier['provider'],
                                                     'research_dataset.other_identifier.provider')

        for spatial in research_dataset.get('spatial', []):
            for place_uri in spatial.get('place_uri', []):
                ref_entry = cls.check_ref_data(refdata['location'], place_uri['identifier'],
                                               'research_dataset.spatial.place_uri.identifier', errors)
                if ref_entry:
                    cls.populate_from_ref_data(ref_entry, place_uri, label_field='pref_label')

        for file in research_dataset.get('files', []):
            if file.get('type', False):
                ref_entry = cls.check_ref_data(refdata['resource_type'], file['type']['identifier'],
                                               'research_dataset.files.type.identifier', errors)
                if ref_entry:
                    cls.populate_from_ref_data(ref_entry, file['type'], label_field='pref_label')

        for contributor in research_dataset.get('contributor', []):
            cls.process_research_agent_obj_with_type(orgdata, contributor, 'research_dataset.contributor')

        if 'publisher' in research_dataset:
            cls.process_research_agent_obj_with_type(orgdata, research_dataset['publisher'], 'research_dataset.publisher')

        for curator in research_dataset.get('curator', []):
            cls.process_research_agent_obj_with_type(orgdata, curator, 'research_dataset.curator')

        for creator in research_dataset.get('creator', []):
            cls.process_research_agent_obj_with_type(orgdata, creator, 'research_dataset.creator')

        if 'rights_holder' in research_dataset:
            cls.process_research_agent_obj_with_type(orgdata, research_dataset['rights_holder'], 'research_dataset.rights_holder')

        for activity in research_dataset.get('provenance', []):
            for was_associated_with in activity.get('was_associated_with', []):
                cls.process_research_agent_obj_with_type(orgdata, was_associated_with,
                                               'research_dataset.provenance.was_associated_with')

        if errors:
            raise ValidationError(errors)
