from datetime import datetime
from copy import deepcopy

from rest_framework import status
from metax_api.exceptions import Http400, Http403
from metax_api.models import CatalogRecord, Contract
from .common_service import CommonService

import logging
_logger = logging.getLogger(__name__)
d = logging.getLogger(__name__).debug

class CatalogRecordService(CommonService):

    @staticmethod
    def create_new_dataset_version(request, catalog_record_current, **kwargs):
        """
        Note: no tests yet before further spekking
        """

        if catalog_record_current.next_version_identifier:
            raise Http403({ 'next_version_identifier': ['A newer version already exists. You can not create new versions from archived versions.'] })

        if catalog_record_current.research_dataset['ready_status'] != CatalogRecord.READY_STATUS_FINISHED:
            raise Http403({ 'research_dataset': { 'ready_status': ['Value has to be \'Ready\' in order to create a new version.'] }})

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
        catalog_record_new['research_dataset']['ready_status'] = 'Unfinished'
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
            queryset_search_params['state'] = state_vals

        if request.query_params.get('curator', False):
            queryset_search_params['curator'] = request.query_params['curator']

        if request.query_params.get('owner_id', False):
            queryset_search_params['owner_id'] = request.query_params['owner_id']

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

        if not catalog_record.dataset_is_finished():
            raise Http403({ 'research_dataset': { 'ready_status': ['Value has to be \'Ready\' in order to propose to PAS.'] }})

        if not catalog_record.can_be_proposed_to_pas():
            raise Http403({ 'preservation_state': ['Value must be 0 (not proposed to PAS),'
                ' 7 (longterm PAS rejected), or 8 (midterm PAS rejected), when proposing to PAS. '
                'Current state is %d.' % catalog_record.preservation_state ] })

        try:
            contract = Contract.objects.get(contract_json__identifier=request.query_params.get('contract'))
        except Contract.DoesNotExist:
            raise Http400({ 'contract': ['Contract not found']})

        catalog_record.preservation_state = request.query_params.get('state')
        catalog_record.save()
        contract.catalogrecord_set.add(catalog_record)
        contract.save()

    @staticmethod
    def validate_reference_data(research_dataset, cache):

        def check_ref_data(index, datatype, obj, field_to_check, relation_name):
            """
            Check if the given field exists in the reference data.

            In case the value is not found, an error is appended to the 'errors' dict.

            params:
            index:          the ES index to search from
            datatype:       the ES datatype to search from
            obj:            the dict to read the value from
            field_to_check: the name of the field to read
            relation_name:  the full relation path to the field to hand out in case of errors
            """
            rdtypes = refdata if index == 'ref' else orgdata
            if obj[field_to_check] not in rdtypes[datatype]:
                if not isinstance(errors.get(relation_name, None), list):
                    errors[relation_name] = []
                errors[relation_name].append('Identifier \'%s\' not found in reference data (type: %s)' % (obj[field_to_check], datatype))

        reference_data = cache.get('reference_data')
        refdata = reference_data['reference_data']
        orgdata = reference_data['organization_data']
        errors = {}

        for theme in research_dataset.get('theme', []):
            check_ref_data('ref', 'keyword', theme, 'identifier', 'research_dataset.theme.identifier')

        for discipline in research_dataset.get('discipline', []):
            check_ref_data('ref', 'field_of_science', discipline, 'identifier', 'research_dataset.discipline.identifier')

        for remote_resource in research_dataset.get('remote_resources', []):
            check_ref_data('ref', 'checksum_algorithm', remote_resource['checksum'], 'algorithm', 'research_dataset.remote_resources.checksum.algorithm')

            for license in remote_resource.get('license', []):
                check_ref_data('ref', 'license', license, 'identifier', 'research_dataset.remote_resources.license.identifier')

        for language in research_dataset.get('language', []):
            check_ref_data('ref', 'language', language, 'identifier', 'research_dataset.language.identifier')

        access_rights = research_dataset.get('access_rights', None)
        if access_rights:
            for rights_statement_type in access_rights.get('type', []):
                check_ref_data('ref', 'access_type', rights_statement_type, 'identifier', 'research_dataset.access_rights.type.identifier')

            for rights_statement_license in access_rights.get('license', []):
                check_ref_data('ref', 'license', rights_statement_license, 'identifier', 'research_dataset.access_rights.license.identifier')

        for project in research_dataset.get('is_output_of', []):
            for organization in project.get('source_organization', []):
                check_ref_data('org', 'organization', organization, 'identifier', 'research_dataset.is_output_of.source_organization.identifier')

        if errors:
            raise Http400(errors)
