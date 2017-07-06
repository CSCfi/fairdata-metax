from datetime import datetime
from copy import deepcopy

from rest_framework import status
from metax_api.exceptions import Http400, Http403
from metax_api.models import CatalogRecord, Contract
from metax_api.api.base.serializers import CatalogRecordSerializer
from .common_service import CommonService

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

        if request.query_params.get('owner', False):
            queryset_search_params['owner'] = request.query_params['owner']

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
