from copy import deepcopy
from datetime import datetime

from django.http import Http404
from rest_framework import status
from rest_framework.decorators import detail_route
from rest_framework.generics import get_object_or_404
from rest_framework.response import Response

from metax_api.exceptions import Http400, Http403
from metax_api.models import CatalogRecord, Contract
from .common_view import CommonViewSet
from ..serializers import CatalogRecordSerializer, FileSerializer

import logging
_logger = logging.getLogger(__name__)
d = logging.getLogger(__name__).debug

class DatasetViewSet(CommonViewSet):

    authentication_classes = ()
    permission_classes = ()

    # note: override get_queryset() to get more control
    queryset = CatalogRecord.objects.all()
    serializer_class = CatalogRecordSerializer
    object = CatalogRecord

    lookup_field = 'pk'

    # allow search by external identifier (urn, or whatever string in practice) as well
    lookup_field_other = 'identifier'

    def __init__(self, *args, **kwargs):
        self.set_json_schema(__file__)
        super(DatasetViewSet, self).__init__(*args, **kwargs)

    def get_object(self):
        try:
            return super(DatasetViewSet, self).get_object()
        except Http404:
            if self.is_primary_key(self.kwargs.get(self.lookup_field, False)):
                # fail on pk search is clear, but other identifiers can have matches
                # in a dataset's other identifier fields
                raise
        except Exception:
            raise

        return self._search_using_other_dataset_identifiers()

    def get_queryset(self):
        if self.kwargs.get('pk', None):
            # operations on individual resources can find old versions. those operations
            # then decide if they allow modifying the resource or not
            additional_filters = {}
        else:
            # list operations only list current versions
            additional_filters = { 'next_version_id': None }

        if hasattr(self, 'queryset_search_params'):
            if self.queryset_search_params.get('owner', False):
                additional_filters['research_dataset__contains'] = { 'curator': [{ 'identifier': self.queryset_search_params['owner'] }]}
            if self.queryset_search_params.get('state', False):
                additional_filters['preservation_state__in'] = self.queryset_search_params['state'].split(',')

        return super(DatasetViewSet, self).get_queryset().filter(**additional_filters)

    def list(self, request, *args, **kwargs):

        # best to specify a variable for parameters intended for filtering purposes in get_queryset(),
        # because other api's may use query parameters of the same name, which can
        # mess up filtering if get_queryset() uses request.query_parameters directly.
        self.queryset_search_params = {}

        if request.query_params.get('state', False):
            for val in request.query_params['state'].split(','):
                try:
                    int(val)
                except ValueError:
                    raise Http400({ 'state': ['Value \'%s\' is not an integer' % val] })
            self.queryset_search_params['state'] = request.query_params['state']

        if request.query_params.get('owner', False):
            self.queryset_search_params['owner'] = request.query_params['owner']

        return super(DatasetViewSet, self).list(request, *args, **kwargs)

    @detail_route(methods=['get'], url_path="files")
    def files_get(self, request, pk=None):
        catalog_record = self.get_object()
        files = [ FileSerializer(f).data for f in catalog_record.files.all() ]
        return Response(data=files, status=status.HTTP_200_OK)

    @detail_route(methods=['post'], url_path="createversion")
    def create_version(self, request, pk=None):
        previous_catalog_record = self.get_object()

        if previous_catalog_record.next_version_identifier:
            raise Http403({ 'next_version_identifier': ['A newer version already exists. You can not create new versions from archived versions.'] })
        elif previous_catalog_record.research_dataset['ready_status'] != 'Ready':
            raise Http403({ 'research_dataset': { 'ready_status': ['Value has to be \'Ready\' in order to create a new version.'] }})

        prev_ver_serializer = CatalogRecordSerializer(previous_catalog_record)
        current_time = datetime.now()

        catalog_record_new = deepcopy(prev_ver_serializer.data)
        catalog_record_new.pop('id')
        catalog_record_new.pop('modified_by_api')
        catalog_record_new.pop('modified_by_user')
        catalog_record_new['identifier'] = 'urn:nice:generated:identifier' # TODO
        catalog_record_new['research_dataset']['identifier'] = 'urn:nice:generated:identifier' # TODO
        catalog_record_new['research_dataset']['preferred_identifier'] = request.query_params.get('preferred_identifier', None)
        catalog_record_new['research_dataset']['ready_status'] = 'Unfinished'
        catalog_record_new['previous_version_identifier'] = previous_catalog_record.identifier
        catalog_record_new['previous_version_id'] = previous_catalog_record.id
        catalog_record_new['version_created'] = current_time
        request.data.update(catalog_record_new)

        res = super(DatasetViewSet, self).create(request, pk=pk)

        previous_catalog_record.next_version_id = CatalogRecord.objects.get(pk=res.data['id'])
        previous_catalog_record.next_version_identifier = res.data['identifier']
        previous_catalog_record.modified_by_api = current_time
        previous_catalog_record.modified_by_user = request.user.id or None
        previous_catalog_record.save()

        return res

    @detail_route(methods=['post'], url_path="proposetopas")
    def propose_to_pas(self, request, pk=None):

        if not request.query_params.get('state', False):
            raise Http400({ 'state': ['Query parameter \'state\' is a required parameter.'] })
        elif request.query_params.get('state') not in ('1', '2'):
            raise Http400({ 'state': ['Query parameter \'state\' value must be 1 or 2.'] })
        elif not request.query_params.get('contract', False):
            raise Http400({ 'contract': ['Query parameter \'contract\' is a required parameter.'] })

        catalog_record = self.get_object()

        if catalog_record.research_dataset['ready_status'] != 'Ready':
            raise Http403({ 'research_dataset': { 'ready_status': ['Value has to be \'Ready\' in order to propose to PAS.'] }})

        if catalog_record.preservation_state not in (
                CatalogRecord.PRESERVATION_STATE_NOT_IN_PAS,
                CatalogRecord.PRESERVATION_STATE_LONGTERM_PAS_REJECTED,
                CatalogRecord.PRESERVATION_STATE_MIDTERM_PAS_REJECTED):
            raise Http400({ 'error': ['Dataset preservation_state must be 0 (not proposed to PAS),'
                ' 7 (longterm PAS rejected), or 8 (midterm PAS rejected), when proposing to PAS. '
                'Current state is %d.' % catalog_record.preservation_state ] })

        try:
            contract_id = Contract.objects.get(contract_json__identifier=request.query_params.get('contract')).id
        except Contract.DoesNotExist:
            raise Http404({ 'contract': ['Contract does not exist']})

        request.data['preservation_state'] = request.query_params.get('state')
        request.data['contract'] = contract_id
        return super(DatasetViewSet, self).update(request, partial=True)

    def _search_using_other_dataset_identifiers(self):
        """
        URN-lookup from self.lookup_field_other failed. Look from dataset json fields
        preferred_identifier first, and array other_identifier after, if there are matches
        """
        lookup_value = self.kwargs[self.lookup_field_other]
        search_params = { 'research_dataset__contains': { 'urn_identifier': lookup_value }}
        try:
            return super(DatasetViewSet, self).get_object(search_params=search_params)
        except Http404:
            # more fields to try, dont raise yet...
            pass
        except Exception:
            raise

        # finally try search the array other_identifier
        queryset = self.filter_queryset(self.get_queryset())
        filter_kwargs = { 'research_dataset__contains': { 'other_identifier': [{ 'local_identifier': lookup_value }]}}

        try:
            obj = get_object_or_404(queryset, **filter_kwargs)
        except Exception:
            # this was last chance, now raise 404
            raise Http404

        # May raise a permission denied
        self.check_object_permissions(self.request, obj)

        return obj
