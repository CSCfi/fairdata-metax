import logging

from rest_framework import status
from rest_framework.decorators import detail_route
from rest_framework.response import Response

from metax_api.models import Contract
from metax_api.services import CommonService
from .common_view import CommonViewSet
from ..serializers import ContractSerializer, CatalogRecordSerializer

_logger = logging.getLogger(__name__)
d = logging.getLogger(__name__).debug


class ContractViewSet(CommonViewSet):

    authentication_classes = ()
    permission_classes = ()

    serializer_class = ContractSerializer
    object = Contract

    lookup_field = 'pk'

    def __init__(self, *args, **kwargs):
        self.set_json_schema(__file__)
        super(ContractViewSet, self).__init__(*args, **kwargs)

    def get_object(self):
        lookup_value = self.kwargs.get(self.lookup_field, False)
        search_params = None
        if not CommonService.is_primary_key(lookup_value):
            search_params = { 'contract_json__contains': { 'identifier': lookup_value }}
        return super(ContractViewSet, self).get_object(search_params=search_params)

    def get_queryset(self):
        if not self.request.query_params:
            return super(ContractViewSet, self).get_queryset()
        else:
            query_params = self.request.query_params
            additional_filters = {}
            if query_params.get('organization', False):
                additional_filters['contract_json__contains'] = {
                    'organization': { 'organization_identifier': query_params['organization'] }}
            return super(ContractViewSet, self).get_queryset().filter(**additional_filters)

    @detail_route(methods=['get'], url_path="datasets")
    def datasets_get(self, request, pk=None):
        contract = self.get_object()
        catalog_records = [ CatalogRecordSerializer(f).data for f in contract.records.all() ]
        return Response(data=catalog_records, status=status.HTTP_200_OK)
