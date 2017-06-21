from metax_api.models import Contract
from .common_view import CommonViewSet
from ..serializers import ContractSerializer

import logging
_logger = logging.getLogger(__name__)
d = logging.getLogger(__name__).debug

class ContractViewSet(CommonViewSet):

    authentication_classes = ()
    permission_classes = ()

    queryset = Contract.objects.filter(active=True, removed=False)
    serializer_class = ContractSerializer
    object = Contract

    lookup_field = 'pk'

    def __init__(self, *args, **kwargs):
        self.set_json_schema(__file__)
        super(ContractViewSet, self).__init__(*args, **kwargs)

    def get_queryset(self):
        if not self.request.query_params:
            return super(ContractViewSet, self).get_queryset()
        else:
            query_params = self.request.query_params
            additional_filters = {}
            if query_params.get('organization', False):
                additional_filters['contract_json__contains'] = { 'organization': { 'organization_identifier': query_params['organization'] }}
            return self.queryset.filter(**additional_filters)
