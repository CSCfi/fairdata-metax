from rest_framework import status
from rest_framework.decorators import detail_route
from rest_framework.response import Response

from metax_api.models import Dataset
from .common_view import CommonViewSet
from ..serializers import DatasetReadSerializer, FileReadSerializer

import logging
_logger = logging.getLogger(__name__)
d = logging.getLogger(__name__).debug

class DatasetViewSet(CommonViewSet):

    authentication_classes = ()
    permission_classes = ()

    # note: override get_queryset() to get more control
    queryset = Dataset.objects.filter(active=True, removed=False)
    serializer_class = DatasetReadSerializer
    object = Dataset

    lookup_field = 'pk'

    # allow search by external identifier (urn, or whatever string in practice) as well
    lookup_field_other = 'identifier'

    def __init__(self, *args, **kwargs):
        self.set_json_schema(__file__)
        super(DatasetViewSet, self).__init__(*args, **kwargs)

    def get_object(self):
        """
        todo:
        - look also by json field otherIdentifier, if no match is found?
        """
        return super(DatasetViewSet, self).get_object()

    def get_queryset(self):
        if not self.request.query_params:
            return super(DatasetViewSet, self).get_queryset()
        else:
            query_params = self.request.query_params
            additional_filters = {}
            if query_params.get('owner', False):
                additional_filters['dataset_json__contains'] = { 'rightsHolder': { 'name': query_params['owner'] }}
            if query_params.get('state', False):
                additional_filters['pas_state'] = query_params['state']
            return self.queryset.filter(**additional_filters)

    @detail_route(methods=['get'], url_path="files")
    def files_get(self, request, pk=None):
        dataset = self.get_object()
        files = [ FileReadSerializer(f).data for f in dataset.files.all() ]
        return Response(data=files, status=status.HTTP_200_OK)
