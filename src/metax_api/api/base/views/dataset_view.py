from rest_framework import status
from rest_framework.decorators import detail_route
from rest_framework.response import Response

from metax_api.models import CatalogRecord
from .common_view import CommonViewSet
from ..serializers import CatalogRecordSerializer, FileSerializer

import logging
_logger = logging.getLogger(__name__)
d = logging.getLogger(__name__).debug

class DatasetViewSet(CommonViewSet):

    authentication_classes = ()
    permission_classes = ()

    # note: override get_queryset() to get more control
    queryset = CatalogRecord.objects.filter(active=True, removed=False)
    serializer_class = CatalogRecordSerializer
    object = CatalogRecord

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
                additional_filters['research_dataset__contains'] = { 'rightsHolder': { 'name': query_params['owner'] }}
            if query_params.get('state', False):
                additional_filters['preservation_state'] = query_params['state']
            return self.queryset.filter(**additional_filters)

    # def update(self, *args, **kwargs):
    #     # causes the serializer later on to return only fields relevant to a dataset
    #     kwargs.update({ 'dataset_only': True, 'partial': True })
    #     return super(DatasetViewSet, self).update(*args, **kwargs)

    # def partial_update(self, *args, **kwargs):
    #     # causes the serializer later on to return only fields relevant to a dataset
    #     kwargs.update({ 'dataset_only': True, 'partial': True })
    #     return super(DatasetViewSet, self).update(*args, **kwargs)

    @detail_route(methods=['get'], url_path="files")
    def files_get(self, request, pk=None):
        catalog_record = self.get_object()
        files = [ FileSerializer(f).data for f in catalog_record.files.all() ]
        return Response(data=files, status=status.HTTP_200_OK)
