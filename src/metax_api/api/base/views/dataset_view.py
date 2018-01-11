import logging

from django.http import Http404
from rest_framework import status
from rest_framework.decorators import detail_route, list_route
from rest_framework.response import Response

from metax_api.models import CatalogRecord
from metax_api.renderers import XMLRenderer
from metax_api.services import CatalogRecordService as CRS, CommonService as CS
from metax_api.utils import RabbitMQ
from .common_view import CommonViewSet
from ..serializers import CatalogRecordSerializer, FileSerializer

_logger = logging.getLogger(__name__)
d = logging.getLogger(__name__).debug


class DatasetViewSet(CommonViewSet):

    authentication_classes = ()
    permission_classes = ()

    # note: override get_queryset() to get more control
    queryset = CatalogRecord.objects.select_related('data_catalog', 'contract').all()
    serializer_class = CatalogRecordSerializer
    object = CatalogRecord

    lookup_field = 'pk'

    def __init__(self, *args, **kwargs):
        # As opposed to other views, do not set json schema here
        # It is done in the serializer
        super(DatasetViewSet, self).__init__(*args, **kwargs)

    def get_object(self):
        try:
            return super(DatasetViewSet, self).get_object()
        except Http404:
            if CRS.is_primary_key(self.kwargs.get(self.lookup_field, False)):
                # fail on pk search is clear...
                raise
        # ...but other identifiers can have matches in a dataset's other identifier fields
        return self._search_using_other_dataset_identifiers()

    def get_queryset(self):

        additional_filters = {}

        if self.kwargs.get('pk', None):
            # operations on individual resources can find old versions. those operations
            # then decide if they allow modifying the resource or not
            pass
        else:
            # list operations only list current versions
            additional_filters = { 'next_version_id': None }

        if hasattr(self, 'queryset_search_params'):
            additional_filters.update(**self.queryset_search_params)

        CS.set_if_modified_since_filter(self.request, additional_filters)

        return super(DatasetViewSet, self).get_queryset().filter(**additional_filters)

    def retrieve(self, request, *args, **kwargs):
        self.queryset_search_params = {}
        CS.set_if_modified_since_filter(self.request, self.queryset_search_params)

        res = super(DatasetViewSet, self).retrieve(request, *args, **kwargs)

        if 'dataset_format' in request.query_params:
            res.data = CRS.transform_datasets_to_format(res.data, request.query_params['dataset_format'])
            request.accepted_renderer = XMLRenderer()
        elif 'file_details' in request.query_params:
            CRS.populate_file_details(res.data)

        return res

    def list(self, request, *args, **kwargs):
        # best to specify a variable for parameters intended for filtering purposes in get_queryset(),
        # because other api's may use query parameters of the same name, which can
        # mess up filtering if get_queryset() uses request.query_parameters directly.
        self.queryset_search_params = CRS.get_queryset_search_params(request)
        return super(DatasetViewSet, self).list(request, *args, **kwargs)

    def update(self, request, *args, **kwargs):
        res = super(DatasetViewSet, self).update(request, *args, **kwargs)
        CRS.publish_updated_datasets(res)
        return res

    def update_bulk(self, request, *args, **kwargs):
        res = super(DatasetViewSet, self).update_bulk(request, *args, **kwargs)
        CRS.publish_updated_datasets(res)
        return res

    def partial_update(self, request, *args, **kwargs):
        res = super(DatasetViewSet, self).partial_update(request, *args, **kwargs)
        CRS.publish_updated_datasets(res)
        return res

    def partial_update_bulk(self, request, *args, **kwargs):
        res = super(DatasetViewSet, self).partial_update_bulk(request, *args, **kwargs)
        CRS.publish_updated_datasets(res)
        return res

    def destroy(self, request, *args, **kwargs):
        res = super(DatasetViewSet, self).destroy(request, *args, **kwargs)
        if res.status_code == status.HTTP_204_NO_CONTENT:
            removed_object = self._get_removed_dataset()
            rabbitmq = RabbitMQ()
            rabbitmq.publish({'urn_identifier': removed_object.research_dataset['urn_identifier']},
                routing_key='delete', exchange='datasets')
        return res

    def create(self, request, *args, **kwargs):
        res = super(DatasetViewSet, self).create(request, *args, **kwargs)

        if res.status_code == status.HTTP_201_CREATED:
            if 'success' in res.data:
                # was bulk create
                message = [ r['object'] for r in res.data['success'] ]
            else:
                message = res.data
            rabbitmq = RabbitMQ()
            rabbitmq.publish(message, routing_key='create', exchange='datasets')

        return res

    @detail_route(methods=['get'], url_path="files")
    def files_get(self, request, pk=None):
        """
        Get files associated to this dataset
        """
        catalog_record = self.get_object()
        files = [ FileSerializer(f).data for f in catalog_record.files.all() ]
        return Response(data=files, status=status.HTTP_200_OK)

    @detail_route(methods=['post'], url_path="proposetopas")
    def propose_to_pas(self, request, pk=None):
        CRS.propose_to_pas(request, self.get_object())
        return Response(data={}, status=status.HTTP_204_NO_CONTENT)

    @detail_route(methods=['get'], url_path="exists")
    def dataset_exists(self, request, pk=None):
        try:
            self.get_object()
        except Http404:
            return Response(data=False, status=status.HTTP_200_OK)
        except Exception:
            return Response(data='', status=status.HTTP_500_INTERNAL_SERVER_ERROR)

        return Response(data=True, status=status.HTTP_200_OK)

    @list_route(methods=['get'], url_path="urn_identifiers")
    def get_all_urn_identifiers(self, request):
        self.queryset_search_params = CRS.get_queryset_search_params(request)
        q = self.get_queryset().values('research_dataset')
        urn_ids = [item['research_dataset']['urn_identifier'] for item in q]
        return Response(urn_ids)

    def _search_using_other_dataset_identifiers(self):
        """
        URN-lookup from self.lookup_field failed. Look from dataset json fields
        preferred_identifier first, and array other_identifier after, if there are matches
        """
        lookup_value = self.kwargs.get(self.lookup_field)
        obj = self._search_from_research_dataset({'urn_identifier': lookup_value}, False)

        # allow preferred and other identifier searches only for GET, since their persistence
        # is not guaranteed over time.
        if not obj and self.request.method == 'GET':
            obj = self._search_from_research_dataset({'preferred_identifier': lookup_value}, False)
            if not obj:
                obj = self._search_from_research_dataset(
                    {'other_identifier': [{'local_identifier': lookup_value}]}, True)

        if not obj:
            raise Http404

        return obj

    def _search_from_research_dataset(self, search_json, raise_on_404):
        try:
            return super(DatasetViewSet, self).get_object(
                search_params={'research_dataset__contains': search_json})
        except Http404:
            if raise_on_404:
                raise
            return None

    def _get_removed_dataset(self):
        """
        Get dataset from self.object.objects_unfiltered so that removed objects can be found
        """
        lookup_value = self.kwargs.get('pk')
        different_fields = [
            {},
            { 'search_params': { 'research_dataset__contains': {'urn_identifier': lookup_value }} },
            { 'search_params': { 'research_dataset__contains': {'preferred_identifier': lookup_value }} },
            { 'search_params': { 'research_dataset__contains':
                                {'other_identifier': [{'local_identifier': lookup_value }]}}},
        ]
        for params in different_fields:
            try:
                return self.get_removed_object(**params)
            except Http404:
                pass
        raise Http404

    @detail_route(methods=['get'], url_path="redis")
    def redis_test(self, request, pk=None): # pragma: no cover
        try:
            cached = self.cache.get('cr-1211%s' % pk)
        except:
            d('redis: could not connect during read')
            cached = None
            raise

        if cached:
            d('found in cache, returning')
            return Response(data=cached, status=status.HTTP_200_OK)

        data = self.get_serializer(CatalogRecord.objects.get(pk=1)).data

        try:
            self.cache.set('cr-1211%s' % pk, data)
        except:
            d('redis: could not connect during write')
            raise

        return Response(data=data, status=status.HTTP_200_OK)

    @detail_route(methods=['get'], url_path="rabbitmq")
    def rabbitmq_test(self, request, pk=None): # pragma: no cover
        rabbitmq = RabbitMQ()
        rabbitmq.publish({ 'msg': 'hello create'}, routing_key='create', exchange='datasets')
        rabbitmq.publish({ 'msg': 'hello update'}, routing_key='update', exchange='datasets')
        return Response(data={}, status=status.HTTP_200_OK)
