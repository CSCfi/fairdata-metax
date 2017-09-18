from django.http import Http404
from rest_framework import status
from rest_framework.decorators import detail_route
from rest_framework.response import Response
from metax_api.models import CatalogRecord
from metax_api.services import CatalogRecordService as CRS
from .common_view import CommonViewSet
from ..serializers import CatalogRecordSerializer, FileSerializer

import logging
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
            additional_filters.update(**self.queryset_search_params)

        return super(DatasetViewSet, self).get_queryset().filter(**additional_filters)

    def list(self, request, *args, **kwargs):
        # best to specify a variable for parameters intended for filtering purposes in get_queryset(),
        # because other api's may use query parameters of the same name, which can
        # mess up filtering if get_queryset() uses request.query_parameters directly.
        self.queryset_search_params = CRS.get_queryset_search_params(request)
        return super(DatasetViewSet, self).list(request, *args, **kwargs)

    def update(self, request, *args, **kwargs):
        res = super(DatasetViewSet, self).update(request, *args, **kwargs)
        if res.status_code == status.HTTP_204_NO_CONTENT:
            self._publish_message(self._updated_request_data, routing_key='update', exchange='datasets')
        return res

    def update_bulk(self, request, *args, **kwargs):
        res = super(DatasetViewSet, self).update_bulk(request, *args, **kwargs)

        # successful operation returns no content at all.
        # however, partially successful operation has to return
        # the errors, so status code cant be 204
        if res.status_code in (status.HTTP_204_NO_CONTENT, status.HTTP_200_OK):
            self._publish_message(self._updated_request_data, routing_key='update', exchange='datasets')

        return res

    def partial_update(self, request, *args, **kwargs):
        res = super(DatasetViewSet, self).partial_update(request, *args, **kwargs)
        if res.status_code == status.HTTP_200_OK:
            self._publish_message(self._updated_request_data, routing_key='update', exchange='datasets')
        return res

    def partial_update_bulk(self, request, *args, **kwargs):
        res = super(DatasetViewSet, self).partial_update_bulk(request, *args, **kwargs)
        if res.status_code == status.HTTP_200_OK:
            self._publish_message(self._updated_request_data, routing_key='update', exchange='datasets')
        return res

    def destroy(self, request, *args, **kwargs):
        res = super(DatasetViewSet, self).destroy(request, *args, **kwargs)
        if res.status_code == status.HTTP_204_NO_CONTENT:
            removed_object = self._get_removed_dataset()
            self._publish_message({ 'urn_identifier': removed_object.research_dataset['urn_identifier'] },
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
            self._publish_message(message, routing_key='create', exchange='datasets')

        return res

    @detail_route(methods=['get'], url_path="files")
    def files_get(self, request, pk=None):
        """
        Get files associated to this dataset
        """
        catalog_record = self.get_object()
        files = [ FileSerializer(f).data for f in catalog_record.files.all() ]
        return Response(data=files, status=status.HTTP_200_OK)

    @detail_route(methods=['post'], url_path="createversion")
    def create_version(self, request, pk=None):
        """
        Create a new version from a dataset that has been marked as finished
        """
        kwargs = { 'context': self.get_serializer_context() }
        new_version_data, http_status = CRS.create_new_dataset_version(request, self.get_object(), **kwargs)
        return Response(data=new_version_data, status=http_status)

    @detail_route(methods=['post'], url_path="proposetopas")
    def propose_to_pas(self, request, pk=None):
        CRS.propose_to_pas(request, self.get_object())
        return Response(data={}, status=status.HTTP_204_NO_CONTENT)

    @detail_route(methods=['get'], url_path="exists")
    def dataset_exists(self, request, pk=None):
        try:
            self.get_object()
        except Exception:
            return Response(data=False, status=status.HTTP_404_NOT_FOUND)

        return Response(data=True, status=status.HTTP_200_OK)

    def _search_using_other_dataset_identifiers(self):
        """
        URN-lookup from self.lookup_field failed. Look from dataset json fields
        preferred_identifier first, and array other_identifier after, if there are matches
        """
        lookup_value = self.kwargs.get(self.lookup_field)
        try:
            obj = self._search_from_research_dataset({'urn_identifier': lookup_value}, False)

            # allow preferred and other identifier searches only for GET, since their persistence
            # is not guaranteed over time.
            if not obj and self.request.method == 'GET':
                obj = self._search_from_research_dataset({'preferred_identifier': lookup_value}, False)
                if not obj:
                    obj = self._search_from_research_dataset(
                        {'other_identifier': [{'local_identifier': lookup_value}]}, True)
        except Exception:
            raise

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
            else:
                return None
        except Exception:
            raise

    def _get_removed_dataset(self):
        """
        Get dataset from self.object.objects_unfiltered so that removed objects can be found
        """
        lookup_value = self.kwargs.get('pk')
        different_fields = [
            {},
            { 'search_params': { 'research_dataset__contains': {'urn_identifier': lookup_value }} },
            { 'search_params': { 'research_dataset__contains': {'preferred_identifier': lookup_value }} },
            { 'search_params': { 'research_dataset__contains': {'other_identifier': [{'local_identifier': lookup_value }]}}},
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
        self._publish_message({ 'msg': 'hello create'}, routing_key='create', exchange='datasets')
        self._publish_message({ 'msg': 'hello update'}, routing_key='update', exchange='datasets')
        return Response(data={}, status=status.HTTP_200_OK)
