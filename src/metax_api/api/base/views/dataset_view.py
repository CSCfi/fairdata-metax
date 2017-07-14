from django.http import Http404
from rest_framework import status
from rest_framework.decorators import detail_route
from rest_framework.generics import get_object_or_404
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
            if self.queryset_search_params.get('owner', False):
                additional_filters['research_dataset__contains'] = { 'curator': [{ 'identifier': self.queryset_search_params['owner'] }]}
            if self.queryset_search_params.get('state', False):
                additional_filters['preservation_state__in'] = self.queryset_search_params['state']

        return super(DatasetViewSet, self).get_queryset().filter(**additional_filters)

    def list(self, request, *args, **kwargs):
        # best to specify a variable for parameters intended for filtering purposes in get_queryset(),
        # because other api's may use query parameters of the same name, which can
        # mess up filtering if get_queryset() uses request.query_parameters directly.
        self.queryset_search_params = CRS.get_queryset_search_params(request)
        return super(DatasetViewSet, self).list(request, *args, **kwargs)

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
