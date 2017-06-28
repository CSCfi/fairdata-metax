from django.http import Http404

from rest_framework import status
from rest_framework.decorators import detail_route
from rest_framework.generics import get_object_or_404
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
        if not self.request.query_params:
            return super(DatasetViewSet, self).get_queryset()
        else:
            query_params = self.request.query_params
            additional_filters = {}
            if query_params.get('owner', False):
                additional_filters['research_dataset__contains'] = { 'curator': [{ 'identifier': query_params['owner'] }]}
            if query_params.get('state', False):
                additional_filters['preservation_state__in'] = query_params['state'].split(',')
            return self.queryset.filter(**additional_filters)

    def list(self, request, *args, **kwargs):
        if request.query_params.get('state', False):
            for val in request.query_params['state'].split(','):
                try:
                    int(val)
                except ValueError:
                    return Response(data={ 'state': ['Value \'%s\' is not an integer' % val] }, status=status.HTTP_400_BAD_REQUEST)
        return super(DatasetViewSet, self).list(request, *args, **kwargs)

    @detail_route(methods=['get'], url_path="files")
    def files_get(self, request, pk=None):
        catalog_record = self.get_object()
        files = [ FileSerializer(f).data for f in catalog_record.files.all() ]
        return Response(data=files, status=status.HTTP_200_OK)

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
