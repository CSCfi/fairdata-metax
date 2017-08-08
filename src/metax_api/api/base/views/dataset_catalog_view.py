from metax_api.models import DatasetCatalog
from django.http import Http404
from .common_view import CommonViewSet
from ..serializers import DatasetCatalogSerializer
from rest_framework.decorators import detail_route
from rest_framework.response import Response
from rest_framework import status

import logging
_logger = logging.getLogger(__name__)
d = logging.getLogger(__name__).debug

class DatasetCatalogViewSet(CommonViewSet):

    authentication_classes = ()
    permission_classes = ()

    # note: override get_queryset() to get more control
    queryset = DatasetCatalog.objects.filter(active=True, removed=False)
    serializer_class = DatasetCatalogSerializer
    object = DatasetCatalog

    lookup_field = 'pk'

    def __init__(self, *args, **kwargs):
        self.set_json_schema(__file__)
        super(DatasetCatalogViewSet, self).__init__(*args, **kwargs)

    def get_object(self):
        try:
            return super(DatasetCatalogViewSet, self).get_object()
        except Http404:
            pass
        except Exception:
            raise

        return self._search_using_other_dataset_catalog_identifiers()

    def _search_using_other_dataset_catalog_identifiers(self):
        """
        URN-lookup from self.lookup_field failed. Look from catalog json
        identifiers, if there are matches
        """

        lookup_value = self.kwargs.pop(self.lookup_field)
        try:
            obj = self._search_from_catalog_json({'identifier': lookup_value}, True)
        except Exception:
            raise

        return obj

    def _search_from_catalog_json(self, search_json, raise_on_404):
        try:
            return super(DatasetCatalogViewSet, self).get_object(
                search_params={'catalog_json__contains': search_json})
        except Http404:
            if raise_on_404:
                raise
            else:
                return None
        except Exception:
            raise

    @detail_route(methods=['get'], url_path="exists")
    def dataset_catalog_exists(self, request, pk=None):
        try:
            self.get_object()
        except Exception:
            return Response(data=False, status=status.HTTP_404_NOT_FOUND)

        return Response(data=True, status=status.HTTP_200_OK)
