import logging

from django.http import Http404
from rest_framework import status
from rest_framework.decorators import detail_route
from rest_framework.response import Response

from metax_api.models import DataCatalog
from .common_view import CommonViewSet
from ..serializers import DataCatalogSerializer

_logger = logging.getLogger(__name__)
d = logging.getLogger(__name__).debug

class DataCatalogViewSet(CommonViewSet):

    authentication_classes = ()
    permission_classes = ()

    # note: override get_queryset() to get more control
    queryset = DataCatalog.objects.filter(active=True, removed=False)
    serializer_class = DataCatalogSerializer
    object = DataCatalog

    lookup_field = 'pk'

    def __init__(self, *args, **kwargs):
        self.set_json_schema(__file__)
        super(DataCatalogViewSet, self).__init__(*args, **kwargs)

    def get_object(self):
        try:
            return super(DataCatalogViewSet, self).get_object()
        except Http404:
            pass
        except Exception:
            raise

        return self._search_using_other_data_catalog_identifiers()

    def _search_using_other_data_catalog_identifiers(self):
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
            return super(DataCatalogViewSet, self).get_object(
                search_params={'catalog_json__contains': search_json})
        except Http404:
            if raise_on_404:
                raise
            else:
                return None
        except Exception:
            raise

    @detail_route(methods=['get'], url_path="exists")
    def data_catalog_exists(self, request, pk=None):
        try:
            self.get_object()
        except Http404:
            return Response(data=False, status=status.HTTP_200_OK)
        except Exception:
            return Response(data='', status=status.HTTP_500_INTERNAL_SERVER_ERROR)

        return Response(data=True, status=status.HTTP_200_OK)
