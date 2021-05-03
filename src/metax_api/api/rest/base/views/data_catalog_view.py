# This file is part of the Metax API service
#
# Copyright 2017-2018 Ministry of Education and Culture, Finland
#
# :author: CSC - IT Center for Science Ltd., Espoo Finland <servicedesk@csc.fi>
# :license: MIT

from django.http import Http404

from metax_api.models import DataCatalog

from ..serializers import DataCatalogSerializer
from .common_view import CommonViewSet


class DataCatalogViewSet(CommonViewSet):

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
