import logging

from rest_framework import viewsets
from metax_api.services import SchemaService


_logger = logging.getLogger(__name__)
d = logging.getLogger(__name__).debug


class SchemaViewSet(viewsets.ReadOnlyModelViewSet):

    authentication_classes = ()
    permission_classes = ()

    def list(self, request, *args, **kwargs):
        return SchemaService.get_all_schemas()

    def retrieve(self, request, *args, **kwargs):
        return SchemaService.get_schema_content(kwargs.get('pk'))
