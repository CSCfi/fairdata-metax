from rest_framework import viewsets

from metax_api.permissions import ServicePermissions
from metax_api.services import SchemaService


class SchemaViewSet(viewsets.ReadOnlyModelViewSet):

    authentication_classes = ()
    permission_classes = (ServicePermissions,)

    def list(self, request, *args, **kwargs):
        return SchemaService.get_all_schemas()

    def retrieve(self, request, *args, **kwargs):
        return SchemaService.get_schema_content(kwargs.get('pk'))

    def get_queryset(self):
        return self.list(None)

    def get_api_name(self):
        """
        Does not inherit from common...
        """
        return 'schemas'
