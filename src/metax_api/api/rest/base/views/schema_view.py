# This file is part of the Metax API service
#
# Copyright 2017-2018 Ministry of Education and Culture, Finland
#
# :author: CSC - IT Center for Science Ltd., Espoo Finland <servicedesk@csc.fi>
# :license: MIT

from rest_framework import viewsets

from metax_api.permissions import ServicePermissions
from metax_api.services import SchemaService


class SchemaViewSet(viewsets.ReadOnlyModelViewSet):

    filter_backends = ()
    authentication_classes = ()
    permission_classes = (ServicePermissions,)
    api_type = 'rest'

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
