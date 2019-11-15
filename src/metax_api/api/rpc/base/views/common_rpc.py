# This file is part of the Metax API service
#
# Copyright 2017-2018 Ministry of Education and Culture, Finland
#
# :author: CSC - IT Center for Science Ltd., Espoo Finland <servicedesk@csc.fi>
# :license: MIT

import logging

from metax_api.api.rest.base.views import CommonViewSet
from metax_api.exceptions import Http501
from metax_api.permissions import EndUserPermissions, ServicePermissions

_logger = logging.getLogger(__name__)


class CommonRPC(CommonViewSet):

    """
    A base object for other RPC classes. Inherits CommonViewSet in order to use some of the
    common tricks, such as saving errors to /apierrors, request modifications, permission objects...
    """

    api_type = 'rpc'

    permission_classes = (EndUserPermissions, ServicePermissions)

    def get_api_name(self):
        """
        Return api name, example: DatasetRPC -> datasets.
        Some views where the below formula does not produce a sensible result
        will inherit this and return a customized result.
        """
        return '%ss' % self.__class__.__name__.split('RPC')[0].lower()

    def create(self, request, *args, **kwargs):
        raise Http501()

    def retrieve(self, request, *args, **kwargs):
        raise Http501()

    def list(self, request, *args, **kwargs):
        raise Http501()

    def update(self, request, *args, **kwargs):
        raise Http501()

    def delete(self, request, *args, **kwargs):
        raise Http501()

    def update_bulk(self, request, *args, **kwargs):
        raise Http501()

    def partial_update(self, request, *args, **kwargs):
        raise Http501()

    def partial_update_bulk(self, request, *args, **kwargs):
        raise Http501()

    def destroy(self, request, *args, **kwargs):
        raise Http501()
