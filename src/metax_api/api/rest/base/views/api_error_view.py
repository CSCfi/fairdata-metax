# This file is part of the Metax API service
#
# Copyright 2017-2018 Ministry of Education and Culture, Finland
#
# :author: CSC - IT Center for Science Ltd., Espoo Finland <servicedesk@csc.fi>
# :license: MIT

import logging

from django.http import Http404
from rest_framework.decorators import list_route
from rest_framework.response import Response

from metax_api.exceptions import Http403, Http501
from metax_api.services import ApiErrorService
from .common_view import CommonViewSet
from ..serializers import FileSerializer

"""
An API to browse error files and retrieve complete error details.

Allows only reading and deleting by user 'metax', inaccessible to everyone else.

Not connected to the DB in any way, deals directly with files saved in the designated
error file location.
"""


_logger = logging.getLogger(__name__)


class ApiErrorViewSet(CommonViewSet):

    # this serves no purpose, but strangely in local dev browsable api for /rest/apierrors/pid
    # works fine, while in metax-test it will throw an error complaining about a missing serializer.
    serializer_class = FileSerializer

    def initial(self, request, *args, **kwargs):
        if request.user.username != 'metax':
            raise Http403
        return super(ApiErrorViewSet, self).initial(request, *args, **kwargs)

    def retrieve(self, request, *args, **kwargs):
        try:
            error_details = ApiErrorService.retrieve_error_details(kwargs['pk'])
        except:
            raise Http404
        return Response(data=error_details, status=200)

    def list(self, request, *args, **kwargs):
        error_list = ApiErrorService.retrieve_error_list()
        return Response(data=error_list, status=200)

    def destroy(self, request, *args, **kwargs):
        _logger.info('DELETE %s called by %s' % (request.META['PATH_INFO'], request.user.username))
        ApiErrorService.remove_error_file(kwargs['pk'])
        return Response(status=204)

    @list_route(methods=['post'], url_path="flush")
    def flush_errors(self, request):
        _logger.info('%s called by %s' % (request.META['PATH_INFO'], request.user.username))
        files_deleted_count = ApiErrorService.flush_errors()
        return Response(data={ 'files_deleted': files_deleted_count }, status=200)

    def update(self, request, *args, **kwargs):
        raise Http501()

    def update_bulk(self, request, *args, **kwargs):
        raise Http501()

    def partial_update(self, request, *args, **kwargs):
        raise Http501()

    def partial_update_bulk(self, request, *args, **kwargs):
        raise Http501()

    def create(self, request, *args, **kwargs):
        raise Http501()
