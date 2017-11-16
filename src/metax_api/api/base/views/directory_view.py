# from django.http import Http404
# from rest_framework import status
from rest_framework.response import Response

from rest_framework.decorators import detail_route, list_route

from metax_api.api.base.serializers import DirectorySerializer
from metax_api.exceptions import Http400, Http501
from metax_api.models import Directory
from metax_api.services import FileService
from .common_view import CommonViewSet

import logging
_logger = logging.getLogger(__name__)
d = logging.getLogger(__name__).debug


class DirectoryViewSet(CommonViewSet):

    authentication_classes = ()
    permission_classes = ()

    object = Directory
    queryset = Directory.objects.select_related('parent_directory').all()
    serializer_class = DirectorySerializer

    lookup_field_other = 'identifier'
    create_bulk_method = FileService.create_bulk

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

    @detail_route(methods=['get'], url_path="files")
    def get_files(self, request, pk=None):
        """
        Return a list of child files and directories of a directory.
        """
        recursive = 'recursive' in request.query_params
        files_and_dirs = FileService.get_directory_contents(pk, recursive=recursive)
        return Response(files_and_dirs)

    @list_route(methods=['get'], url_path="root")
    def get_project_root_directories(self, request):
        """
        Return root directory for a project. This is useful when starting
        to browse files for a project, when individual root-level directory identifier
        is not yet known.

        Example: GET /directories/project?name=projext_x
        """
        if 'project' not in request.query_params:
            raise Http400('project is a required query parameter')

        root_dirs = FileService.get_project_root_directories(request.query_params['project'])

        return Response(root_dirs)
