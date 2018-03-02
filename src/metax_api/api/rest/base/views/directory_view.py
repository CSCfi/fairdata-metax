from collections import defaultdict
import logging

from rest_framework.decorators import detail_route, list_route
from rest_framework.response import Response

from metax_api.api.rest.base.serializers import DirectorySerializer
from metax_api.exceptions import Http400, Http501
from metax_api.models import Directory
from metax_api.services import CommonService, FileService
from .common_view import CommonViewSet

_logger = logging.getLogger(__name__)
d = logging.getLogger(__name__).debug


class DirectoryViewSet(CommonViewSet):

    authentication_classes = ()
    permission_classes = ()

    object = Directory

    queryset = Directory.objects.select_related('parent_directory').all()
    queryset_unfiltered = Directory.objects_unfiltered.select_related('parent_directory').all()

    serializer_class = DirectorySerializer

    lookup_field_other = 'identifier'
    create_bulk_method = FileService.create_bulk

    def list(self, request, *args, **kwargs):
        raise Http501()

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

    def _get_directory_contents(self, request, identifier=None):
        """
        A wrapper to call FS to collect and validate parameters from the request,
        and then call FS.get_directory_contents().
        """
        include_parent = CommonService.get_boolean_query_param(request, 'include_parent')
        dirs_only = CommonService.get_boolean_query_param(request, 'directories_only')
        recursive = CommonService.get_boolean_query_param(request, 'recursive')
        max_depth = request.query_params.get('depth', 1)

        # max_depth can be an integer > 0, or * for everything.
        try:
            max_depth = int(max_depth)
        except ValueError:
            if max_depth != '*':
                raise Http400({ 'detail': ['value of depth must be an integer higher than 0, or *'] })
        else:
            if max_depth <= 0:
                raise Http400({ 'detail': ['value of depth must be higher than 0'] })

        urn_identifier = request.query_params.get('urn_identifier', None)

        files_and_dirs = FileService.get_directory_contents(
            identifier=identifier,
            path=request.query_params.get('path', None),
            project_identifier=request.query_params.get('project', None),
            recursive=recursive,
            max_depth=max_depth,
            dirs_only=dirs_only,
            include_parent=include_parent,
            urn_identifier=urn_identifier
        )

        return Response(files_and_dirs)

    @detail_route(methods=['get'], url_path="files")
    def get_files(self, request, pk=None):
        """
        Return a list of child files and directories of a directory.
        """
        return self._get_directory_contents(request, identifier=pk)

    @list_route(methods=['get'], url_path="files")
    def get_files_by_path(self, request):
        """
        Return a list of child files and directories of a directory, queried
        by path and project identifier, instead of directly by identifier
        """
        errors = defaultdict(list)

        if 'project' not in request.query_params:
            errors['detail'].append('project is a required query parameter')
        if 'path' not in request.query_params:
            errors['detail'].append('path is a required query parameter')

        if errors:
            raise Http400(errors)

        return self._get_directory_contents(request)

    @list_route(methods=['get'], url_path="root")
    def get_project_root_directory(self, request):
        """
        Return root directory for a project. This is useful when starting
        to browse files for a project, when individual root-level directory identifier
        is not yet known.

        Example: GET /directories/root?project=projext_x
        """
        if 'project' not in request.query_params:
            raise Http400('project is a required query parameter')

        root_dirs = FileService.get_project_root_directory(request.query_params['project'])

        return Response(root_dirs)
