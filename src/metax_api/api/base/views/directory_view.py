from django.http import Http404
from rest_framework import status
from rest_framework.viewsets import GenericViewSet
from rest_framework.response import Response

from metax_api.exceptions import Http400
from metax_api.models import File
from metax_api.api.base.serializers import FileSerializer

import logging
_logger = logging.getLogger(__name__)
d = logging.getLogger(__name__).debug


class DirectoryViewSet(GenericViewSet):

    """
    Inherited from GenericViewSet because this view does not use any of the rest_framework
    Create, Update etc mixins
    """

    authentication_classes = ()
    permission_classes = ()

    # needed to make the ViewSet work, but not actually used / loaded ever
    object = File
    queryset = File.objects.all()
    serializer_class = FileSerializer

    def get(self, request, pk=None):
        # will probably be implemented
        raise Http404

    def rename_directory(self, request, pk=None):
        # will probably be implemented
        raise Http404

    def delete_directory(self, request, pk=None):
        try:
            path = request.query_params['path']
        except KeyError:
            raise Http400('path is a required query parameter')

        files_exist = File.objects.filter(file_path__startswith=path).first()
        if not files_exist:
            raise Http404

        affected_files = File.objects.delete_directory(path)
        return Response(data={ 'affected_files': affected_files }, status=status.HTTP_200_OK)
