from django.conf import settings
from django.http import Http404
from rest_framework import status
from rest_framework.decorators import detail_route
from rest_framework.response import Response

from metax_api.models import File
from .common_view import CommonViewSet
from ..serializers import FileSerializer, FileDebugSerializer, XmlMetadataSerializer

import logging
_logger = logging.getLogger(__name__)
d = logging.getLogger(__name__).debug

class FileViewSet(CommonViewSet):

    authentication_classes = ()
    permission_classes = ()

    # note: override get_queryset() to get more control
    queryset = File.objects.all()
    serializer_class = FileSerializer
    object = File

    lookup_field = 'pk'

    # allow search by external identifier (urn, or whatever string in practice) as well
    lookup_field_other = 'identifier'

    def __init__(self, *args, **kwargs):
        self.set_json_schema(__file__)
        super(FileViewSet, self).__init__(*args, **kwargs)

    def get_object(self):
        """
        future todo:
        - query params:
            - owner_email (string)
            - fields (list of strings)
            - offset (integer) (paging)
            - limit (integer) (limit for paging)
        """
        return super(FileViewSet, self).get_object()

    def get_serializer_class(self, *args, **kwargs):
        if settings.DEBUG:
            debug = self.request.query_params.get('debug', False)
            if debug and debug == 'true':
                _logger.debug('get_serializer_class(): returning FileDebugSerializer')
                return FileDebugSerializer
        return FileSerializer

    @detail_route(methods=['get'], url_path="xml")
    def xml_get(self, request, pk=None):
        file = self.get_object()
        try:
            xml_metadata = file.xmlmetadata_set.get(namespace=request.query_params.get('namespace', False))
        except Exception as e:
            raise Http404

        # return Response(data=xml_metadata.xml, status=status.HTTP_200_OK, content_type='text/xml')
        return Response(data=xml_metadata.xml, status=status.HTTP_200_OK)

    @detail_route(methods=['put'], url_path="xml")
    def xml_put(self, request, pk=None):
        file = self.get_object()
        self._update_common_info(request)
        try:
            xml_metadata = file.xmlmetadata_set.get(namespace=request.query_params.get('namespace', False))
            request.data['id'] = xml_metadata.id
        except Exception as e:
            # not found - create for the first time
            pass
        request.data['file_id'] = file.id
        serializer = XmlMetadataSerializer(request.data)
        serializer.is_valid()
        serializer.save()
        return Response(data=None, status=status.HTTP_204_NO_CONTENT)
