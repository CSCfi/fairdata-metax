from rest_framework.generics import get_object_or_404

from metax_api.models import File
from .common_view import CommonViewSet
from ..serializers import FileReadSerializer, FileWriteSerializer, FileDebugSerializer

import logging
_logger = logging.getLogger(__name__)
d = logging.getLogger(__name__).debug

def debug_request(request, *args, **kwargs):
    d("====================")
    d(request)
    d(request.query_params)
    d(dir(request))
    d(request.user)
    d(dir(request.user))
    d(args)
    d(kwargs)
    d(request.data)
    d('------------------')
    # raise Exception("stop")

class FileViewSet(CommonViewSet):

    authentication_classes = ()
    permission_classes = ()

    # note: override get_queryset() to get more control
    queryset = File.objects.filter(active=True, removed=False)
    serializer_class = FileReadSerializer
    object = File

    # lookup field from the perspective of the user. will show up in swagger schemas etc
    lookup_field = 'identifier'

    # actual lookup from db using this field
    lookup_field_internal = 'identifier_sha256'

    def __init__(self, *args, **kwargs):
        self.set_json_schema(__file__)
        super(FileViewSet, self).__init__(*args, **kwargs)

    # def get_queryset(self):
    #     return File.objects.all()

    def get_object(self):
        """
        Overrided from rest_framework generics.py method to primarily use self.lookup_field_internal
        for searching objects.
        """
        self._convert_identifier_to_internal()

        queryset = self.filter_queryset(self.get_queryset())

        # Perform the lookup filtering.
        lookup_url_kwarg = self.lookup_field_internal or self.lookup_url_kwarg or self.lookup_field

        assert lookup_url_kwarg in self.kwargs, (
            'Expected view %s to be called with a URL keyword argument '
            'named "%s". Fix your URL conf, or set the `.lookup_field` '
            'attribute on the view correctly.' %
            (self.__class__.__name__, lookup_url_kwarg)
        )

        filter_kwargs = { self.lookup_field_internal: self.kwargs[lookup_url_kwarg] }

        obj = get_object_or_404(queryset, **filter_kwargs)

        # May raise a permission denied
        self.check_object_permissions(self.request, obj)

        return obj

    def get_serializer_class(self, *args, **kwargs):
        # optionally to disable in production, can also call and test:
        # from django.conf import settings
        # settings.DEBUG
        debug = self.request.query_params.get('debug', False)
        method = self.request.method
        if debug and debug == 'true':
            return FileDebugSerializer
        elif method in ('POST', 'PUT', 'PATCH', 'DELETE'):
            return FileWriteSerializer
        elif method == 'GET':
            return FileReadSerializer
        else:
            _logger.error('get_serializer_class() received unexpected HTTP method: %s' % method)

    def list(self, request, *args, **kwargs):
        return super(FileViewSet, self).list(request, *args, **kwargs)

    # def retrieve(self, request, *args, **kwargs):
    #     return super(FileViewSet, self).retrieve(request, *args, **kwargs)

    # def update(self, request, *args, **kwargs):
    #     return super(FileViewSet, self).update(request, *args, **kwargs)

    # def partial_update(self, request, *args, **kwargs):
    #     return super(FileViewSet, self).partial_update(request, *args, **kwargs)

    def create(self, request, *args, **kwargs):
        if isinstance(request.data, list):
            for row in request.data:
                row.update({ self.lookup_field_internal: self._string_to_int(row[self.lookup_field]) })
        else:
            request.data.update({ self.lookup_field_internal: self._string_to_int(request.data[self.lookup_field]) })
        return super(FileViewSet, self).create(request, *args, **kwargs)

    # def destroy(self, request, *args, **kwargs):
    #     return super(FileViewSet, self).destroy(request, *args, **kwargs)
