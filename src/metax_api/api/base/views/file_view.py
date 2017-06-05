from django.conf import settings
from django.http import Http404
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

    lookup_field = 'pk'

    # allow search by external identifier (urn, or whatever string in practice) as well
    lookup_field_other = 'identifier'

    def __init__(self, *args, **kwargs):
        self.set_json_schema(__file__)
        super(FileViewSet, self).__init__(*args, **kwargs)

    def get_object(self):
        """
        Overrided from rest_framework generics.py method to also allow searching by the field
        lookup_field_other

        future todo:
        - query params:
            - dataset (string)
            - owner_email (string)
            - fields (list of strings)
            - offset (integer) (paging)
            - limit (integer) (limit for paging)
        """

        if self.kwargs.get(self.lookup_field, False) and ':' in self.kwargs[self.lookup_field]:
            # lookup by alternative field lookup_field_other
            lookup_url_kwarg = self.lookup_field_other

            # replace original field name with field name in lookup_field_other
            self.kwargs[lookup_url_kwarg] = self.kwargs.pop(self.lookup_field)
        else:
            # lookup by originak lookup_field. standard django procedure
            lookup_url_kwarg = self.lookup_url_kwarg or self.lookup_field

        queryset = self.filter_queryset(self.get_queryset())

        assert lookup_url_kwarg in self.kwargs, (
            'Expected view %s to be called with a URL keyword argument '
            'named "%s". Fix your URL conf, or set the `.lookup_field` '
            'attribute on the view correctly.' %
            (self.__class__.__name__, lookup_url_kwarg)
        )

        filter_kwargs = { lookup_url_kwarg: self.kwargs[lookup_url_kwarg] }

        try:
            obj = get_object_or_404(queryset, **filter_kwargs)
        except Exception as e:
            _logger.debug('get_object(): could not find an object with field and value: %s: %s' % (lookup_url_kwarg, filter_kwargs[lookup_url_kwarg]))
            raise Http404

        # May raise a permission denied
        self.check_object_permissions(self.request, obj)

        return obj

    def get_serializer_class(self, *args, **kwargs):
        if settings.DEBUG:
            debug = self.request.query_params.get('debug', False)
            if debug and debug == 'true':
                _logger.debug('get_serializer_class(): returning FileDebugSerializer')
                return FileDebugSerializer

        method = self.request.method

        if method in ('POST', 'PUT', 'PATCH', 'DELETE'):
            return FileWriteSerializer
        elif method == 'GET':
            return FileReadSerializer
        else:
            _logger.error('get_serializer_class() received unexpected HTTP method: %s. returning FileReadSerializer' % method)
            return FileReadSerializer

    def list(self, request, *args, **kwargs):
        return super(FileViewSet, self).list(request, *args, **kwargs)

    def create(self, request, *args, **kwargs):
        return super(FileViewSet, self).create(request, *args, **kwargs)
