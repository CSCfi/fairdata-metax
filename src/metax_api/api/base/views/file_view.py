from metax_api.models import File
from .common_view import CommonViewSet
from ..serializers import FileSerializer

import logging
_logger = logging.getLogger(__name__)
d = logging.getLogger(__name__).debug

def debug_request(request, *args, **kwargs):
    d("====================")
    d(request)
    d(dir(request))
    d(request.user)
    d(dir(request.user))
    d(args)
    d(kwargs)
    d(request.data)
    # raise Exception("stop")


class FileViewSet(CommonViewSet):

    authentication_classes = ()
    permission_classes = ()

    queryset = File.objects.filter(active=True, removed=False)
    serializer_class = FileSerializer
    object = File

    lookup_field = 'json__identifier'

    def __init__(self, *args, **kwargs):
        self.use_json_schema_validation(__file__)
        super(FileViewSet, self).__init__(*args, **kwargs)

    # def get_queryset(self):
    #     return File.objects.all()

    def list(self, request, *args, **kwargs):
        return super(FileViewSet, self).list(request, *args, **kwargs)

    def retrieve(self, request, *args, **kwargs):
        # debug_request(request, *args, **kwargs)
        return super(FileViewSet, self).retrieve(request, *args, **kwargs)

    def update(self, request, *args, **kwargs):
        return super(FileViewSet, self).update(request, *args, **kwargs)

    # def partial_update(self, request, *args, **kwargs):
    #     return super(FileViewSet, self).partial_update(request, *args, **kwargs)

    # def create(self, request, *args, **kwargs):
    #     return super(FileViewSet, self).create(request, *args, **kwargs)

    def destroy(self, request, *args, **kwargs):
        return super(FileViewSet, self).destroy(request, *args, **kwargs)
