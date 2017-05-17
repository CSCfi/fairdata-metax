from rest_framework import authentication, permissions

from metax_api.models import File
from .common_view import CommonViewSet
from ..serializers import FileSerializer

class FileViewSet(CommonViewSet):

    authentication_classes = ()
    permission_classes = ()

    queryset = File.objects.all()
    serializer_class = FileSerializer

    def __init__(self, *args, **kwargs):
        self.use_json_schema_validation(__file__)
        super(FileViewSet, self).__init__(*args, **kwargs)

    # def get_queryset(self):
    #     return File.objects.all()

    # def list(self, request, *args, **kwargs):
    #     return super(FileViewSet, self).list(request, *args, **kwargs)

    # def retrieve(self, request, *args, **kwargs):
    #     return super(FileViewSet, self).retrieve(request, *args, **kwargs)

    # def update(self, request, *args, **kwargs):
    #     return super(FileViewSet, self).update(request, *args, **kwargs)

    # def partial_update(self, request, *args, **kwargs):
    #     return super(FileViewSet, self).partial_update(request, *args, **kwargs)

    # def create(self, request, *args, **kwargs):
    #     return super(FileViewSet, self).create(request, *args, **kwargs)

    # def destroy(self, request, *args, **kwargs):
    #     return super(FileViewSet, self).destroy(request, *args, **kwargs)
