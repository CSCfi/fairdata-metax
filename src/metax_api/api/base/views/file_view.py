from rest_framework import authentication, permissions
from rest_framework.viewsets import ModelViewSet

from metax_api.models import File
from ..serializers import FileSerializer

# import logging
# d = logging.getLogger(__name__).debug

# prints to console when running manage.py runserver
d = print

class FileViewSet(ModelViewSet):

    authentication_classes = ()
    permission_classes = ()

    queryset = File.objects.none()
    serializer_class = FileSerializer

    def get_queryset(self):
        return File.objects.all()

    def list(self, request, *args, **kwargs):
        d('list called')
        return super(FileViewSet, self).list(request, *args, **kwargs)

    def retrieve(self, request, *args, **kwargs):
        d('retrieve called')
        return super(FileViewSet, self).retrieve(request, *args, **kwargs)
