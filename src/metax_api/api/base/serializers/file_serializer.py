from rest_framework.serializers import HyperlinkedModelSerializer
from metax_api.models import File

class FileSerializer(HyperlinkedModelSerializer):

    class Meta:
        model = File
        fields = ('id', 'file_name', 'json')
