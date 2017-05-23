from rest_framework.serializers import HyperlinkedModelSerializer
from metax_api.models import File

class FileSerializer(HyperlinkedModelSerializer):

    class Meta:
        model = File
        fields = (
            'access_group',
            'byte_size',
            'checksum_algorithm',
            'checksum_checked',
            'checksum_value',
            'download_url',
            'file_format',
            'file_modified',
            'file_name',
            'file_path',
            'json',
            'open_access',
            'removed',
            'replication_path',
        )
