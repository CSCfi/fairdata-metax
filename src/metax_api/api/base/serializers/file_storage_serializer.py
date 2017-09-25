from metax_api.models import FileStorage
from .common_serializer import CommonSerializer

class FileStorageSerializer(CommonSerializer):

    class Meta:
        model = FileStorage

        # todo include common fields: modified_by, etc ?
        fields = (
            'id',
            'file_storage_json',
        )
