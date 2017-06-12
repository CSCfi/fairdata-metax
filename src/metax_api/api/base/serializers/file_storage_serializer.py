from rest_framework.serializers import ModelSerializer

from metax_api.models import FileStorage

class FileStorageReadSerializer(ModelSerializer):

    class Meta:
        model = FileStorage

        # todo include common fields: modified_by, etc ?
        fields = (
            'id',
            'file_storage_json',
        )
