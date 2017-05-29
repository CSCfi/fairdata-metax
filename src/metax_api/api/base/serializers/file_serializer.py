from jsonschema import validate as json_validate
from jsonschema.exceptions import ValidationError as JsonValidationError
from rest_framework.serializers import ModelSerializer, ValidationError

from metax_api.models import File

import logging
_logger = logging.getLogger(__name__)
d = logging.getLogger(__name__).debug

class FileReadSerializer(ModelSerializer):

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
            'file_storage_id',
            'file_path',
            'identifier',
            'file_characteristics',
            'open_access',
            'replication_path',
        )

class FileWriteSerializer(ModelSerializer):

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
            'file_storage_id',
            'file_path',
            'identifier',
            'identifier_sha256',
            'file_characteristics',
            'open_access',
            'replication_path',

            # todo always include in returned values or not ?
            'modified_by_user_id',
            'modified_by_api',
            'created_by_user_id',
            'created_by_api',
        )

    def validate_file_characteristics(self, value):
        try:
            json_validate(value, self.context['view'].json_schema)
        except JsonValidationError as e:
            raise ValidationError('%s. Json field: %s, schema: %s' % (e.message, e.path[0], e.schema))
        return value

    def to_representation(self, obj):
        res = super(FileWriteSerializer, self).to_representation(obj)
        res.pop('identifier_sha256')
        return res

class FileDebugSerializer(ModelSerializer):

    """
    Used when the following query params are used in any request to /fields/:
    ?debug=true.

    Includes all fields.
    """

    class Meta:
        model = File
        fields = '__all__'
