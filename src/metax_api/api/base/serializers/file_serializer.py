from uuid import UUID

from jsonschema import validate as json_validate
from jsonschema.exceptions import ValidationError as JsonValidationError
from rest_framework.serializers import ModelSerializer, ValidationError

from metax_api.models import File, FileStorage

import logging
_logger = logging.getLogger(__name__)
d = logging.getLogger(__name__).debug

# todo to common serializers probably
class HttpReprMixin():

    @property
    def http_repr(self):
        """
        Used to serialize the fields in a serializers to the form below
        """
        obj = { 'object': self.data }
        if self.errors:
            obj['errors'] = self.errors
        return obj

class FileStorageReadSerializer(ModelSerializer):

    class Meta:
        model = FileStorage
        fields = (
            'id',
            'file_storage_json',
        )

class FileReadSerializer(ModelSerializer):

    file_storage_id = FileStorageReadSerializer(read_only=True)

    class Meta:
        model = File
        fields = (
            'id',
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
            'modified_by_user_id',
            'modified_by_api',
            'created_by_user_id',
            'created_by_api',
        )

class FileWriteSerializer(ModelSerializer, HttpReprMixin):

    class Meta:
        model = File
        fields = (
            'id',
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
            'modified_by_user_id',
            'modified_by_api',
            'created_by_user_id',
            'created_by_api',
        )
        extra_kwargs = {
            # not required during creation, or updating
            # they would be overwritten by the api anyway
            'modified_by_user_id': { 'required': False },
            'modified_by_api': { 'required': False },
            'created_by_user_id': { 'required': False },
            'created_by_api': { 'required': False },
        }

    def is_valid(self, raise_exception=False):
        """
        Different kind of validations are done at different times, and the type of
        file_storage_id can be one of the following. Deal with accordingly.

        Probably there would be some nice of way of leveraging serializer fields...
        """
        if self.initial_data.get('file_storage_id', False):
            if isinstance(self.initial_data['file_storage_id'], str):
                uuid_obj = UUID(self.initial_data['file_storage_id'])
            elif isinstance(self.initial_data['file_storage_id'], dict):
                uuid_obj = UUID(self.initial_data['file_storage_id']['id'])
            elif isinstance(self.initial_data['file_storage_id'], UUID):
                uuid_obj = self.initial_data['file_storage_id']
            else:
                _logger.error('is_valid() field validation for file_storage: unexpected type for file_storage_id: %s'
                              % type(self.initial_data['file_storage_id']))
                raise ValidationError('Validation error for field filed_storage_id. Data in unexpected format')
            self.initial_data['file_storage_id'] = uuid_obj
        super(FileWriteSerializer, self).is_valid(raise_exception=raise_exception)

    # def update(self, instance, validated_data):
    #     # todo looks like not needed?
    #     file_storage_id = validated_data.pop('file_storage_id', False)
    #     if file_storage_id:
    #         instance.file_storage_id = file_storage_id
    #     return super(FileWriteSerializer, self).update(instance, validated_data)

    def to_representation(self, data):
        res = super(FileWriteSerializer, self).to_representation(data)
        fsrs = FileStorageReadSerializer(FileStorage.objects.get(id=res['file_storage_id']))
        res['file_storage_id'] = fsrs.data
        return res

    def validate_file_characteristics(self, value):
        try:
            json_validate(value, self.context['view'].json_schema)
        except JsonValidationError as e:
            raise ValidationError('%s. Json field: %s, schema: %s' % (e.message, e.path[0], e.schema))
        return value

class FileDebugSerializer(ModelSerializer, HttpReprMixin):

    """
    Used when the following query params are used in any request to /fields/:
    ?debug=true.

    Includes all fields.
    """

    class Meta:
        model = File
        fields = '__all__'
