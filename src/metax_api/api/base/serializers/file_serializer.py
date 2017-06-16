from jsonschema import validate as json_validate
from jsonschema.exceptions import ValidationError as JsonValidationError
from rest_framework.serializers import ModelSerializer, ValidationError

from metax_api.models import File, FileStorage
from .file_storage_serializer import FileStorageSerializer

import logging
_logger = logging.getLogger(__name__)
d = logging.getLogger(__name__).debug

class FileSerializer(ModelSerializer):

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
            'file_storage',
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
        file_storage can be one of the following. Deal with accordingly.

        Probably there would be some nice of way of leveraging serializer fields...
        """
        if self.initial_data.get('file_storage', False):
            if type(self.initial_data['file_storage']) in (int, str):
                id = self.initial_data['file_storage']
            elif isinstance(self.initial_data['file_storage'], dict):
                id = int(self.initial_data['file_storage']['id'])
            else:
                _logger.error('is_valid() field validation for file_storage: unexpected type: %s'
                              % type(self.initial_data['file_storage']))
                raise ValidationError('Validation error for field file_storage. Data in unexpected format')
            self.initial_data['file_storage'] = id
        super(FileSerializer, self).is_valid(raise_exception=raise_exception)

    def to_representation(self, data):
        res = super(FileSerializer, self).to_representation(data)
        # todo this is an extra query... (albeit qty of storages in db is tiny)
        # get FileStorage dict from context somehow ?
        fsrs = FileStorageSerializer(FileStorage.objects.get(id=res['file_storage']))
        res['file_storage'] = fsrs.data
        return res

    def validate_file_characteristics(self, value):
        try:
            json_validate(value, self.context['view'].json_schema)
        except JsonValidationError as e:
            raise ValidationError('%s. Json field: %s, schema: %s' % (e.message, e.path[0], e.schema))
        return value


class FileDebugSerializer(FileSerializer):

    """
    Used when the following query params are used in any request to /fields/:
    ?debug=true.

    Includes all fields.
    """

    class Meta:
        model = File
        fields = '__all__'
