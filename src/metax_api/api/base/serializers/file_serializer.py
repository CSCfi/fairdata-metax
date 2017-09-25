from rest_framework.serializers import ValidationError

from metax_api.models import File, FileStorage
from .common_serializer import CommonSerializer
from .file_storage_serializer import FileStorageSerializer
from .serializer_utils import validate_json
import logging
_logger = logging.getLogger(__name__)
d = logging.getLogger(__name__).debug


class FileSerializer(CommonSerializer):

    _checksum_fields = ['algorithm', 'checked', 'value']

    class Meta:
        model = File
        fields = (
            'id',
            'byte_size',
            'checksum_algorithm',
            'checksum_checked',
            'checksum_value',
            'download_url',
            'file_deleted',
            'file_frozen',
            'file_format',
            'file_modified',
            'file_name',
            'file_path',
            'file_storage',
            'file_uploaded',
            'identifier',
            'file_characteristics',
            'file_characteristics_extension',
            'open_access',
            'project_identifier',
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

        if 'checksum' in self.initial_data:
            self._flatten_checksum(self.initial_data['checksum'])

        super(FileSerializer, self).is_valid(raise_exception=raise_exception)

    def to_representation(self, data):
        res = super(FileSerializer, self).to_representation(data)
        # todo this is an extra query... (albeit qty of storages in db is tiny)
        # get FileStorage dict from context somehow ?
        fsrs = FileStorageSerializer(FileStorage.objects.get(id=res['file_storage']))
        res['file_storage'] = fsrs.data

        res['checksum'] = self._form_checksum(res)
        if not res['checksum']:
            # dont return fields with null values
            res.pop('checksum')

        return res

    def validate_file_characteristics(self, value):
        validate_json(value, self.context['view'].json_schema)
        return value

    def _flatten_checksum(self, checksum):
        """
        Note: not a real field in the model, but a representation-thing for api users.
        Flatten values from "relation checksum" to model-level fields:

        'checksum': {
            'value': val,
            ...
        }

        ->

        'checksum_value': val

        """
        for key in self._checksum_fields:
            if key in checksum:
                self.initial_data['checksum_%s' % key] = checksum[key]

    def _form_checksum(self, file_data):
        """
        Do the opposite of _flatten_checksum(). Take model-level checksum-fields
        and form a "relation checksum" to return from the api.
        """
        checksum = {}
        for key in self._checksum_fields:
            checksum_field = 'checksum_%s' % key
            if checksum_field in file_data:
                checksum[key] = file_data[checksum_field]
                file_data.pop(checksum_field)
        return checksum
