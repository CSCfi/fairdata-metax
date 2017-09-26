from rest_framework import serializers
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

    # has to be present to not break rest_framework browsabale api
    checksum = serializers.CharField(source='checksum_value', read_only=True)

    class Meta:
        model = File
        fields = (
            'id',
            'byte_size',
            'checksum',
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
        if 'file_storage' in self.initial_data:
            self.initial_data['file_storage'] = self._get_id_from_related_object('file_storage', self._get_file_storage_relation)
        if 'checksum' in self.initial_data:
            self._flatten_checksum(self.initial_data['checksum'])

        super(FileSerializer, self).is_valid(raise_exception=raise_exception)

    def to_representation(self, data):
        res = super(FileSerializer, self).to_representation(data)
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

    def _get_file_storage_relation(self, identifier_value):
        """
        Passed to _get_id_from_related_object() to be used when relation was a string identifier
        """
        if isinstance(identifier_value, dict):
            identifier_value = identifier_value['file_storage_json']['identifier']
        try:
            return FileStorage.objects.get(
                file_storage_json__contains={ 'identifier': identifier_value }
            ).id
        except FileStorage.DoesNotExist:
            raise ValidationError({ 'file_storage': ['identifier %s not found' % str(identifier_value)]})

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
