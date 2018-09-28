# This file is part of the Metax API service
#
# Copyright 2017-2018 Ministry of Education and Culture, Finland
#
# :author: CSC - IT Center for Science Ltd., Espoo Finland <servicedesk@csc.fi>
# :license: MIT

import logging

from rest_framework import serializers
from rest_framework.serializers import ValidationError
from rest_framework.validators import UniqueValidator

from metax_api.models import Directory, File, FileStorage
from .common_serializer import CommonSerializer, LightSerializer
from .directory_serializer import DirectorySerializer
from .file_storage_serializer import FileStorageSerializer
from .serializer_utils import validate_json

_logger = logging.getLogger(__name__)
d = logging.getLogger(__name__).debug


class FileSerializer(CommonSerializer):

    checksum_fields = set(['algorithm', 'checked', 'value'])

    # has to be present to not break rest_framework browsabale api
    checksum = serializers.CharField(source='checksum_value', read_only=True)

    # file paths must preserve any leading or trailing whitespace, as those
    # are valid characters in a filename. note: file_path is actually a
    # TextField, but it does not have a separate serializer field.
    file_name = serializers.CharField(max_length=None, trim_whitespace=False)
    file_path = serializers.CharField(max_length=None, trim_whitespace=False)

    # identifier is not set as unique in the db due to having to allow removed files exist.
    # use the same validator that would otherwise automatically be used, to verify uniqueness
    # among non-removed files.
    identifier = serializers.CharField(validators=[UniqueValidator(
        queryset=File.objects.all(),
        message='a file with given identifier already exists'
    )])

    class Meta:
        model = File
        fields = (
            'id',
            'byte_size',
            'checksum',
            'checksum_algorithm',
            'checksum_checked',
            'checksum_value',
            'parent_directory',
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
        ) + CommonSerializer.Meta.fields

        extra_kwargs = CommonSerializer.Meta.extra_kwargs

    def is_valid(self, raise_exception=False):
        if 'file_storage' in self.initial_data:
            self.initial_data['file_storage'] = self._get_id_from_related_object(
                'file_storage', self._get_file_storage_relation)
        if 'checksum' in self.initial_data:
            self._flatten_checksum(self.initial_data['checksum'])
        if 'parent_directory' in self.initial_data:
            self.initial_data['parent_directory'] = self._get_id_from_related_object(
                'parent_directory', self._get_parent_directory_relation)
        super(FileSerializer, self).is_valid(raise_exception=raise_exception)

    def to_representation(self, instance):
        res = super(FileSerializer, self).to_representation(instance)

        if 'file_storage' in res:
            if self.expand_relation_requested('file_storage'):
                res['file_storage'] = FileStorageSerializer(instance.file_storage).data
            else:
                res['file_storage'] = {
                    'id': instance.file_storage.id,
                    'identifier': instance.file_storage.file_storage_json['identifier'],
                }

        if 'parent_directory' in res:
            if self.expand_relation_requested('parent_directory'):
                res['parent_directory'] = DirectorySerializer(instance.parent_directory).data
            else:
                res['parent_directory'] = {
                    'id': instance.parent_directory.id,
                    'identifier': instance.parent_directory.identifier,
                }

        if not self.requested_fields or 'checksum' in self.requested_fields:
            res['checksum'] = self._form_checksum(res)

        return res

    def validate_file_characteristics(self, value):
        validate_json(value, self.context['view'].json_schema)
        return value

    def validate_file_path(self, value):
        """
        Ensure file_path is unique in the project, within unremoved files.
        file_path can exist multiple times for removed files though.
        """
        if hasattr(self, 'file_path_checked'):
            # has been previously validated during bulk operation processing.
            # saves a fetch to the db.
            return value

        if self._operation_is_create:
            if 'project_identifier' not in self.initial_data:
                # the validation for project_identifier is executed later...
                return value

            project = self.initial_data['project_identifier']
            if File.objects.filter(project_identifier=project, file_path=value).exists():
                raise ValidationError('a file with path %s already exists in project %s' % (value, project))

        elif self._operation_is_update:
            if 'file_path' not in self.initial_data:
                return value
            if self.instance.file_path != self.initial_data['file_path']:
                # would require re-arranging the virtual file tree... implement in the future if need arises
                raise ValidationError('file_path can not be changed after creating')
        else:
            # delete
            pass

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
        for key in self.checksum_fields:
            if key in checksum:
                self.initial_data['checksum_%s' % key] = checksum[key]

    def _form_checksum(self, file_data):
        """
        Do the opposite of _flatten_checksum(). Take model-level checksum-fields
        and form a "relation checksum" to return from the api.
        """
        checksum = {}
        for key in self.checksum_fields:
            checksum_field = 'checksum_%s' % key
            if checksum_field in file_data:
                checksum[key] = file_data[checksum_field]
                file_data.pop(checksum_field)
        return checksum

    def _get_parent_directory_relation(self, identifier_value):
        """
        Passed to _get_id_from_related_object() to be used when relation was a string identifier
        """
        if isinstance(identifier_value, dict):
            identifier_value = identifier_value['identifier']
        try:
            return Directory.objects.get(identifier=identifier_value).id
        except Directory.DoesNotExist:
            raise ValidationError({ 'parent_directory': ['identifier %s not found' % str(identifier_value)]})


class LightFileSerializer(LightSerializer):

    allowed_fields = set(FileSerializer.Meta.fields)
    relation_fields = set(['file_storage', 'parent_directory'])
    special_fields = set([ 'checksum_%s' % field for field in FileSerializer.checksum_fields ])

    @classmethod
    def ls_field_list(cls, received_field_list=[]):
        """
        Make requested fields compatible with LightSerializer use.
        """
        field_list = super().ls_field_list(received_field_list)
        if not received_field_list:
            # a field of this name does not exist in the db - remove or will cause crash
            field_list.discard('checksum')

        return_file_fields = []
        for field in field_list:
            if field not in cls.allowed_fields:
                continue
            elif field == 'checksum':
                for checksum_field in FileSerializer.checksum_fields:
                    return_file_fields.append('checksum_%s' % checksum_field)
                break
            elif field in cls.relation_fields:
                if field == 'file_storage':
                    return_file_fields.append('%s__file_storage_json' % field)
                    return_file_fields.append('%s__id' % field)
                else:
                    # parent directory
                    return_file_fields.append('%s__identifier' % field)
                    return_file_fields.append('%s__id' % field)
            else:
                return_file_fields.append(field)

        return return_file_fields

    @staticmethod
    def serialize_special_field(file, field, value):
        if field.startswith('checksum'):
            try:
                file['checksum'][field.split('_')[1]] = value
            except KeyError:
                file['checksum'] = { field.split('_')[1]: value }
