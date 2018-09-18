# This file is part of the Metax API service
#
# Copyright 2017-2018 Ministry of Education and Culture, Finland
#
# :author: CSC - IT Center for Science Ltd., Espoo Finland <servicedesk@csc.fi>
# :license: MIT

import logging

from rest_framework.serializers import ValidationError
from rest_framework import serializers

from metax_api.models import Directory
from metax_api.services import FileService as FS
from .common_serializer import CommonSerializer

_logger = logging.getLogger(__name__)
d = _logger.debug


class DirectorySerializer(CommonSerializer):

    # directory paths must preserve any leading or trailing whitespace, as those
    # are valid characters in a filename. note: directory_path is actually a
    # TextField, but it does not have a separate serializer field.
    directory_name = serializers.CharField(max_length=None, trim_whitespace=False)
    directory_path = serializers.CharField(max_length=None, trim_whitespace=False)

    class Meta:
        model = Directory
        fields = (
            'id',
            'byte_size',
            'directory_deleted',
            'directory_modified',
            'directory_name',
            'directory_path',
            'file_count',
            'identifier',
            'parent_directory',
            'project_identifier',
            'user_modified',
            'date_modified',
            'user_created',
            'date_created',
        ) + CommonSerializer.Meta.fields

        extra_kwargs = CommonSerializer.Meta.extra_kwargs

    def is_valid(self, raise_exception=False):
        if 'parent_directory' in self.initial_data:
            self.initial_data['parent_directory'] = self._get_id_from_related_object(
                'parent_directory', self._get_parent_directory_relation)
        super(DirectorySerializer, self).is_valid(raise_exception=raise_exception)

    def validate_directory_path(self, value):
        """
        Check that a directory being created does not already exist in project UNREMOVED
        directories scope.

        There can however exist multiple directories with same path with removed=True,
        since directories can be frozen and unfrozen multiple times.
        """
        dir_exists_in_project_scope = Directory.objects.filter(
            project_identifier=self.initial_data['project_identifier'],
            directory_path=self.initial_data['directory_path']
        ).exists()

        if dir_exists_in_project_scope:
            raise ValidationError([
                'directory path %s already exists in project %s scope. Are you trying to freeze same directory again?'
                % (self.initial_data['directory_path'], self.initial_data['project_identifier'])
            ])

        return value

    def to_representation(self, instance):
        res = super(DirectorySerializer, self).to_representation(instance)

        if 'parent_directory' in res and instance.parent_directory:
            res['parent_directory'] = {
                'id': instance.parent_directory.id,
                'identifier': instance.parent_directory.identifier,
            }

        if self.strip_sensitive_fields:
            res = FS.strip_directory(res)

        return res

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
