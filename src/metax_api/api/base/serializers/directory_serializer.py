import logging

from metax_api.models import Directory
from rest_framework.serializers import ValidationError

from .common_serializer import CommonSerializer

_logger = logging.getLogger(__name__)
d = _logger.debug


class DirectorySerializer(CommonSerializer):

    class Meta:
        model = Directory
        fields = (
            'id',
            'byte_size',
            'directory_deleted',
            'directory_modified',
            'directory_name',
            'directory_path',
            'identifier',
            'parent_directory',
            'project_identifier',
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
        if 'parent_directory' in self.initial_data:
            self.initial_data['parent_directory'] = self._get_id_from_related_object('parent_directory', self._get_parent_directory_relation)
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
                'directory path %s already exists in project %s scope. are you trying to freeze the same directory again?'
                % (self.initial_data['directory_path'], self.initial_data['project_identifier'])
            ])

        return value

    def to_representation(self, instance):
        res = super(DirectorySerializer, self).to_representation(instance)

        if instance.parent_directory:
            res['parent_directory'] = {
                'id': instance.parent_directory.id,
                'identifier': instance.parent_directory.identifier,
            }

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
