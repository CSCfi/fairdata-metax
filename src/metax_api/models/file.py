# This file is part of the Metax API service
#
# Copyright 2017-2018 Ministry of Education and Culture, Finland
#
# :author: CSC - IT Center for Science Ltd., Espoo Finland <servicedesk@csc.fi>
# :license: MIT

from django.db import models
from django.db.models import JSONField
from rest_framework.serializers import ValidationError

from .common import Common, CommonManager


class FileManager(CommonManager):

    def get(self, *args, **kwargs):
        if kwargs.get('using_dict', None):
            # for a simple "just get me the instance that equals this dict i have" search.

            # this is useful if during a request the url does not contain the identifier (bulk update),
            # and in generic operations where the type of object being handled is not known (also bulk operations).
            row = kwargs.pop('using_dict')
            if row.get('id', None):
                kwargs['id'] = row['id']
            elif row.get('identifier', None):
                kwargs['identifier'] = row['identifier']
            else:
                raise ValidationError([
                    'this operation requires one of the following identifying keys to be present: %s'
                    % ', '.join([ 'id', 'identifier' ])])
        return super(FileManager, self).get(*args, **kwargs)


class File(Common):

    # MODEL FIELD DEFINITIONS #

    byte_size = models.BigIntegerField(default=0)
    checksum_algorithm = models.CharField(max_length=200)
    checksum_checked = models.DateTimeField()
    checksum_value = models.TextField()
    file_characteristics = JSONField(blank=True, null=True)
    file_characteristics_extension = JSONField(blank=True, null=True)
    file_deleted = models.DateTimeField(null=True)
    file_frozen = models.DateTimeField()
    file_format = models.CharField(max_length=200, null=True)
    file_modified = models.DateTimeField()
    file_name = models.TextField()
    file_path = models.TextField()
    file_storage = models.ForeignKey('metax_api.FileStorage', on_delete=models.DO_NOTHING)
    file_uploaded = models.DateTimeField()
    identifier = models.CharField(max_length=200)
    open_access = models.BooleanField(default=False)
    parent_directory = models.ForeignKey('metax_api.Directory', on_delete=models.SET_NULL, null=True,
        related_name='files')
    project_identifier = models.CharField(max_length=200)

    # END OF MODEL FIELD DEFINITIONS #

    class Meta:
        indexes = [
            models.Index(fields=['file_path']),
            models.Index(fields=['identifier']),
            models.Index(fields=['parent_directory']),
            models.Index(fields=['project_identifier']),
        ]

    objects = FileManager()

    def user_has_access(self, request):
        if request.user.is_service:
            return True
        from metax_api.services import AuthService
        return self.project_identifier in AuthService.get_user_projects(request)

    def __repr__(self):
        return '<%s: %d, removed: %s, project_identifier: %s, identifier: %s, file_path: %s >' % (
            'File',
            self.id,
            str(self.removed),
            self.project_identifier,
            self.identifier,
            self.file_path
        )

    def delete(self):
        super(File, self).remove()