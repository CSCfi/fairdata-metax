from django.contrib.postgres.fields import JSONField
from django.db import models
from rest_framework.serializers import ValidationError

from .common import Common, CommonManager
from .directory import Directory
from .file_storage import FileStorage


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

    byte_size = models.PositiveIntegerField(default=0)
    checksum_algorithm = models.CharField(max_length=200)
    checksum_checked = models.DateTimeField()
    checksum_value = models.TextField()
    download_url = models.URLField(null=True)
    file_characteristics = JSONField(blank=True, null=True)
    file_characteristics_extension = JSONField(blank=True, null=True)
    file_deleted = models.DateTimeField(null=True)
    file_frozen = models.DateTimeField()
    file_format = models.CharField(max_length=200)
    file_modified = models.DateTimeField(auto_now=True)
    file_name = models.CharField(max_length=200)
    file_path = models.TextField()
    file_storage = models.ForeignKey(FileStorage, on_delete=models.DO_NOTHING)
    file_uploaded = models.DateTimeField()
    identifier = models.CharField(max_length=200, unique=True)
    open_access = models.BooleanField(default=False)
    parent_directory = models.ForeignKey(Directory, on_delete=models.SET_NULL, null=True, related_name='files')
    project_identifier = models.CharField(max_length=200)
    replication_path = models.CharField(max_length=200, blank=True, null=True)

    # END OF MODEL FIELD DEFINITIONS #

    indexes = [
        models.Index(fields=['identifier']),
    ]

    objects = FileManager()

    def __repr__(self):
        return '<%s: %d, removed: %s, project_identifier: %s, identifier: %s, file_path: %s >' % (
            'File',
            self.id,
            str(self.removed),
            self.project_identifier,
            self.identifier,
            self.file_path
        )
