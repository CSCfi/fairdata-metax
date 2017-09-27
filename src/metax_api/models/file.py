from django.contrib.postgres.fields import JSONField
from django.db import connection, models
from rest_framework.serializers import ValidationError

from .common import Common, CommonManager
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
                raise ValidationError(['this operation requires one of the following identifying keys to be present: %s' % ', '.join([ 'id', 'identifier' ])])
        return super(FileManager, self).get(*args, **kwargs)

    def delete_directory(self, directory_path):
        """
        For every File whose file_path.startswith(directory_path),
            set removed=True,
            and for every dataset this file belongs to,
                set flag removed_in_ida=True in file in dataset list
        """
        affected_files = 0

        sql = 'update metax_api_file set removed = true ' \
              'where removed = false ' \
              'and active = true ' \
              'and (file_path = %s or file_path like %s)'

        with connection.cursor() as cr:
            cr.execute(sql, [directory_path, '%s/%%' % directory_path])
            affected_files = cr.rowcount

        return affected_files


class File(Common):

    byte_size = models.PositiveIntegerField(default=0)
    checksum_algorithm = models.CharField(max_length=200)
    checksum_checked = models.DateTimeField()
    checksum_value = models.CharField(max_length=200)
    download_url = models.URLField(null=True)
    file_characteristics = JSONField(blank=True, null=True)
    file_characteristics_extension = JSONField(blank=True, null=True)
    file_deleted = models.DateTimeField(null=True)
    file_frozen = models.DateTimeField()
    file_format = models.CharField(max_length=200)
    file_modified = models.DateTimeField(auto_now=True)
    file_name = models.CharField(max_length=64)
    file_path = models.CharField(max_length=200)
    file_storage = models.ForeignKey(FileStorage)
    file_uploaded = models.DateTimeField()
    identifier = models.CharField(max_length=200, unique=True)
    open_access = models.BooleanField(default=False)
    project_identifier = models.CharField(max_length=200)
    replication_path = models.CharField(max_length=200, blank=True, null=True)

    indexes = [
        models.Index(fields=['identifier']),
    ]

    objects = FileManager()
