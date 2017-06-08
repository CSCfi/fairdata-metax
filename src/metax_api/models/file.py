from django.contrib.postgres.fields import JSONField
from django.db import models

from .common import Common
from .file_storage import FileStorage

class File(Common):

    access_group = models.CharField(max_length=200)
    byte_size = models.PositiveIntegerField(default=0)
    checksum_algorithm = models.CharField(max_length=200)
    checksum_checked = models.DateTimeField(null=True)
    checksum_value = models.CharField(max_length=200)
    download_url = models.URLField()
    file_format = models.CharField(max_length=200)
    file_modified = models.DateTimeField(auto_now=True)
    file_name = models.CharField(max_length=64)
    file_storage_id = models.ForeignKey(FileStorage, db_column='file_storage_id', related_name='files')
    file_path = models.CharField(max_length=200)
    identifier = models.CharField(max_length=200, unique=True)
    file_characteristics = JSONField(blank=True, null=True)
    open_access = models.BooleanField(default=False)
    replication_path = models.CharField(max_length=200, blank=True, null=True)

    indexes = [
        models.Index(fields=['identifier']),
    ]
