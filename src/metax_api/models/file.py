from django.contrib.postgres.fields import JSONField
from django.db import models

from .common import Common
from .file_storage import FileStorage

class File(Common):

    byte_size = models.PositiveIntegerField(default=0)
    checksum_algorithm = models.CharField(max_length=200)
    checksum_checked = models.DateTimeField(null=True)
    checksum_value = models.CharField(max_length=200)
    download_url = models.URLField()
    file_deleted = models.DateTimeField(null=True)
    file_frozen = models.DateTimeField(null=True)
    file_format = models.CharField(max_length=200)
    file_modified = models.DateTimeField(auto_now=True)
    file_name = models.CharField(max_length=64)
    file_path = models.CharField(max_length=200)
    file_storage = models.ForeignKey(FileStorage)
    file_uploaded = models.DateTimeField(null=True)
    identifier = models.CharField(max_length=200, unique=True)
    file_characteristics = JSONField(blank=True, null=True)
    file_characteristics_extension = JSONField(blank=True, null=True)
    open_access = models.BooleanField(default=False, help_text='For backwards compatibility with old IDA')
    project_identifier = models.CharField(max_length=200)
    replication_path = models.CharField(max_length=200, blank=True, null=True)

    indexes = [
        models.Index(fields=['identifier']),
    ]
