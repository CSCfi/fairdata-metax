from django.contrib.postgres.fields import JSONField
from django.db import models

from .common import Common

class File(Common):

    # todo add to serializer

    access_group = models.CharField(max_length=200, blank=True)
    byte_size = models.PositiveIntegerField(default=0)
    checksum_algorithm = models.CharField(max_length=200, blank=True)
    checksum_checked = models.DateTimeField(null=True)
    checksum_value = models.CharField(max_length=200, blank=True)
    download_url = models.URLField(blank=True)
    file_format = models.CharField(max_length=200, blank=True)
    file_modified = models.DateTimeField(auto_now=True)
    file_name = models.CharField(max_length=64, blank=True, null=True)
    # file_storage_id = models.ForeignKey('FileStorage')
    file_path = models.CharField(max_length=200, blank=True)
    json = JSONField(blank=True, null=True)
    open_access = models.BooleanField(default=False)
    removed = models.BooleanField(default=False)
    replication_path = models.CharField(max_length=200, blank=True)
