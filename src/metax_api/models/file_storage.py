from django.contrib.postgres.fields import JSONField

from .common import Common

class FileStorage(Common):

    file_storage_json = JSONField(blank=True, null=True)
