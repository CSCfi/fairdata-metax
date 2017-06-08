from django.contrib.postgres.fields import JSONField

from .common import Common

class FileStorage(Common):

    file_storage_json = JSONField(blank=True, null=True)

    class Meta:
        db_table = 'metax_api_file_storage'
