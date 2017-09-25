from django.db import models

from .common import Common
from .file import File

class XmlMetadata(Common):

    namespace = models.CharField(max_length=200)
    xml = models.CharField(max_length=200)
    file_id = models.ForeignKey(File, db_column='file_id')

    class Meta:
        unique_together = ('namespace', 'file_id')

    indexes = [
        models.Index(fields=['namespace']),
    ]
