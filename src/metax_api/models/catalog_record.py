from django.contrib.postgres.fields import JSONField
from django.db import models

from .common import Common
from .file import File
from .dataset_catalog import DatasetCatalog

class CatalogRecord(Common):

    identifier = models.CharField(max_length=200, unique=True)
    research_dataset = JSONField()
    dataset_catalog = models.ForeignKey(DatasetCatalog)
    files = models.ManyToManyField(File)

    indexes = [
        models.Index(fields=['identifier']),
    ]
