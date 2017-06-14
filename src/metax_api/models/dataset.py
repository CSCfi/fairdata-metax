from django.contrib.postgres.fields import JSONField
from django.db import models

from .common import Common
from .file import File
from .dataset_catalog import DatasetCatalog

class Dataset(Common):

    identifier = models.CharField(max_length=200, unique=True)
    dataset_json = JSONField()
    dataset_catalog_id = models.ForeignKey(DatasetCatalog, db_column='dataset_catalog_id')
    files = models.ManyToManyField(File)

    indexes = [
        models.Index(fields=['identifier']),
    ]
