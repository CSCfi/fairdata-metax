from django.contrib.postgres.fields import JSONField
from django.db import models

from .common import Common
from .dataset_catalog import DatasetCatalog

class Dataset(Common):

    identifier = models.CharField(max_length=200, unique=True)
    dataset_json = JSONField(blank=True, null=True)
    dataset_catalog_id = models.ForeignKey(DatasetCatalog, db_column='dataset_catalog_id', null=True, blank=True)

    indexes = [
        models.Index(fields=['identifier']),
    ]
