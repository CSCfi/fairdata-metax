from django.contrib.postgres.fields import JSONField
from django.db import models

from .common import Common

class DatasetCatalog(Common):

    catalog_json = JSONField(blank=True, null=True)
    catalog_record_group_edit = models.CharField(max_length=200, blank=True, null=True, help_text='Group which is allowed to edit catalog records in the catalog.')
    catalog_record_group_create = models.CharField(max_length=200, blank=True, null=True, help_text='Group which is allowed to add new catalog records to the catalog.')
