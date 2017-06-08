from django.contrib.postgres.fields import JSONField

from .common import Common

class DatasetCatalog(Common):

    catalog_json = JSONField(blank=True, null=True)
