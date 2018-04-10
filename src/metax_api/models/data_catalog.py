from django.contrib.postgres.fields import JSONField
from django.db import models

from .common import Common


class DataCatalog(Common):

    # MODEL FIELD DEFINITIONS #

    catalog_json = JSONField()

    catalog_record_group_edit = models.CharField(
        max_length=200, blank=True, null=True,
        help_text='Group which is allowed to edit catalog records in the catalog.')

    catalog_record_group_create = models.CharField(
        max_length=200, blank=True, null=True,
        help_text='Group which is allowed to add new catalog records to the catalog.')

    # END OF MODEL FIELD DEFINITIONS #

    def __init__(self, *args, **kwargs):
        super(DataCatalog, self).__init__(*args, **kwargs)
        self.track_fields('catalog_json.identifier')

    def save(self, *args, **kwargs):
        if self._operation_is_update():
            if self.field_changed('catalog_json.identifier'):
                # read-only after creating
                self.catalog_json['identifier'] = self._initial_data['catalog_json']['identifier']

        super(DataCatalog, self).save(*args, **kwargs)

    def print_records(self): # pragma: no cover
        for r in self.records.all():
            print(r)

    def __repr__(self):
        return '<%s: %d, removed: %s, identifier: %s, research_dataset_schema=%s, dataset_versioning: %s >' % (
            'DataCatalog',
            self.id,
            str(self.removed),
            self.catalog_json['identifier'],
            self.catalog_json['research_dataset_schema'],
            self.catalog_json['dataset_versioning'],
        )
