from django.contrib.postgres.fields import JSONField
from django.db import models

from .common import Common


class DataCatalog(Common):

    catalog_json = JSONField()
    catalog_record_group_edit = models.CharField(max_length=200, blank=True, null=True, help_text='Group which is allowed to edit catalog records in the catalog.')
    catalog_record_group_create = models.CharField(max_length=200, blank=True, null=True, help_text='Group which is allowed to add new catalog records to the catalog.')

    _need_to_generate_identifier = False

    def __init__(self, *args, **kwargs):
        super(DataCatalog, self).__init__(*args, **kwargs)
        self.track_fields('catalog_json.identifier')

    def save(self, *args, **kwargs):
        if self._operation_is_create():
            self._need_to_generate_identifier = True
        else:
            if self.field_changed('catalog_json.identifier'):
                # read-only after creating
                self.catalog_json['identifier'] = self._initial_data['catalog_json']['identifier']

        super(DataCatalog, self).save(*args, **kwargs)

        if self._need_to_generate_identifier:
            self._generate_catalog_identifier()

    def _generate_catalog_identifier(self):
        """
        Get a generated value for field identifier and save it.
        Field identifier is always generated, and it can not be changed later.
        """
        self.catalog_json['identifier'] = self._generate_identifier('dc')
        super(DataCatalog, self).save()

        # save can be called several times during an object's lifetime in a request. make sure
        # not to generate identifier again.
        self._need_to_generate_identifier = False
