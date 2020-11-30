# This file is part of the Metax API service
#
# Copyright 2017-2020 Ministry of Education and Culture, Finland
#
# :author: CSC - IT Center for Science Ltd., Espoo Finland <servicedesk@csc.fi>
# :license: MIT
from os import path

from rest_framework.serializers import ValidationError

from metax_api.api.rest.base.serializers import DataCatalogSerializer


class DataCatalogSerializerV2(DataCatalogSerializer):

    def is_valid(self, raise_exception=False):
        """
        Define this here that data catalogs are validated
        using JSON-schemas from V2 directory.
        """
        super(DataCatalogSerializer, self).is_valid(raise_exception=raise_exception)
        if 'catalog_json' in self.initial_data:
            self._validate_dataset_schema()
            if self.initial_data['catalog_json'].get('dataset_versioning', False) is True \
                    and self.initial_data['catalog_json'].get('harvested', False) is True:
                raise ValidationError({
                    'detail': ['versioning cannot be enabled in harvested catalogs.']
                })

    def _validate_dataset_schema(self):
        rd_schema = self.initial_data['catalog_json'].get('research_dataset_schema', None)
        if not rd_schema:
            return
        schema_path = '%s/../schemas/%s_dataset_schema.json' % (path.dirname(__file__), rd_schema)
        if not path.isfile(schema_path):
            raise ValidationError({'catalog_json': ['research dataset schema \'%s\' not found' % rd_schema]})
