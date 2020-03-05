# This file is part of the Metax API service
#
# Copyright 2017-2018 Ministry of Education and Culture, Finland
#
# :author: CSC - IT Center for Science Ltd., Espoo Finland <servicedesk@csc.fi>
# :license: MIT

from os import path

from rest_framework.serializers import ValidationError

from metax_api.models import DataCatalog
from metax_api.services import DataCatalogService as DCS
from .common_serializer import CommonSerializer
from .serializer_utils import validate_json


class DataCatalogSerializer(CommonSerializer):

    class Meta:
        model = DataCatalog
        fields = (
            'id',
            'catalog_json',
            'catalog_record_group_edit',
            'catalog_record_group_create',
            'catalog_record_services_edit',
            'catalog_record_services_create',
        ) + CommonSerializer.Meta.fields

        extra_kwargs = CommonSerializer.Meta.extra_kwargs

    def is_valid(self, raise_exception=False):
        super(DataCatalogSerializer, self).is_valid(raise_exception=raise_exception)
        if 'catalog_json' in self.initial_data:
            self._validate_dataset_schema()
            if self.initial_data['catalog_json'].get('dataset_versioning', False) is True \
                    and self.initial_data['catalog_json'].get('harvested', False) is True:
                raise ValidationError({
                    'detail': ['versioning cannot be enabled in harvested catalogs.']
                })

    def validate_catalog_json(self, value):
        validate_json(value, self.context['view'].json_schema)
        DCS.validate_reference_data(value, self.context['view'].cache)
        # ensure ref data validation/population did not break anything
        validate_json(value, self.context['view'].json_schema)
        if self._operation_is_create:
            self._validate_identifier_uniqueness(value)
        return value

    def _validate_dataset_schema(self):
        rd_schema = self.initial_data['catalog_json'].get('research_dataset_schema', None)
        if not rd_schema:
            return
        schema_path = '%s/../schemas/%s_dataset_schema.json' % (path.dirname(__file__), rd_schema)
        if not path.isfile(schema_path):
            raise ValidationError({'catalog_json': ['research dataset schema \'%s\' not found' % rd_schema]})

    def _validate_identifier_uniqueness(self, catalog_json):
        if DataCatalog.objects.filter(catalog_json__identifier=catalog_json['identifier']).exists():
            raise ValidationError({'identifier':
                ['identifier %s already exists' % catalog_json['identifier']]
            })
