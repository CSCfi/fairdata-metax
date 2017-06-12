from uuid import UUID

from jsonschema import validate as json_validate
from jsonschema.exceptions import ValidationError as JsonValidationError
from rest_framework.serializers import ModelSerializer, ValidationError

from metax_api.models import Dataset, DatasetCatalog
from .dataset_catalog_serializer import DatasetCatalogReadSerializer

import logging
_logger = logging.getLogger(__name__)
d = logging.getLogger(__name__).debug

class DatasetReadSerializer(ModelSerializer):

    class Meta:
        model = Dataset
        fields = (
            'id',
            'identifier',
            'dataset_json',
            'dataset_catalog_id',
            'modified_by_user_id',
            'modified_by_api',
            'created_by_user_id',
            'created_by_api',
        )
        extra_kwargs = {
            # not required during creation, or updating
            # they would be overwritten by the api anyway
            'modified_by_user_id': { 'required': False },
            'modified_by_api': { 'required': False },
            'created_by_user_id': { 'required': False },
            'created_by_api': { 'required': False },
        }

    def is_valid(self, raise_exception=False):
        if self.initial_data.get('dataset_catalog_id', False):
            if isinstance(self.initial_data['dataset_catalog_id'], str):
                uuid_obj = UUID(self.initial_data['dataset_catalog_id'])
            elif isinstance(self.initial_data['dataset_catalog_id'], dict):
                uuid_obj = UUID(self.initial_data['dataset_catalog_id']['id'])
            elif isinstance(self.initial_data['dataset_catalog_id'], UUID):
                uuid_obj = self.initial_data['dataset_catalog_id']
            else:
                _logger.error('is_valid() field validation for dataset_catalog_id: unexpected type: %s'
                              % type(self.initial_data['dataset_catalog_id']))
                raise ValidationError('Validation error for field dataset_catalog_id. Data in unexpected format')
            self.initial_data['dataset_catalog_id'] = uuid_obj
        super(DatasetReadSerializer, self).is_valid(raise_exception=raise_exception)

    def to_representation(self, data):
        res = super(DatasetReadSerializer, self).to_representation(data)
        # todo this is an extra query... (albeit qty of storages in db is tiny)
        # get FileStorage dict from context somehow ?
        fsrs = DatasetCatalogReadSerializer(DatasetCatalog.objects.get(id=res['dataset_catalog_id']))
        res['dataset_catalog_id'] = fsrs.data
        return res

    def validate_dataset_json(self, value):
        try:
            json_validate(value, self.context['view'].json_schema)
        except JsonValidationError as e:
            raise ValidationError('%s. Json field: %s, schema: %s' % (e.message, e.path[0], e.schema))
        return value
