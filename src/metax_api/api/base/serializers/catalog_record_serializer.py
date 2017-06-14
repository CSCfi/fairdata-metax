from uuid import UUID

# validation disabled until schema is updated
# from jsonschema import validate as json_validate
# from jsonschema.exceptions import ValidationError as JsonValidationError
from rest_framework.serializers import ModelSerializer, ValidationError

from metax_api.models import CatalogRecord, DatasetCatalog, File
from .dataset_catalog_serializer import DatasetCatalogReadSerializer

import logging
_logger = logging.getLogger(__name__)
d = logging.getLogger(__name__).debug

class CatalogRecordReadSerializer(ModelSerializer):

    class Meta:
        model = CatalogRecord
        fields = (
            'id',
            'identifier',
            'research_dataset',
            'dataset_catalog',
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
        if self.initial_data.get('dataset_catalog', False):
            if isinstance(self.initial_data['dataset_catalog'], str):
                uuid_obj = UUID(self.initial_data['dataset_catalog'])
            elif isinstance(self.initial_data['dataset_catalog'], dict):
                uuid_obj = UUID(self.initial_data['dataset_catalog']['id'])
            elif isinstance(self.initial_data['dataset_catalog'], UUID):
                uuid_obj = self.initial_data['dataset_catalog']
            else:
                _logger.error('is_valid() field validation for dataset_catalog: unexpected type: %s'
                              % type(self.initial_data['dataset_catalog']))
                raise ValidationError('Validation error for field dataset_catalog. Data in unexpected format')
            self.initial_data['dataset_catalog'] = uuid_obj
        super(CatalogRecordReadSerializer, self).is_valid(raise_exception=raise_exception)

    def update(self, instance, validated_data):
        instance = super(CatalogRecordReadSerializer, self).update(instance, validated_data)
        files_dict = validated_data.get('research_dataset', None) and validated_data['research_dataset'].get('files', None) or None
        if files_dict:
            file_pids = [ f['identifier'] for f in files_dict ]
            files = File.objects.filter(identifier__in=file_pids)
            instance.files.clear()
            instance.files.add(*files)
            instance.save()
        return instance

    def create(self, validated_data):
        instance = super(CatalogRecordReadSerializer, self).create(validated_data)
        files_dict = validated_data['research_dataset']['files'].copy()
        file_pids = [ f['identifier'] for f in files_dict ]
        files = File.objects.filter(identifier__in=file_pids)
        instance.files.add(*files)
        instance.save()
        return instance

    def to_representation(self, data):
        res = super(CatalogRecordReadSerializer, self).to_representation(data)
        # todo this is an extra query... (albeit qty of storages in db is tiny)
        # get FileStorage dict from context somehow ?
        fsrs = DatasetCatalogReadSerializer(DatasetCatalog.objects.get(id=res['dataset_catalog']))
        res['dataset_catalog'] = fsrs.data
        return res

    def validate_research_dataset(self, value):
        # todo enable validation until json schema is somewhat stable again
        return value
        # try:
        #     json_validate(value, self.context['view'].json_schema)
        # except JsonValidationError as e:
        #     raise ValidationError('%s. Json field: %s, schema: %s' % (e.message, e.path[0], e.schema))
        # return value
