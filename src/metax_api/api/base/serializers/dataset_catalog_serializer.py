from jsonschema import validate as json_validate
from jsonschema.exceptions import ValidationError as JsonValidationError
from rest_framework.serializers import ModelSerializer, ValidationError

from metax_api.models import DatasetCatalog

class DatasetCatalogSerializer(ModelSerializer):

    class Meta:
        model = DatasetCatalog
        fields = (
            'id',
            'catalog_json',
            'catalog_record_group_edit',
            'catalog_record_group_create',
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

    def validate_catalog_json(self, value):
        try:
            json_validate(value, self.context['view'].json_schema)
        except JsonValidationError as e:
            raise ValidationError('%s. Json field: %s, schema: %s' % (e.message, e.path[0], e.schema))
        return value
