from rest_framework.serializers import ModelSerializer
from metax_api.models import DatasetCatalog
from .serializer_utils import validate_json

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
        validate_json(value, self.context['view'].json_schema)
        return value
