from metax_api.models import DataCatalog
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
        self._validate_json_schema(value)
        return value

    def _validate_json_schema(self, value):
        if self._operation_is_create():
            # identifier cant be provided by the user, but it is a required field =>
            # add identifier temporarily to pass schema validation. proper value
            # will be generated later in model save().
            value['identifier'] = 'temp'
            validate_json(value, self.context['view'].json_schema)
            value.pop('identifier')
        else:
            # update operations
            validate_json(value, self.context['view'].json_schema)
