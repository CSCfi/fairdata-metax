from jsonschema import validate as json_validate
from jsonschema.exceptions import ValidationError as JsonValidationError
from rest_framework.serializers import ModelSerializer, ValidationError

from metax_api.models import Contract

class ContractSerializer(ModelSerializer):

    class Meta:
        model = Contract
        fields = (
            'id',
            'contract_json',
            'modified_by_user_id',
            'modified_by_api',
            'created_by_user_id',
            'created_by_api',
        )
        extra_kwargs = {
            'modified_by_user_id':  { 'required': False },
            'modified_by_api':      { 'required': False },
            'created_by_user_id':   { 'required': False },
            'created_by_api':       { 'required': False },
        }

    def validate_contract_json(self, value):
        try:
            json_validate(value, self.context['view'].json_schema)
        except JsonValidationError as e:
            raise ValidationError('%s. Json field: %s, schema: %s' % (e.message, e.path[0], e.schema))
        return value
