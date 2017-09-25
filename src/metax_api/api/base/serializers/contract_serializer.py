from metax_api.models import Contract
from .common_serializer import CommonSerializer
from .serializer_utils import validate_json

class ContractSerializer(CommonSerializer):

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
        validate_json(value, self.context['view'].json_schema)
        return value
