# This file is part of the Metax API service
#
# Copyright 2017-2018 Ministry of Education and Culture, Finland
#
# :author: CSC - IT Center for Science Ltd., Espoo Finland <servicedesk@csc.fi>
# :license: MIT

from rest_framework.serializers import ValidationError

from metax_api.models import Contract

from .common_serializer import CommonSerializer
from .serializer_utils import validate_json


class ContractSerializer(CommonSerializer):

    class Meta:
        model = Contract
        fields = (
            'id',
            'contract_json',
        ) + CommonSerializer.Meta.fields

        extra_kwargs = CommonSerializer.Meta.extra_kwargs

    def validate_contract_json(self, value):
        validate_json(value, self.context['view'].json_schema)
        if self._operation_is_create:
            self._validate_identifier_uniqueness(value)
        return value

    def _validate_identifier_uniqueness(self, contract_json):
        if Contract.objects.filter(contract_json__identifier=contract_json['identifier']).exists():
            raise ValidationError(f"identifier {contract_json['identifier']} already exists")
