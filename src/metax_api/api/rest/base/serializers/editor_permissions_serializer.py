from django.core.validators import EMPTY_VALUES

from rest_framework.serializers import ValidationError, ModelSerializer

from metax_api.models import EditorUserPermission

from .common_serializer import CommonSerializer
from .serializer_utils import validate_json


class EditorPermissionsSerializer(ModelSerializer):
    class Meta:
        model = EditorUserPermission
        fields = '__all__'

        extra_kwargs = CommonSerializer.Meta.extra_kwargs

    def validate(self, attrs):
        data = ModelSerializer.validate(self, attrs)

        if data.get('verified') and data.get('verification_token') in EMPTY_VALUES:
            raise ValidationError({'verification_token': 'Verification token missing'})

        return data
