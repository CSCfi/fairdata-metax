from django.core.validators import EMPTY_VALUES

from rest_framework.serializers import ValidationError, ModelSerializer

from metax_api.models import EditorUserPermission

from .common_serializer import CommonSerializer


class EditorPermissionsSerializer(ModelSerializer):
    class Meta:
        model = EditorUserPermission
        fields = "__all__"

        extra_kwargs = CommonSerializer.Meta.extra_kwargs

    def validate(self, attrs):
        data = ModelSerializer.validate(self, attrs)

        return data
