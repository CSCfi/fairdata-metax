from django.core.validators import EMPTY_VALUES

from rest_framework.serializers import (
    ValidationError,
    ModelSerializer,
    PrimaryKeyRelatedField,
    UUIDField,
)

from metax_api.models import EditorPermissions, EditorUserPermission

from .common_serializer import CommonSerializer


class EditorPermissionsUserSerializer(ModelSerializer):
    class Meta:
        model = EditorUserPermission
        fields = "__all__"

        extra_kwargs = {
            **CommonSerializer.Meta.extra_kwargs,
            "editor_permissions": {"pk_field": UUIDField()},
        }

    def validate(self, attrs):
        data = ModelSerializer.validate(self, attrs)

        return data


class EditorPermissionsSerializer(ModelSerializer):
    users = EditorPermissionsUserSerializer(many=True)

    class Meta:
        model = EditorPermissions
        fields = "__all__"


class EditorPermissionsWithAllUsersSerializer(ModelSerializer):
    users = EditorPermissionsUserSerializer(many=True, source="all_users")

    class Meta:
        model = EditorPermissions
        fields = "__all__"
