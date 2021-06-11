from rest_framework.serializers import ModelSerializer

from metax_api.models import ApiError

class ApiErrorSerializerV2(ModelSerializer):
    class Meta:
        model = ApiError

        fields = (
            "id",
            "identifier",
            "error",
            "date_created"
        )