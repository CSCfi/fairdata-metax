from rest_framework.serializers import ModelSerializer

class ApiErrorSerializerV2(ModelSerializer):

    def __init__(self, *args, **kwargs):

        super(ApiErrorSerializerV2, self).__init__(*args, **kwargs)