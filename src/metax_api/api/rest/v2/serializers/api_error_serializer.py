from rest_framework.serializers import ModelSerializer

class ApiErrorSerializer(ModelSerializer):

    def __init__(self, *args, **kwargs):

        super(ApiErrorSerializer, self).__init__(*args, **kwargs)