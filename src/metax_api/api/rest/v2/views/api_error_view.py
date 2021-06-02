from rest_framework.viewsets import ModelViewSet

class ApiErrorViewSet(ModelViewSet):

    def __init__(self, *args, **kwargs):

        super(ApiErrorViewSet, self).__init__(*args, **kwargs)