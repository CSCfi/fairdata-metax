from metax_api.permissions import ServicePermissions
from metax_api.api.rest.base.views import ApiErrorViewSet

class ApiErrorViewSet(ApiErrorViewSet):

    api_type = "rest"
    authentication_classes = ()
    permission_classes = [ServicePermissions]

    def __init__(self, *args, **kwargs):

        super(ApiErrorViewSet, self).__init__(*args, **kwargs)
        
    # def list(self, request, *args, **kwargs):
    #     # hae modelista
    #     error_list = ApiErrorService.retrieve_error_list()
    #     return Response(data=error_list, status=200)