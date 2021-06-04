from django.db.models import JSONField
from django.db import models

class ApiError(models.Model):

    id = models.BigAutoField(primary_key=True, editable=False)
    identifier = models.CharField(max_length=200, unique=True, null=False)
    error = JSONField(null=False)
    date_created = models.DateTimeField()

    # def save(self, *args, **kwargs):
    #     error_info = {
    #         "method": request.method,
    #         "user": request.user.username or "guest",
    #         "data": request_data,
    #         "headers": {
    #             k: v
    #             for k, v in request.META.items()
    #             if k.startswith("HTTP_") and k != "HTTP_AUTHORIZATION"
    #         },
    #         "status_code": response.status_code,
    #         "response": response.data,
    #         "traceback": traceback.format_exc(),
    #         # during test case execution, RAW_URI is not set
    #         "url": request.META.get("RAW_URI", request.META.get("PATH_INFO", "???")),
    #         "identifier": "%s-%s" % (current_time[:19], str(uuid4())[:8]),
    #         "exception_time": current_time,
    #     }