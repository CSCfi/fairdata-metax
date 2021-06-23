import traceback
from uuid import uuid4

from rest_framework.serializers import ModelSerializer

from metax_api.models import ApiError
from metax_api.utils import get_tz_aware_now_without_micros


class ApiErrorSerializerV2(ModelSerializer):
    class Meta:
        model = ApiError

        fields = (
            "id",
            "identifier",
            "error",
            "date_created"
        )

    @staticmethod
    def to_rabbitmq_json(request, response, other={}):
        current_time = str(get_tz_aware_now_without_micros()).replace(" ", "T")

        if request.method in ("POST", "PUT", "PATCH"):
            # cast possible datetime objects to strings, because those cant be json-serialized...
            request_data = request.data
            for date_field in ("date_modified", "date_created"):
                if isinstance(request_data, list):
                    for item in request_data:
                        if isinstance(item, dict) and date_field in item:
                            item[date_field] = str(item[date_field])
                elif isinstance(request_data, dict) and date_field in request_data:
                    request_data[date_field] = str(request_data[date_field])
                else:
                    pass
        else:
            request_data = None

        error_info = {
            "method": request.method,
            "user": request.user.username or "guest",
            "data": request_data,
            "headers": {
                k: v
                for k, v in request.META.items()
                if k.startswith("HTTP_") and k != "HTTP_AUTHORIZATION"
            },
            "status_code": response.status_code,
            "response": response.data,
            "traceback": traceback.format_exc(),
            # during test case execution, RAW_URI is not set
            "url": request.META.get("RAW_URI", request.META.get("PATH_INFO", "???")),
            "identifier": "%s-%s" % (current_time[:19], str(uuid4())[:8]),
            "exception_time": current_time,
        }

        if other:
            # may contain info that the request was a bulk operation
            error_info["other"] = {k: v for k, v in other.items()}
            if "bulk_request" in other:
                error_info["other"]["data_row_count"] = len(request_data)

        return error_info
