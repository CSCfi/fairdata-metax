# This file is part of the Metax API service
#
# Copyright 2017-2018 Ministry of Education and Culture, Finland
#
# :author: CSC - IT Center for Science Ltd., Espoo Finland <servicedesk@csc.fi>
# :license: MIT

import logging
import traceback
from uuid import uuid4

from django.db.models import JSONField
from django.db import models

from metax_api.utils import executing_test_case, get_tz_aware_now_without_micros, json_logger

# from rest_framework.parsers import JSONParser

_logger = logging.getLogger(__name__)

class ApiErrorManager(models.Manager):
    def store_error_details(self, request, response, exception=None, other={}):
        """
        Store error and request details to database.
        """
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

        try:
            error = self.create(identifier=error_info["identifier"], error=error_info, date_created=current_time)
            error.save()
            return error
        except:
            _logger.exception("Failed to save error info...")
        else:
            response.data["error_identifier"] = error_info["identifier"]

            if response.status_code == 500:

                json_logger.error(
                    event="api_exception",
                    error={
                        "error_identifier": error_info["identifier"],
                        "status_code": response.status_code,
                        "traceback": error_info["traceback"],
                    },
                )

                if executing_test_case():
                    response.data["traceback"] = traceback.format_exc()

class ApiError(models.Model):

    id = models.BigAutoField(primary_key=True, editable=False)
    identifier = models.CharField(max_length=200, unique=True, null=False)
    error = JSONField(null=False)
    date_created = models.DateTimeField(default=get_tz_aware_now_without_micros)

    objects = ApiErrorManager()
