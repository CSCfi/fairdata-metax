# This file is part of the Metax API service
#
# Copyright 2017-2018 Ministry of Education and Culture, Finland
#
# :author: CSC - IT Center for Science Ltd., Espoo Finland <servicedesk@csc.fi>
# :license: MIT

import logging

from django.utils import timezone
from pytz import timezone as tz

from metax_api.utils import parse_timestamp_string_to_tz_aware_datetime

_logger = logging.getLogger(__name__)
d = logging.getLogger(__name__).debug


class AddLastModifiedHeaderToResponse(object):
    """
    The purpose of this middleware is to add a Modified-Since header to the response for all GET, POST, PUT and PATCH
    requests. This is done only when the response contains data and that data object directly contains a timestamp
    indicating when the resource has been modified or created. Primarily, if the modification timestamp is found, then
    it will be used as the Modified-Since header value. Secondarily, if the created timestamp is found, then it will
    be used as the Modified-Since header value. In case neither is found, Modified-Since header is not set.
    """

    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):

        # Code to be executed for each request before
        # the view (and later middleware) are called.

        # Then comes the response
        response = self.get_response(request)

        # Code to be executed for each request/response after
        # the view is called.

        if request.method in ["GET", "POST", "PUT", "PATCH"]:
            self._add_last_modified_header_to_response(response)

        return response

    @staticmethod
    def _add_last_modified_header_to_response(response):
        if hasattr(response, "data"):
            obj = None
            if isinstance(response.data, dict):
                obj = response.data.get("success", response.data)
            if isinstance(obj, list) and len(obj) > 0:
                obj = obj[0].get("object", None)

            modified = None
            if obj:
                if "date_modified" in obj:
                    modified = obj.get("date_modified")
                elif "date_created" in obj:
                    modified = obj.get("date_created")

            if modified:
                modified_dt = parse_timestamp_string_to_tz_aware_datetime(modified)
                if modified_dt:
                    date_modified_in_gmt = timezone.localtime(
                        modified_dt, timezone=tz("GMT")
                    )
                    response["Last-Modified"] = date_modified_in_gmt.strftime(
                        "%a, %d %b %Y %H:%M:%S GMT"
                    )
