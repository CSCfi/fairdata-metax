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

        if request.method in ['GET', 'POST', 'PUT', 'PATCH']:
            self._add_last_modified_header_to_response(response)

        return response

    @staticmethod
    def _add_last_modified_header_to_response(response):
        if response.data and isinstance(response.data, dict):
            modified = None
            if 'modified_by_api' in response.data:
                modified = response.data.get('modified_by_api')
            elif 'created_by_api' in response.data:
                modified = response.data.get('created_by_api')

            if modified:
                modified_dt = parse_timestamp_string_to_tz_aware_datetime(modified)
                if modified_dt:
                    modified_by_api_in_gmt = timezone.localtime(modified_dt, timezone=tz('GMT'))
                    response['Last-Modified'] = modified_by_api_in_gmt.strftime('%a, %d %b %Y %H:%M:%S GMT')
