from base64 import b64decode
from datetime import datetime
from json import loads as json_loads
import logging
import requests
from requests.adapters import HTTPAdapter, Retry

from pprint import pprint

from django.conf import settings as django_settings
from django.utils.functional import SimpleLazyObject
from metax_api.utils import get_tz_aware_now_without_micros

_logger = logging.getLogger("metax_api")


class MetricsTracking:
    def __init__(self, get_response):
        self.get_response = get_response
        # One-time configuration and initialization.

    def __call__(self, request):
        # Code to be executed for each request before
        # the view (and later middleware) are called.

        response = self.get_response(request)

        # Code to be executed for each request/response after
        # the view is called.
        self.publish_event(request)

        return response

    def format_query_params(self, query_dict):
        if len(query_dict) == 0:
            return ""
        query_param_string = " / PARAMETERS"
        param_list = list(query_dict.keys())
        param_list = sorted(param_list, key=str.lower)
        for param in param_list:
            query_param_string += ":" + param.lower()
        return query_param_string

    def get_event_name(self, endpoint_path):
        event_name = ""
        for part in endpoint_path:
            if part == "":
                continue
            if self.string_is_identifier(part):
                event_name += " / ID"
            else:
                event_name += " / " + part.upper()
        return event_name

    def string_is_identifier(self, string):
        return any(char.isdigit() for char in string)

    def construct_event_title(self, request_method, request_path, request_params, username):
        path_parts = request_path.split("/")

        if path_parts[2].upper() in ["V1", "V2"]:
            api_version = path_parts[2].upper()
            endpoint_path = path_parts[3:]
        # If api version is not part of request path, V1 is used
        else:
            api_version = "V1"
            endpoint_path = path_parts[2:]

        event_title = api_version
        event_title += " / " + request_method.upper()

        api_type = path_parts[1]
        if api_type.upper() == "RPC":
            event_title += " / RPC"

        event_title += self.get_event_name(endpoint_path)
        event_title += self.format_query_params(request_params)
        event_title += " / " + username
        return event_title

    def get_event_environment(self):
        domain_name = django_settings.SERVER_DOMAIN_NAME
        if domain_name == "metax.fd-dev.csc.fi":
            return "LOCAL"
        if domain_name == "metax.fairdata.fi":
            return "PRODUCTION"
        if domain_name == "metax.fd-staging.csc.fi":
            return "STAGING"
        if domain_name == "metax.fd-test.csc.fi":
            return "TEST"
        if domain_name == "metax.fd-stable.csc.fi":
            return "STABLE"
        if domain_name == "metax.demo.fairdata.fi":
            return "DEMO"

        return None

    def get_username(self, request):
        if "HTTP_AUTHORIZATION" not in request.META:
            return "ANONYMOUS"
        if isinstance(request.user, SimpleLazyObject):
            return "FAILED_AUTH"
        if request.user.is_service == False and request.user.username != "":
            return "USER"
        if request.user.is_service and request.user.username != "":
            return request.user.username.upper()
        return "UNKNOWN"

    def publish_event(self, request):
        _logger.info(f"Publishing event: {request.method} {request.path}")
        try:
            environment = self.get_event_environment()
            if environment in ["LOCAL", None]:
                _logger.info(f"environment is {environment}. Not publishing the event to Metrics")
                return

            username = self.get_username(request)
            if username in ["IDA", "ETSIN", "QVAIN-LIGHT", "FAILED_AUTH"]:
                _logger.info(f"username is {username}. Not publishing the event to Metrics")
                return

            api = django_settings.METRICS_API_ADDRESS
            if api == None:
                _logger.info(
                    "METRICS_API_ADDRESS env variable not defined. Not publishing the event to Metrics"
                )
                return

            token = django_settings.METRICS_API_TOKEN
            if token == None:
                _logger.info(
                    "METRICS_API_TOKEN env variable not defined. Not publishing the event to Metrics"
                )
                return

            request_params = request.GET

            title = self.construct_event_title(
                request.method, request.path, request_params, username
            )
            service = "METAX"

            query_params = {
                "environment": environment,
                "scope": title,
                "service": service,
                "token": token,
            }

            # Check first that connection to Metrics API is working
            try:
                requests.head(api, timeout=1)
            except TimeoutError as e:
                _logger.error(f"Connection to {api} not working. Metrics will not be updated.")
                _logger.error(e)
                return

            # Send the event to Metrics API
            response = requests.post(f"{api}/report", params=query_params)
            if response.status_code != 200:
                _logger.error(f"{api} returned status_code: {response.status_code}")
                _logger.error(response.text)

        except Exception as error:
            _logger.error("Failed to publish event")
            _logger.error(error)
