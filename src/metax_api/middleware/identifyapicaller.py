# This file is part of the Metax API service
#
# Copyright 2017-2018 Ministry of Education and Culture, Finland
#
# :author: CSC - IT Center for Science Ltd., Espoo Finland <servicedesk@csc.fi>
# :license: MIT

import json
import logging
from base64 import b64decode

import requests
from django.conf import settings as django_settings
from django.http import HttpResponseForbidden

from metax_api.exceptions import Http403
from metax_api.utils import executing_test_case

_logger = logging.getLogger(__name__)


"""
Currently API access authnz is already enforced once on the web server proxy level.
For fine-grained authorization per action, or user-specific special logic, this
middleware stores the API user information to request.user.username, which will
be accessible in views.

A middleware is initialized only once during the lifetime of a process, and users
are loaded during middleware initialization. Thus when new api users are added to
app_config, the processes need to be restarted. This should be a rare enough occasion
to not be a cataclysmic event for the project.

The object request.user is Django's default AnonymousUser-object, since we are not using
ordinary Django authz methods, nor are we storing any sessions. Meaning user authnz
is performed on each request. This, however, is very fast, since there are no db calls to
fetch users or sessions: Everything required for user authnz is living inside the middleware object
for the lifetime of the process.
"""


WRITE_METHODS = ("POST", "PUT", "PATCH", "DELETE")


if not django_settings.TLS_VERIFY:
    requests.packages.urllib3.disable_warnings()


class _IdentifyApiCaller:
    def __init__(self, get_response):
        self.get_response = get_response
        self.API_USERS = self._get_api_users()
        self.ALLOWED_AUTH_METHODS = django_settings.ALLOWED_AUTH_METHODS

    def __call__(self, request):

        # Code to be executed for each request before
        # the view (and later middleware) are called.

        request.user.is_service = False

        if self._caller_should_be_identified(request):
            try:
                self._identify_api_caller(request)
            except Http403 as e:
                return HttpResponseForbidden(e)

        response = self.get_response(request)

        # Code to be executed for each request/response after
        # the view is called.

        return response

    def _get_api_users(self):
        """
        Services, or other pre-defined api users.
        """

        try:
            return django_settings.API_USERS
        except:
            _logger.exception("API_USERS missing from app_config")
            raise

    def _caller_should_be_identified(self, request):
        if request.META.get("HTTP_AUTHORIZATION", None):
            return True
        elif request.method in WRITE_METHODS:
            return True
        return False

    def _identify_api_caller(self, request):
        """
        Try to identify the user from a HTTP authorization header. Places information about
        the user into request.user, such as request.user.username, is_service, and possibly
        other info. Raises 403 forbidden on errors.

        Service users are authenticated against pre-defined settings, while End Users will have
        their auth token validated.

        Valid service users and authentication methods are listed in app_config.
        """

        http_auth_header = request.META.get("HTTP_AUTHORIZATION", None)

        if not http_auth_header:
            _logger.warning(
                "Unauthenticated access attempt from ip: %s. Authorization header missing"
                % request.META["REMOTE_ADDR"]
            )
            raise Http403({"detail": ["Access denied."]})

        try:
            auth_method, auth_b64 = http_auth_header.split(" ")
        except ValueError:
            raise Http403(
                {
                    "detail": [
                        "Invalid HTTP authorization method. Ensure you included on of the following "
                        "methods inside the auth header: %s" % ", ".join(self.ALLOWED_AUTH_METHODS)
                    ]
                }
            )

        if auth_method not in self.ALLOWED_AUTH_METHODS:
            _logger.warning("Invalid HTTP authorization method: %s" % auth_method)
            raise Http403(
                {
                    "detail": [
                        "Invalid HTTP authorization method: %s. Allowed auth methods: %s"
                        % (auth_method, ", ".join(self.ALLOWED_AUTH_METHODS))
                    ]
                }
            )

        if auth_method.lower() == "basic":
            self._auth_basic(request, auth_b64)
        elif auth_method.lower() == "bearer":
            self._auth_bearer(request, auth_b64)
        else:
            raise Exception("The allowed auth method %s is missing handling" % auth_method)

        return request

    def _auth_basic(self, request, auth_b64):
        """
        Check request user credentials for service-level users.
        """
        try:
            username, apikey = b64decode(auth_b64).decode("utf-8").split(":")
        except:
            _logger.warning("Malformed HTTP Authorization header (Basic)")
            raise Http403({"detail": ["Malformed HTTP Authorization header (Basic)"]})

        user = next((u for u in self.API_USERS if u["username"] == username), None)

        if not user:
            _logger.warning("Failed authnz for user %s: user not found" % username)
            raise Http403({"detail": ["Access denied."]})
        if apikey != user["password"]:
            _logger.warning("Failed authnz for user %s: password mismatch" % username)
            raise Http403({"detail": ["Access denied."]})

        request.user.username = username
        request.user.is_service = True

    def _auth_bearer(self, request, auth_b64):
        _logger.debug("validating bearer token...")

        response = requests.get(
            # url protected by oidc. the proxy is configured to return 200 OK for any valid token
            django_settings.VALIDATE_TOKEN_URL,
            headers={"Authorization": request.META.get("HTTP_AUTHORIZATION", None)},
            verify=False,
        )

        _logger.debug("response from token validation: %s" % str(response))

        if response.status_code != 200:
            _logger.warning("Bearer token validation failed")
            raise Http403({"detail": ["Access denied."]})

        try:
            token = self._extract_id_token(auth_b64)
        except:
            # the above method should never fail, as this code should not be
            # reachable if the token validation had already failed.
            _logger.exception("Failed to extract token from id_token string")
            raise Http403({"detail": ["Access denied."]})

        if len(token.get("CSCUserName", "")) > 0:
            request.user.username = token["CSCUserName"]
        else:
            _logger.warning("id_token does not contain valid user id: fairdataid or cscusername")
            raise Http403({"detail": ["Access denied."]})

        request.user.is_service = False
        request.user.token = token

    def _extract_id_token(self, id_token_string):
        """
        Extract the interesting part from the dot-separated string that looks something like
        "asdasd.abcabc.defghi".
        """
        id_token_payload_b64 = id_token_string.split(".")[1]

        # in the dot-separated string, the b64 encoded strings may not be multiples of 4, which
        # complete valid b64 strings should be. add a few trailing '=' characters to ensure
        # requirement is satisfied. the b64decode method knows to discard excess '='.
        return json.loads(b64decode("%s===" % id_token_payload_b64).decode("utf-8"))


class _IdentifyApiCallerDummy(_IdentifyApiCaller):
    def _get_api_users(self):
        return django_settings.API_TEST_USERS


if executing_test_case():
    IdentifyApiCaller = _IdentifyApiCallerDummy
else:
    IdentifyApiCaller = _IdentifyApiCaller
