import logging
from base64 import b64decode

import yaml
from django.conf import settings as django_settings
from django.http import HttpResponseForbidden

from metax_api.exceptions import Http403
from metax_api.utils import executing_test_case, executing_travis

_logger = logging.getLogger(__name__)
d = logging.getLogger(__name__).debug

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

def IdentifyApiCaller(*args, **kwargs):
    if executing_test_case() or executing_travis():
        return _IdentifyApiCallerDummy(*args, **kwargs)
    else:
        return _IdentifyApiCaller(*args, **kwargs)


class _IdentifyApiCaller():

    def __init__(self, get_response):
        self.get_response = get_response
        self.API_USERS = self._get_api_users()

    def __call__(self, request):

        # Code to be executed for each request before
        # the view (and later middleware) are called.

        if self._caller_should_be_identified(request):
            try:
                self._identify_api_caller(request)
            except Http403:
                return HttpResponseForbidden()

        response = self.get_response(request)

        # Code to be executed for each request/response after
        # the view is called.

        return response

    def _api_user_allowed(self, username, apikey):
        user = next(( u for u in self.API_USERS if u['username'] == username), None)
        if not user:
            return False
        if apikey != user['password']:
            return False
        return True

    def _caller_should_be_identified(self, request):
        if request.META.get('HTTP_AUTHORIZATION', None):
            return True
        elif request.method in ('POST', 'PUT', 'PATCH', 'DELETE'):
            return True
        return False

    def _get_api_users(self):
        with open('/home/metax-user/app_config') as app_config:
            app_config_dict = yaml.load(app_config)
        try:
            return app_config_dict['API_USERS']
        except:
            _logger.exception('API_USERS missing from app_config')
            raise

    def _identify_api_caller(self, request):
        """
        Try to identify the user from a HTTP authorization header. Places the user's name
        in request.user.username if the user is valid. Raise 403 forbidden on errors.

        Valid users are listed in app_config.

        Currently only Basic Auth is accepted.
        """
        http_auth_header = request.META.get('HTTP_AUTHORIZATION', None)

        if not http_auth_header:
            _logger.warning('Unauthenticated access attempt from ip: %s. Authorization header missing'
                            % request.META['REMOTE_ADDR'])
            raise Http403

        if isinstance(http_auth_header, bytes):
            http_auth_header = http_auth_header.decode('utf-8')

        auth_method, auth_b64 = http_auth_header.split(' ')

        if auth_method != 'Basic':
            _logger.warning('Invalid HTTP authorization method: %s, from ip: %s' %
                            (auth_method, request.META['REMOTE_ADDR']))
            raise Http403

        try:
            username, apikey = b64decode(auth_b64).decode('utf-8').split(':')
        except:
            _logger.warning('Malformed HTTP Authorization header (Basic) from ip: %s' % request.META['REMOTE_ADDR'])
            raise Http403

        if self._api_user_allowed(username, apikey):
            request.user.username = username
        else:
            _logger.warning('Failed authnz for user: %s, from ip: %s' % (username, request.META['REMOTE_ADDR']))
            raise Http403

        return request


class _IdentifyApiCallerDummy(_IdentifyApiCaller):

    def _get_api_users(self):
        return [django_settings.API_TEST_USER]
