import logging

from django.conf import settings
from rest_framework.exceptions import MethodNotAllowed
from rest_framework.permissions import BasePermission


_logger = logging.getLogger(__name__)
READ_METHODS = ('GET', 'HEAD', 'OPTIONS')
WRITE_METHODS = ('POST', 'PUT', 'PATCH', 'DELETE')


class ServicePermissions(BasePermission):

    """
    Permission-object to control api-resource-wide read and write access for each
    service, such as /datasets or /files. If access to individual view method (url)
    needs to be restricted, add additional checks directly inside that view method.

    The source of service permissions is the app_config file.
    """

    perms = {}

    def __init__(self, *args, **kwargs):
        self.set_perms()

    def set_perms(self):
        """
        self.perms dict looks like:
        {
            'datasets': {
                'read': ['service1', 'service2'],
                'write': ['service1', 'service3']
            }
            'files': {
                ...
            },
            ...
        }
        """
        self.perms = settings.API_ACCESS

    def has_permission(self, request, view):
        """
        Return `True` if permission is granted, `False` otherwise.
        """
        api_name = view.get_api_name()

        if api_name not in self.perms: # pragma: no cover
            _logger.info(
                'api_name %s not specified in self.perms - forbidding. this probably should not happen' % api_name
            )
            return False

        elif request.method in READ_METHODS:
            if 'all' in self.perms[api_name]['read']:
                return True
            return request.user.username in self.perms[api_name]['read']

        elif request.method in WRITE_METHODS:
            if 'all' in self.perms[api_name]['write']:
                return True
            return request.user.username in self.perms[api_name]['write']

        else:
            raise MethodNotAllowed

        return False

    def has_object_permission(self, request, view, obj):
        """
        Return `True` if permission is granted, `False` otherwise.
        """
        # default
        return True
