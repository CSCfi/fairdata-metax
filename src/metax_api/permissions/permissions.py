# This file is part of the Metax API service
#
# Copyright 2017-2018 Ministry of Education and Culture, Finland
#
# :author: CSC - IT Center for Science Ltd., Espoo Finland <servicedesk@csc.fi>
# :license: MIT

import logging

from django.conf import settings
from rest_framework.exceptions import MethodNotAllowed
from rest_framework.permissions import BasePermission


_logger = logging.getLogger(__name__)
READ_METHODS = ('GET', 'HEAD', 'OPTIONS')
WRITE_METHODS = ('POST', 'PUT', 'PATCH', 'DELETE')


class MetaxAPIPermissions(BasePermission):

    """
    Base permission-object to control api-resource-wide read and write access for each
    service and end users, such as /datasets or /files. If access to individual view
    method (url) needs to be restricted, add additional checks directly inside that
    view method.

    The source of api-wide permissions is the app_config file.
    """

    # used for filtering when deciding which permissions should be executed
    # for a particular request
    service_permission = False

    # contains api permissions from app_config
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
        Check if the request user has permission to access the particular api,
        and whether they have read or write access to it.

        note: when relevant, permissions per object are checked in has_object_permission().
        """
        api_type = view.api_type
        api_name = view.get_api_name()
        has_perm = False

        _logger.debug('Checking user permission for api %s...' % request.path)

        if api_type not in self.perms: # pragma: no cover
            _logger.error(
                'api_type %s not specified in self.perms - forbidding. this probably should not happen'
                % api_type
            )
            has_perm = False
        elif api_name not in self.perms[api_type]: # pragma: no cover
            _logger.error(
                'api_name %s not specified in self.perms[\'%s\'] - forbidding. this probably should not happen' %
                (api_name, api_type)
            )
            has_perm = False
        elif api_type == 'rest':
            if request.method in READ_METHODS:
                if 'all' in self.perms[api_type][api_name]['read']:
                    has_perm = True
                else:
                    has_perm = self._check_read_perms(request, api_type, api_name)
            elif request.method in WRITE_METHODS:
                if 'all' in self.perms[api_type][api_name]['write']:
                    has_perm = True
                else:
                    has_perm = self._check_write_perms(request, api_type, api_name)
            else:
                raise MethodNotAllowed
        elif api_type == 'rpc':
            rpc_method_name = request.path.split('/')[-1]
            if 'all' in self.perms[api_type][api_name][rpc_method_name]['use']:
                has_perm = True
            else:
                has_perm = self._check_use_perms(request, api_type, api_name, rpc_method_name)
        else:
            _logger.info('Unknown api %s' % request.path)
            raise NotImplementedError

        _logger.debug(
            'user %s has_perm for api %s == %r'
            % (request.user.username or '(anonymous)', request.path, has_perm)
        )

        return has_perm

    def has_object_permission(self, request, view, obj):
        raise NotImplementedError


class EndUserPermissions(MetaxAPIPermissions):

    """
    End User permissions checks per api, and per object.
    """

    service_permission = False
    message = 'End Users are not allowed to access this api.'

    def _check_read_perms(self, request, api_type, api_name):
        return 'endusers' in self.perms[api_type][api_name]['read']

    def _check_write_perms(self, request, api_type, api_name):
        return 'endusers' in self.perms[api_type][api_name]['write']

    def _check_use_perms(self, request, api_type, api_name, rpc_method_name):
        return 'endusers' in self.perms[api_type][api_name][rpc_method_name]['use']

    def has_object_permission(self, request, view, obj):
        """
        For end users, check permission according to object and operation.
        """
        has_perm = obj.user_has_access(request)
        if not has_perm:
            self.message = 'You are not permitted to access this resource.'
        return has_perm


class ServicePermissions(MetaxAPIPermissions):

    """
    Service permissions per api. If access is granted to api, service has access
    to all its objects too.
    """

    service_permission = True
    message = 'Service %s is not allowed to access this api.'

    def has_permission(self, request, view):
        has_perm = super().has_permission(request, view)
        if not has_perm:
            self.message = self.message % request.user.username
        return has_perm

    def _check_read_perms(self, request, api_type, api_name):
        return request.user.username in self.perms[api_type][api_name]['read']

    def _check_write_perms(self, request, api_type, api_name):
        return request.user.username in self.perms[api_type][api_name]['write']

    def _check_use_perms(self, request, api_type, api_name, rpc_method_name):
        return request.user.username in self.perms[api_type][api_name][rpc_method_name]['use']

    def has_object_permission(self, request, view, obj):
        """
        For service users, always returns True, since it is assumed they know what they are doing.
        """
        return True
