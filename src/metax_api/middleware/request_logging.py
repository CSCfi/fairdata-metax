# This file is part of the Metax API service
#
# Copyright 2017-2018 Ministry of Education and Culture, Finland
#
# :author: CSC - IT Center for Science Ltd., Espoo Finland <servicedesk@csc.fi>
# :license: MIT

from base64 import b64decode
from json import loads as json_loads
from time import time
import logging
import os

from metax_api.utils import json_logger


_logger = logging.getLogger('metax_api')


SUCCESS_CODES = (200, 201, 204)

"""
Log basic information about a request before and after the request is executed.
"""


class RequestLogging():

    def __init__(self, get_response):
        self.get_response = get_response
        self._pid = os.getpid()

    def __call__(self, request):

        start_time = time()

        username, user_type = self.get_username(request)

        try:
            json_logger.info(
                event='request_start',
                user_id=username,
                request={
                    'query_string': request.environ['QUERY_STRING'],
                    'url_path': request.environ['PATH_INFO'],
                    'ip': request.environ['HTTP_X_REAL_IP'],
                    'user_type': user_type,
                    'http_method': request.environ['REQUEST_METHOD'],
                    'process_id': self._pid,
                }
            )
        except:
            _logger.exception('Exception during trying to json-log request start')

        try:
            _logger.info(
                '%s - %s - "%s %s %s" %s'
                % (
                    request.environ['HTTP_X_REAL_IP'],
                    username,
                    request.environ['REQUEST_METHOD'],
                    request.get_full_path(),
                    request.environ['SERVER_PROTOCOL'],
                    request.environ.get('HTTP_USER_AGENT', '(no useragent)')
                )
            )
        except:
            _logger.exception('Exception during trying to log request start')

        response = self.get_response(request)

        try:
            _logger.info(
                '%s - "%s %s" %d %s'
                % (
                    username,
                    request.method,
                    request.get_full_path(),
                    response.status_code,
                    response._headers.get('content-length', ['-', '-'])[1]
                )
            )
        except:
            _logger.exception('Exception during trying to log request end')

        try:
            request_info = {
                'event': 'request_end',
                'user_id': username,
                'request': {
                    'query_string': request.environ['QUERY_STRING'],
                    'url_path': request.environ['PATH_INFO'],
                    'ip': request.environ['HTTP_X_REAL_IP'],
                    'user_type': user_type,
                    'http_method': request.environ['REQUEST_METHOD'],
                    'http_status': response.status_code,
                    'process_id': self._pid,
                    'request_duration': float('%.3f' % (time() - start_time)),
                }
            }

            if response.status_code not in SUCCESS_CODES and response.data and 'error_identifier' in response.data:
                request_info['error'] = { 'error_identifier': response.data['error_identifier'] }

            json_logger.info(**request_info)
        except:
            _logger.exception('Exception during trying to json-log request end')

        return response

    def get_username(self, request):
        """
        Add more auth methods as necessary...
        """
        auth_header = request.environ.get('HTTP_AUTHORIZATION', None)
        user = ''
        user_type = 'guest'

        if not auth_header:
            return user, user_type

        if 'Basic' in auth_header:
            try:
                user_type = 'service'
                user = b64decode(auth_header.split(' ')[1]).decode('utf-8').split(':')[0]
            except:
                _logger.exception('Could not extract username from http auth header')
        elif 'Bearer' in auth_header or 'bearer' in auth_header:
            try:
                user_type = 'end_user'
                user = json_loads(b64decode(auth_header.split(' ')[1].split('.')[1] + '===').decode('utf-8'))['sub']
            except:
                # dont log as an error or crash, since we dont want to get bothered by
                # errors about malformed tokens. auth middleware is going to reject this
                # token later too.
                _logger.info('Faulty token: Could not extract username from bearer token')
        else:
            _logger.info('HTTP Auth method not basic or bearer - unable to get username')

        return user, user_type
