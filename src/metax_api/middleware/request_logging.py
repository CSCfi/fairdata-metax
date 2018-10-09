# This file is part of the Metax API service
#
# Copyright 2017-2018 Ministry of Education and Culture, Finland
#
# :author: CSC - IT Center for Science Ltd., Espoo Finland <servicedesk@csc.fi>
# :license: MIT

from base64 import b64decode
from json import loads as json_loads
import logging


_logger = logging.getLogger('metax_api')


"""
Log basic information about a request before and after the request is executed.
"""


class RequestLogging():

    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        username = self.get_username(request)
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

        return response

    def get_username(self, request):
        """
        Add more auth methods as necessary...
        """
        auth_header = request.environ.get('HTTP_AUTHORIZATION', None)
        if not auth_header:
            return ''
        if 'Basic' in auth_header:
            try:
                return b64decode(auth_header.split(' ')[1]).decode('utf-8').split(':')[0]
            except:
                _logger.exception('Could not extract username from http auth header')
        elif 'Bearer' in auth_header or 'bearer' in auth_header:
            try:
                return json_loads(b64decode(auth_header.split(' ')[1].split('.')[1] + '===').decode('utf-8'))['sub']
            except:
                # dont log as an error or crash, since we dont want to get bothered by
                # errors about malformed tokens. auth middleware is going to reject this
                # token later too.
                _logger.info('Faulty token: Could not extract username from bearer token')
        else:
            _logger.info('HTTP Auth method not basic or bearer - unable to get username')
        return ''
