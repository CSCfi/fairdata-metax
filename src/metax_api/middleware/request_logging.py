# This file is part of the Metax API service
#
# Copyright 2017-2018 Ministry of Education and Culture, Finland
#
# :author: CSC - IT Center for Science Ltd., Espoo Finland <servicedesk@csc.fi>
# :license: MIT

from base64 import b64decode
import logging


_logger = logging.getLogger('metax_api')


"""
Log basic information about a request before and after the request is executed.
"""


class RequestLogging():

    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        try:
            _logger.info(
                '%s - %s - "%s %s %s" %s'
                % (
                    request.environ['HTTP_X_REAL_IP'],
                    self.get_username(request),
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
                    self.get_username(request),
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
        return ''
