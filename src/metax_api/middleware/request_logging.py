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
        _logger.info(f"request: {request.method} usertype: {user_type}, username: {username} pid: {self._pid}")


        response = self.get_response(request)

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
                user = json_loads(
                    b64decode(auth_header.split(' ')[1].split('.')[1] + '===')
                    .decode('utf-8')
                )['CSCUserName']
            except:
                # dont log as an error or crash, since we dont want to get bothered by
                # errors about malformed tokens. auth middleware is going to reject this
                # token later too.
                _logger.info('Faulty token: Could not extract username from bearer token')
        else:
            _logger.info('HTTP Auth method not basic or bearer - unable to get username')

        return user, user_type
