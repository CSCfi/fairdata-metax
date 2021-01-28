# This file is part of the Metax API service
#
# Copyright 2017-2018 Ministry of Education and Culture, Finland
#
# :author: CSC - IT Center for Science Ltd., Espoo Finland <servicedesk@csc.fi>
# :license: MIT

from rest_framework.exceptions import APIException

"""
class APIException():

    def __init__(self, detail=None, code=None):
        ...

        detail can be a dict, then the client would receive error message such as:
        {
            'state': 'state is a required field'
        }

        Giving only a string as detail, will show to the client as:
        {
            'detail': 'Some description of error'
        }
"""

class MetaxAPIException(APIException):

    def __init__(self, detail=None, code=None):
        """
        Override so that exceptions can be used in a more simple manner such that the message
        always ends up in an array.
        """
        if type(detail) is str:
            detail = { 'detail': [detail] }
        super().__init__(detail=detail, code=code)

class Http400(MetaxAPIException):
    # bad request
    status_code = 400

class Http401(MetaxAPIException):
    # unauthorized
    # note: request is missing authentication information, or it was wrong
    status_code = 401

class Http403(MetaxAPIException):
    # forbidden
    # note: request user is correctly authenticated, but has no permission
    status_code = 403

class Http412(MetaxAPIException):
    # precondition failed
    status_code = 412

class Http500(MetaxAPIException):
    # internal server error
    status_code = 500

class Http501(MetaxAPIException):
    # not implemented
    status_code = 501

class Http503(MetaxAPIException):
    # service unavailable
    status_code = 503
