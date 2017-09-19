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

class Http400(APIException):
    # bad request
    status_code = 400

class Http403(APIException):
    # forbidden
    status_code = 403

class Http412(APIException):
    # precondition failed
    status_code = 412

class Http503(APIException):
    # service unavailable
    status_code = 503
