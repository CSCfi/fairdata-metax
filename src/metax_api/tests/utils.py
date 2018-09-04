# This file is part of the Metax API service
#
# Copyright 2017-2018 Ministry of Education and Culture, Finland
#
# :author: CSC - IT Center for Science Ltd., Espoo Finland <servicedesk@csc.fi>
# :license: MIT

from base64 import b64encode
from json import load as json_load
from os import path

from django.conf import settings as django_settings
import jwt
import responses


datetime_format = '%Y-%m-%dT%H:%M:%S.%fZ'

# path to data used by automatic tests
test_data_file_path = 'metax_api/tests/testdata/test_data.json'


def get_json_schema(model_name):
    with open(path.dirname(path.realpath(__file__)) + '/../api/rest/base/schemas/%s_schema.json' % model_name) as f:
        return json_load(f)

def get_test_oidc_token():
    return {
        "sub": "testuser123456@fairdataid",
        "linkedIds": [
            "testuser123456@fairdataid",
            "testuser@csc.fi",
            "testuser@cscuserid"
        ],
        "displayName": "Teppo Testaaja",
        "eppn": "testuser@csc.fi",
        "iss": "https://fd-auth.csc.fi",
        "group_names": [
            "fairdata:TAITO01",
            "fairdata:TAITO01:2002013"
        ],
        "schacHomeOrganizationType": "urn:schac:homeOrganizationType:fi:other",
        "given_name": "Teppo",
        "nonce": "qUu_QUwfHaB92m3ng8PVZ3ycGXlecvMejgATXsuC9OM",
        "aud": "fairdata-metax",
        "uid": "testuser@csc.fi",
        "acr": "password",
        "auth_time": 1535535590,
        "name": "Teppo Testaaja",
        "schacHomeOrganization": "csc.fi",
        "exp": 1535539191,
        "iat": 1535535591,
        "family_name": "Testaaja",
        "email": "teppo.testaaja@csc.fi"
    }

def generate_test_identifier(itype, index, urn=True):
    '''
    Generate urn-type identifier ending with uuid4

    :param index: positive integer for making test identifiers unique. Replaces characters from the end of uuid
    :param itype: to separate identifiers from each other on model level (e.g. is it a catalog record or data catalog)
    :param urn: use urn prefix or not
    :return: test identifier
    '''

    postfix = str(index)
    # Use the same base identifier for the tests and vary by the identifier type and the incoming positive integer
    uuid = str(itype) + '955e904-e3dd-4d7e-99f1-3fed446f96d5'
    if urn:
        return 'urn:nbn:fi:att:%s' % (uuid[:-len(postfix)] + postfix)
    return uuid[:-len(postfix)] + postfix

def generate_test_token(payload):
    '''
    While the real algorithm used in the Fairdata auth component is RS256, HS256 is the only one
    supported in the PyJWT lib, and since we are mocking the responses anyway, it does not matter,
    as long as the token otherwise looks legit, can be parse etc.
    '''
    return jwt.encode(payload, 'secret', 'HS256').decode('utf-8')

class TestClassUtils():
    """
    Test classes may (multi-)inherit this class in addition to APITestCase to use these helpers
    """

    def _use_http_authorization(self, username='testuser', password=None, header_value=None,
            method='basic', token=None):
        """
        Include a HTTP Authorization header in api requests. By default, the test
        user specified in settings.py will be used. Different user credentials can be
        passed in the parameters.

        Parameter 'header_value' can be used to directly insert the header value, to for example
        test for malformed values, or other auth methods than BA.
        """
        if header_value:
            # used as is
            pass
        elif method == 'basic':
            if username == 'testuser':
                user = django_settings.API_TEST_USER
                username = user['username']
                if not password:
                    # password can still be passed as a param, to test wrong password
                    password = user['password']
            elif username == 'metax':
                user = django_settings.API_METAX_USER
                username = user['username']
                password = user['password']
            elif username == 'api_auth_user':
                user = django_settings.API_AUTH_TEST_USER
                username = user['username']
                password = user['password']
            else:
                if not password:
                    raise Exception('Missing parameter \'password\' for HTTP Authorization header')

            header_value = b'Basic %s' % b64encode(bytes('%s:%s' % (username, password), 'utf-8'))

        elif method == 'bearer':
            assert token is not None, 'token (dictionary) is required when using auth method bearer'
            header_value = 'Bearer %s' % generate_test_token(token)

        self.client.credentials(HTTP_AUTHORIZATION=header_value)

    def _mock_token_validation_succeeds(self):
        '''
        Since the End User authnz utilizes OIDC, and there is no legit local OIDC OP,
        responses from /secure/validate_token are mocked. The endpoint only returns
        200 OK for successful token validation, or 403 for failed validation.

        Use this method to simulate requests where token validation succeeds.
        '''
        responses.add(responses.GET, django_settings.VALIDATE_TOKEN_URL, status=200)

    def _mock_token_validation_fails(self):
        '''
        Since the End User authnz utilizes OIDC, and there is no legit local OIDC OP,
        responses from /secure/validate_token are mocked. The endpoint only returns
        200 OK for successful token validation, or 403 for failed validation.

        Use this method to simulate requests where token validation fails.
        '''
        responses.add(responses.GET, django_settings.VALIDATE_TOKEN_URL, status=403)

    def _get_object_from_test_data(self, model_name, requested_index=0):
        with open(test_data_file_path) as test_data_file:
            test_data = json_load(test_data_file)

        model = 'metax_api.%s' % model_name
        i = 0

        for row in test_data:
            if row['model'] == model:
                if i == requested_index:
                    obj = {
                        'id': row['pk'],
                    }
                    obj.update(row['fields'])
                    return obj
                else:
                    i += 1

        raise Exception('Could not find model %s from test data with index == %d. '
                        'Are you certain you generated rows for model %s in generate_test_data.py?'
                        % (model_name, requested_index))
