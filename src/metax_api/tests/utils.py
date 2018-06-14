from base64 import b64encode
from json import load as json_load
from os import path

from django.conf import settings as django_settings

datetime_format = '%Y-%m-%dT%H:%M:%S.%fZ'

# path to data used by automatic tests
test_data_file_path = 'metax_api/tests/testdata/test_data.json'


def get_json_schema(model_name):
    with open(path.dirname(path.realpath(__file__)) + '/../api/rest/base/schemas/%s_schema.json' % model_name) as f:
        return json_load(f)


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


class TestClassUtils():
    """
    Test classes may (multi-)inherit this class in addition to APITestCase to use these helpers
    """

    def _use_http_authorization(self, username='testuser', password=None, header_value=None):
        """
        Include a HTTP Authorization header in api requests. By default, the test
        user specified in settings.py will be used. Different user credentials can be
        passed in the parameters.

        Parameter 'header_value' can be used to directly insert the header value, to for example
        test for malformed values, or other auth methods than BA.
        """
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

        if not header_value:
            header_value = b'Basic %s' % b64encode(bytes('%s:%s' % (username, password), 'utf-8'))

        self.client.credentials(HTTP_AUTHORIZATION=header_value)

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
