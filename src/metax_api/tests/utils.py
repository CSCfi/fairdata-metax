from base64 import b64encode
from os import path
from json import load as json_load
from django.conf import settings as django_settings

datetime_format = '%Y-%m-%dT%H:%M:%S.%fZ'

# path to data used by automatic tests
test_data_file_path = 'metax_api/tests/testdata/test_data.json'

def get_json_schema(model_name):
    with open(path.dirname(path.realpath(__file__)) + '/../api/base/schemas/json_schema_%s.json' % model_name) as f:
        return json_load(f)

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

        raise Exception('Could not find model %s from test data with index == %d.'
            ' Are you certain you generated rows for model %s in generate_test_data.py?'
            % (model_name, requested_index))
