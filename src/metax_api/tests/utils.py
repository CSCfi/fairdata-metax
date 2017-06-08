from os import path
from json import load as json_load

datetime_format = '%Y-%m-%dT%H:%M:%S.%fZ'

# path to data used by automatic tests
test_data_file_path = 'metax_api/tests/testdata/test_data.json'

def get_json_schema(model_name):
    with open(path.dirname(path.realpath(__file__)) + '/../api/base/schemas/json_schema_%s.json' % model_name) as f:
        return json_load(f)

class TestModelFieldsMixin():

    """
    Test classes may (multi-)inherit this class in addition to APITestCase to use this method
    """

    def _test_model_fields_as_expected(self, expected_fields, actual_fields):

        for field in expected_fields:
            if field not in actual_fields:
                raise Exception('Model is missing an expected field: %s' % field)
            actual_fields.remove(field)

        self.assertEqual(len(actual_fields), 0, 'Model contains unexpected fields: %s' % str(actual_fields))
