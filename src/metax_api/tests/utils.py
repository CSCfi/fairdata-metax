from os import path
from json import load as json_load

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

    def _test_model_fields_as_expected(self, expected_fields, actual_fields):

        for field in expected_fields:
            if field not in actual_fields:
                raise Exception('Model is missing an expected field: %s. Did you add new fields to this model recently?' % field)
            actual_fields.remove(field)

        self.assertEqual(len(actual_fields), 0, 'Model contains unexpected fields: %s' % str(actual_fields))

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
