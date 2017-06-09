from jsonschema import validate as json_validate

from django.core.management import call_command
from django.test import TestCase

from metax_api.tests.utils import get_json_schema, test_data_file_path, TestClassUtils
from metax_api.models import Dataset

d = print

class DatasetModelBasicTest(TestCase, TestClassUtils):

    """
    Verify that at least test data is correct on a basic level, and the model contains
    the expected fields.
    """

    dataset_field_names = (
        'identifier',
        'dataset_json',
        'dataset_catalog_id',
    )

    common_fields_names = (
        'id',
        'active',
        'removed',
        'modified_by_api',
        'modified_by_user_id',
        'created_by_api',
        'created_by_user_id',
    )

    @classmethod
    def setUpClass(cls):
        """
        Loaded only once for test cases inside this class.
        """
        call_command('loaddata', test_data_file_path, verbosity=0)
        super(DatasetModelBasicTest, cls).setUpClass()

    def setUp(self):
        dataset_from_test_data = self._get_object_from_test_data('dataset')
        self.identifier = dataset_from_test_data['identifier']

    def test_get_by_identifier(self):
        dataset = Dataset.objects.get(identifier=self.identifier)
        self.assertEqual(dataset.identifier, self.identifier)

    def test_validate_dataset_json(self):
        dataset = Dataset.objects.get(identifier=self.identifier)

        try:
            json_validate(dataset.dataset_json, get_json_schema('dataset'))
            json_validation_success = True
        except Exception as e:
            d(e)
            json_validation_success = False

        self.assertEqual(json_validation_success, True, 'dataset.dataset_json failed json schema validation')

    def test_model_fields_as_expected(self):
        actual_model_fields = [ f.name for f in Dataset._meta.get_fields() ]
        self._test_model_fields_as_expected(self.dataset_field_names + self.common_fields_names, actual_model_fields)
