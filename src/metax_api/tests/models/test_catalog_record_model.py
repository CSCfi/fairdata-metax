# todo validation disabled until schema updated
# from jsonschema import validate as json_validate

from django.core.management import call_command
from django.test import TestCase

# from metax_api.tests.utils import get_json_schema, test_data_file_path, TestClassUtils
from metax_api.tests.utils import test_data_file_path, TestClassUtils
from metax_api.models import CatalogRecord

d = print

class CatalogRecordModelBasicTest(TestCase, TestClassUtils):

    """
    Verify that at least test data is correct on a basic level, and the model contains
    the expected fields.
    """

    catalog_record_field_names = (
        'identifier',
        'dataset_catalog',
        'research_dataset',
        'preservation_state',
        'preservation_state_description',
        'preservation_state_modified',
        'ready_status',
        'contract_identifier',
        'mets_object_identifier',
        'catalog_record_modified',
        'dataset_group_edit',
        'files',
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
        super(CatalogRecordModelBasicTest, cls).setUpClass()

    def setUp(self):
        dataset_from_test_data = self._get_object_from_test_data('catalogrecord')
        self.identifier = dataset_from_test_data['identifier']

    def test_get_by_identifier(self):
        catalog_record = CatalogRecord.objects.get(identifier=self.identifier)
        self.assertEqual(catalog_record.identifier, self.identifier)

    # todo validation disabled until schema updated
    # def test_validate_research_dataset(self):
    #    catalog_record = CatalogRecord.objects.get(identifier=self.identifier)

    #     try:
    #         json_validate(catalog_record.research_dataset, get_json_schema('catalog_record'))
    #         json_validation_success = True
    #     except Exception as e:
    #         d(e)
    #         json_validation_success = False

    #     self.assertEqual(json_validation_success, True, 'catalog_record.research_dataset failed json schema validation')

    def test_model_fields_as_expected(self):
        actual_model_fields = [ f.name for f in CatalogRecord._meta.get_fields() ]
        self._test_model_fields_as_expected(self.catalog_record_field_names + self.common_fields_names, actual_model_fields)
