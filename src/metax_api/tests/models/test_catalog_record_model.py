from jsonschema import validate as json_validate

from django.core.management import call_command
from django.test import TestCase

from metax_api.tests.utils import get_json_schema, test_data_file_path, TestClassUtils
from metax_api.models import CatalogRecord

class CatalogRecordModelBasicTest(TestCase, TestClassUtils):

    """
    Verify that at least test data is correct on a basic level, and the model contains
    the expected fields.
    """

    catalog_record_field_names = (
        'contract',
        'dataset_catalog',
        'research_dataset',
        'preservation_state',
        'preservation_state_modified',
        'preservation_description',
        'preservation_reason_description',
        'mets_object_identifier',
        'dataset_group_edit',
        'files',
        'next_version_id',
        'next_version_identifier',
        'previous_version_id',
        'previous_version_identifier',
        'version_created',

        # the field through which the object of the next/prev version can be directly accessed
        'next_version',
        'previous_version',
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
        self.urn_identifier = dataset_from_test_data['research_dataset']['urn_identifier']

    def test_get_by_identifier(self):
        catalog_record = CatalogRecord.objects.get(research_dataset__contains={ 'urn_identifier': self.urn_identifier })
        self.assertEqual(catalog_record.urn_identifier, self.urn_identifier)

    def test_validate_research_dataset(self):
        catalog_record = CatalogRecord.objects.get(research_dataset__contains={ 'urn_identifier': self.urn_identifier })

        try:
            json_validate(catalog_record.research_dataset, get_json_schema('dataset'))
            json_validation_success = True
        except Exception as e:
            print(e)
            json_validation_success = False

        self.assertEqual(json_validation_success, True, 'catalog_record.research_dataset failed json schema validation')

    def test_model_fields_as_expected(self):
        actual_model_fields = [ f.name for f in CatalogRecord._meta.get_fields() ]
        self._test_model_fields_as_expected(self.catalog_record_field_names + self.common_fields_names, actual_model_fields)
