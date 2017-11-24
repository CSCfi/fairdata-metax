from django.core.management import call_command
from django.test import TestCase

from metax_api.models import DataCatalog
from metax_api.tests.utils import test_data_file_path, TestClassUtils


class DataCatalogModelTests(TestCase, TestClassUtils):

    @classmethod
    def setUpClass(cls):
        """
        Loaded only once for test cases inside this class.
        """
        call_command('loaddata', test_data_file_path, verbosity=0)
        super(DataCatalogModelTests, cls).setUpClass()

    def setUp(self):
        self.dc = DataCatalog.objects.get(pk=1)

    def test_disallow_identifier_manual_update(self):
        dc = self.dc
        old = dc.catalog_json['identifier']
        dc.catalog_json['identifier'] = 'changed value'
        dc.save()
        self.assertEqual(old, dc.catalog_json['identifier'])

    def test_identifier_is_auto_generated(self):
        dc_from_test_data = self._get_object_from_test_data('datacatalog')
        dc_from_test_data.pop('id')
        dc_from_test_data['catalog_json'].pop('identifier')
        new_dc = DataCatalog(**dc_from_test_data)
        new_dc.save()
        self.assertNotEqual(new_dc.catalog_json.get('identifier', None), None)
