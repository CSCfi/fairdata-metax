from rest_framework import status
from rest_framework.test import APITestCase
from django.core.management import call_command

from metax_api.tests.utils import test_data_file_path, TestClassUtils


class DataCatalogApiReadBasicTests(APITestCase, TestClassUtils):

    @classmethod
    def setUpClass(cls):
        """
        Loaded only once for test cases inside this class.
        """
        call_command('loaddata', test_data_file_path, verbosity=0)
        super(DataCatalogApiReadBasicTests, cls).setUpClass()

    def setUp(self):
        data_catalog_from_test_data = self._get_object_from_test_data('datacatalog', requested_index=0)
        self.pk = data_catalog_from_test_data['id']
        self.identifier = data_catalog_from_test_data['catalog_json']['identifier']

    def test_read_data_catalog_exists(self):
        response = self.client.get('/rest/datacatalogs/%s/exists' % self.pk)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertTrue(response.data)
        response = self.client.get('/rest/datacatalogs/%s/exists' % self.identifier)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertTrue(response.data)

    def test_read_catalog_record_does_not_exist(self):
        response = self.client.get('/rest/datacatalogs/%s/exists' % 'urn:nbn:fi:non_existing_data_catalog_identifier')
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertFalse(response.data)
