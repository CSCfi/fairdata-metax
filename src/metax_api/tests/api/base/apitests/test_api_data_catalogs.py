from rest_framework import status
from rest_framework.test import APITestCase
from django.core.management import call_command

from metax_api.tests.utils import test_data_file_path, TestClassUtils


class DataCatalogApiReadTestV1(APITestCase, TestClassUtils):

    @classmethod
    def setUpClass(cls):
        """
        Loaded only once for test cases inside this class.
        """
        call_command('loaddata', test_data_file_path, verbosity=0)
        super(DataCatalogApiReadTestV1, cls).setUpClass()

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
        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND)
        self.assertFalse(response.data)


class DataCatalogApiWriteTestV1(APITestCase, TestClassUtils):

    @classmethod
    def setUpClass(cls):
        """
        Loaded only once for test cases inside this class.
        """
        call_command('loaddata', test_data_file_path, verbosity=0)
        super(DataCatalogApiWriteTestV1, cls).setUpClass()

    def setUp(self):
        self.new_test_data = self._get_object_from_test_data('datacatalog')
        self.new_test_data.pop('id')
        self.new_test_data['catalog_json'].pop('identifier')
        self._use_http_authorization()

    def test_identifier_is_auto_generated(self):
        response = self.client.post('/rest/datacatalogs', self.new_test_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED)
        self.assertNotEqual(response.data['catalog_json'].get('identifier', None), None, 'identifier should be created')

    def test_research_dataset_schema_missing_ok(self):
        self.new_test_data['catalog_json'].pop('research_dataset_schema', None)
        response = self.client.post('/rest/datacatalogs', self.new_test_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED)

    def test_research_dataset_schema_not_found_error(self):
        self.new_test_data['catalog_json']['research_dataset_schema'] = 'notfound'
        response = self.client.post('/rest/datacatalogs', self.new_test_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST, response.data)
