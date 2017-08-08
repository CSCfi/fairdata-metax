from rest_framework import status
from rest_framework.test import APITestCase
from django.core.management import call_command

from metax_api.tests.utils import test_data_file_path, TestClassUtils


class DatasetCatalogApiReadTestV1(APITestCase, TestClassUtils):

    @classmethod
    def setUpClass(cls):
        """
        Loaded only once for test cases inside this class.
        """
        call_command('loaddata', test_data_file_path, verbosity=0)
        super(DatasetCatalogApiReadTestV1, cls).setUpClass()

    def setUp(self):
        dataset_catalog_from_test_data = self._get_object_from_test_data('datasetcatalog', requested_index=0)
        self.pk = dataset_catalog_from_test_data['id']
        self.identifier = dataset_catalog_from_test_data['catalog_json']['identifier']

    def test_read_dataset_catalog_exists(self):
        response = self.client.get('/rest/datasetcatalogs/%s/exists' % self.pk)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertTrue(response.data)
        response = self.client.get('/rest/datasetcatalogs/%s/exists' % self.identifier)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertTrue(response.data)

    def test_read_catalog_record_does_not_exist(self):
        response = self.client.get('/rest/datasetcatalogs/%s/exists' % 'urn:nbn:fi:non_existing_dataset_catalog_identifier')
        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND)
        self.assertFalse(response.data)