from django.core.management import call_command
from rest_framework import status
from rest_framework.test import APITestCase
from metax_api.models import DataCatalog

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

    def test_removed_parameter_gets_correct_amount_of_objects(self):
        path = '/rest/datacatalogs'
        objects = DataCatalog.objects.all().values()

        results = self.client.get('{0}?no_pagination&removed=false'.format(path)).json()
        initial_amt = len(results)

        results = self.client.get('{0}?no_pagination&removed=true'.format(path)).json()
        self.assertEqual(len(results), 0, "Without removed objects remove=true should return 0 results")

        self._use_http_authorization()
        amt_to_delete = 2
        for i in range(amt_to_delete):
            response = self.client.delete('{0}/{1}'.format(path, objects[i]['id']))
            self.assertEqual(response.status_code, status.HTTP_204_NO_CONTENT, "Deleting object failed")

        results = self.client.get('{0}?no_pagination&removed=false'.format(path)).json()
        self.assertEqual(len(results), initial_amt - amt_to_delete, "Non-removed object amount is incorrect")

        results = self.client.get('{0}?no_pagination&removed=true'.format(path)).json()
        self.assertEqual(len(results), amt_to_delete, "Removed object amount is incorrect")
