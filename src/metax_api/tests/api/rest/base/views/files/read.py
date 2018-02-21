from django.core.management import call_command
from django.db import connection
from rest_framework import status
from rest_framework.test import APITestCase

from metax_api.models import File
from metax_api.tests.utils import test_data_file_path, TestClassUtils


class FileApiReadCommon(APITestCase, TestClassUtils):
    @classmethod
    def setUpClass(cls):
        """
        Loaded only once for test cases inside this class.
        """
        call_command('loaddata', test_data_file_path, verbosity=0)
        super(FileApiReadCommon, cls).setUpClass()

    def setUp(self):
        file_from_test_data = self._get_object_from_test_data('file')
        self.identifier = file_from_test_data['identifier']
        self.pk = file_from_test_data['id']
        self._use_http_authorization()


class FileApiReadBasicTests(FileApiReadCommon):
    def test_read_file_list(self):
        response = self.client.get('/rest/files')
        self.assertEqual(response.status_code, status.HTTP_200_OK)

    def test_read_file_details_by_pk(self):
        response = self.client.get('/rest/files/%s' % self.pk)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(hasattr(response, 'data'), True, 'Request response object is missing attribute \'data\'')
        self.assertEqual('file_name' in response.data.keys(), True)
        self.assertEqual(response.data['identifier'], self.identifier)

    def test_read_file_details_by_identifier(self):
        response = self.client.get('/rest/files/%s' % self.identifier)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(hasattr(response, 'data'), True, 'Request response object is missing attribute \'data\'')
        self.assertEqual('file_name' in response.data.keys(), True)
        self.assertEqual(response.data['identifier'], self.identifier)

    def test_read_file_details_not_found(self):
        response = self.client.get('/rest/files/shouldnotexist')
        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND)

    def test_read_file_details_checksum_relation(self):
        response = self.client.get('/rest/files/%s' % self.pk)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual('checksum' in response.data, True)
        self.assertEqual('value' in response.data['checksum'], True)

    def test_removed_parameter_gets_correct_amount_of_objects(self):
        path = '/rest/files'
        objects = File.objects.all().values()

        results = self.client.get('{0}?no_pagination&removed=false'.format(path)).json()
        initial_amt = len(results)

        results = self.client.get('{0}?no_pagination&removed=true'.format(path)).json()
        self.assertEqual(len(results), 0, "Without removed objects remove=true should return 0 results")

        self._use_http_authorization()
        amt_to_delete = 2
        for i in range(amt_to_delete):
            response = self.client.delete('{0}/{1}'.format(path, objects[i]['id']))
            self.assertEqual(response.status_code, status.HTTP_200_OK, "Deleting object failed")

        results = self.client.get('{0}?no_pagination&removed=false'.format(path)).json()
        self.assertEqual(len(results), initial_amt - amt_to_delete, "Non-removed object amount is incorrect")

        results = self.client.get('{0}?no_pagination&removed=true'.format(path)).json()
        self.assertEqual(len(results), amt_to_delete, "Removed object amount is incorrect")


class FileApiReadGetRelatedDatasets(FileApiReadCommon):

    def test_get_related_datasets_ok_1(self):
        """
        File pk 1 should belong to only 3 datasets
        """
        response = self.client.post('/rest/files/datasets', [1], format='json')
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        self._assert_results_length(response, 3)

    def test_get_related_datasets_ok_2(self):
        """
        File identifiers listed below should belong to 5 datasets
        """
        file_identifiers = File.objects.filter(id__in=[1, 2, 3, 4, 5]).values_list('identifier', flat=True)
        response = self.client.post('/rest/files/datasets', file_identifiers, format='json')
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        self._assert_results_length(response, 5)

    def test_get_related_datasets_files_not_found(self):
        """
        When the files themselves are not found, 404 should be returned
        """
        response = self.client.post('/rest/files/datasets', ['doesnotexist'], format='json')
        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND, response.data)

    def test_get_related_datasets_records_not_found(self):
        """
        When files are found, but no records for them, an empty list should be returned
        """
        with connection.cursor() as cr:
            # detach file pk 1 from any datasets
            cr.execute('delete from metax_api_catalogrecord_files where file_id = 1')

        response = self.client.post('/rest/files/datasets', [1], format='json')
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        self._assert_results_length(response, 0)

    def _assert_results_length(self, response, length):
        self.assertEqual(isinstance(response.data, list), True, response.data)
        self.assertEqual(len(response.data), length)
