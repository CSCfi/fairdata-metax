from django.core.management import call_command
from metax_api.tests.utils import test_data_file_path, TestClassUtils
from rest_framework import status
from rest_framework.test import APITestCase


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
