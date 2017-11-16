from django.core.management import call_command
from rest_framework import status
from rest_framework.test import APITestCase

from metax_api.tests.utils import test_data_file_path, TestClassUtils

class DirectoryApiReadCommon(APITestCase, TestClassUtils):

    @classmethod
    def setUpClass(cls):
        """
        Loaded only once for test cases inside this class.
        """
        call_command('loaddata', test_data_file_path, verbosity=0)
        super(DirectoryApiReadCommon, cls).setUpClass()

    def setUp(self):
        dir_from_test_data = self._get_object_from_test_data('directory')
        self.identifier = dir_from_test_data['identifier']
        self.pk = dir_from_test_data['id']


class DirectoryApiReadBasicTests(DirectoryApiReadCommon):

    def test_read_directory_list(self):
        response = self.client.get('/rest/directories')
        self.assertEqual(response.status_code, status.HTTP_200_OK)

    def test_read_directory_details_by_pk(self):
        response = self.client.get('/rest/directories/%s' % self.pk)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(hasattr(response, 'data'), True, 'Request response object is missing attribute \'data\'')
        self.assertEqual('directory_name' in response.data.keys(), True)
        self.assertEqual(response.data['identifier'], self.identifier)

    def test_read_directory_details_by_identifier(self):
        response = self.client.get('/rest/directories/%s' % self.identifier)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(hasattr(response, 'data'), True, 'Request response object is missing attribute \'data\'')
        self.assertEqual('directory_name' in response.data.keys(), True)
        self.assertEqual(response.data['identifier'], self.identifier)

    def test_read_directory_details_not_found(self):
        response = self.client.get('/rest/directories/shouldnotexist')
        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND)

    def test_read_directory_get_files(self):
        response = self.client.get('/rest/directories/%s/files' % self.pk)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        # self.assertEqual('not implemented yet !' in response.data, True)
