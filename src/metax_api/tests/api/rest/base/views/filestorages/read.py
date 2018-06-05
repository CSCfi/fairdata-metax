from django.core.management import call_command
from rest_framework import status
from rest_framework.test import APITestCase

from metax_api.models import FileStorage
from metax_api.tests.utils import test_data_file_path, TestClassUtils


class FileStorageApiReadBasicTests(APITestCase, TestClassUtils):

    @classmethod
    def setUpClass(cls):
        """
        Loaded only once for test cases inside this class.
        """
        call_command('loaddata', test_data_file_path, verbosity=0)
        super(FileStorageApiReadBasicTests, cls).setUpClass()

    def setUp(self):
        self._use_http_authorization()

    def test_basic_get(self):
        fs = FileStorage.objects.get(pk=1)
        response = self.client.get('/rest/filestorages/%d' % fs.id)
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)

        response = self.client.get('/rest/filestorages/%s' % fs.file_storage_json['identifier'])
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)

    def test_basic_list(self):
        response = self.client.get('/rest/filestorages')
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        self.assertEqual(len(response.data['results']), FileStorage.objects.all().count(), response.data)
