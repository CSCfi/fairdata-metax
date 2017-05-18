from django.core.management import call_command

from rest_framework import status
from rest_framework.test import APITestCase

class ReadFilesTest(APITestCase):

    def setUp(self):
        call_command('loaddata', 'metax_api/tests/test_data.json')

    def test_read_file_list(self):
        response = self.client.get('/rest/files/')
        self.assertEqual(response.status_code, status.HTTP_200_OK)

    def test_read_file_details(self):
        response = self.client.get('/rest/files/', args=['f00bc05b-566e-4267-b82e-d956dcb7bda8'])
        self.assertEqual(response.status_code, status.HTTP_200_OK)
