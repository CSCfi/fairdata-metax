# This file is part of the Metax API service
#
# Copyright 2017-2018 Ministry of Education and Culture, Finland
#
# :author: CSC - IT Center for Science Ltd., Espoo Finland <servicedesk@csc.fi>
# :license: MIT

from os import makedirs
from shutil import rmtree

from django.conf import settings
from django.core.management import call_command
from rest_framework import status
from rest_framework.test import APITestCase

from metax_api.tests.utils import test_data_file_path, TestClassUtils


class ApiErrorReadBasicTests(APITestCase, TestClassUtils):

    """
    Basic read operations
    """

    @classmethod
    def setUpClass(cls):
        """
        Loaded only once for test cases inside this class.
        """
        call_command('loaddata', test_data_file_path, verbosity=0)
        super(ApiErrorReadBasicTests, cls).setUpClass()

    def setUp(self):
        super(ApiErrorReadBasicTests, self).setUp()
        rmtree(settings.ERROR_FILES_PATH, ignore_errors=True)
        makedirs(settings.ERROR_FILES_PATH)
        metax_user = settings.API_METAX_USER
        self._use_http_authorization(username=metax_user['username'], password=metax_user['password'])

    def _assert_fields_presence(self, response):
        """
        Check presence and absence of some key information.
        """
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        self.assertEqual('data' in response.data, True, response.data)
        self.assertEqual('response' in response.data, True, response.data)
        self.assertEqual('traceback' in response.data, True, response.data)
        self.assertEqual('url' in response.data, True, response.data)
        self.assertEqual('HTTP_AUTHORIZATION' in response.data['headers'], False, response.data['headers'])

    def test_list_errors(self):
        """
        Each requesting resulting in an error should leave behind one API error entry.
        """
        cr_1 = self.client.get('/rest/v2/datasets/1').data
        cr_1.pop('id')
        cr_1.pop('identifier')
        cr_1.pop('data_catalog') # causes an error

        response = self.client.post('/rest/v2/datasets', cr_1, format='json')
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        response = self.client.post('/rest/v2/datasets', cr_1, format='json')
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)

        response = self.client.get('/rest/v2/apierrors')
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)

    def test_get_error_details(self):
        cr_1 = self.client.get('/rest/v2/datasets/1').data
        cr_1.pop('id')
        cr_1.pop('identifier')
        cr_1.pop('data_catalog') # causes an error
        cr_1['research_dataset']['title'] = { 'en': 'Abc' }

        response = self.client.post('/rest/v2/datasets', cr_1, format='json')
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)

        # list errors in order to get error identifier
        response = self.client.get('/rest/v2/apierrors')
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        self.assertEqual('identifier' in response.data[0], True, response.data)

        response = self.client.get('/rest/v2/apierrors/%s' % response.data[0]['identifier'])
        self._assert_fields_presence(response)
        self.assertEqual('data_catalog' in response.data['response'], True, response.data['response'])
        self.assertEqual(response.data['data']['research_dataset']['title']['en'], 'Abc',
            response.data['data']['research_dataset']['title'])

    def test_delete_error_details(self):
        cr_1 = self.client.get('/rest/v2/datasets/1').data
        cr_1.pop('id')
        cr_1.pop('identifier')
        cr_1.pop('data_catalog') # causes an error

        response = self.client.post('/rest/v2/datasets', cr_1, format='json')
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)

        response = self.client.get('/rest/v2/apierrors')
        response = self.client.delete('/rest/v2/apierrors/%s' % response.data[0]['identifier'])
        self.assertEqual(response.status_code, status.HTTP_204_NO_CONTENT, response.data)

        response = self.client.get('/rest/v2/apierrors')
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)

    def test_delete_all_error_details(self):
        cr_1 = self.client.get('/rest/v2/datasets/1').data
        cr_1.pop('id')
        cr_1.pop('identifier')
        cr_1.pop('data_catalog') # causes an error

        response = self.client.post('/rest/v2/datasets', cr_1, format='json')
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST, response.data)
        response = self.client.post('/rest/v2/datasets', cr_1, format='json')
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST, response.data)

        # ensure something was produced...
        response = self.client.get('/rest/v2/apierrors')

        response = self.client.post('/rest/v2/apierrors/flush')
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)

        response = self.client.get('/rest/v2/apierrors')
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)

    def test_bulk_operation_produces_error_entry(self):
        """
        Ensure also bulk operations produce error entries.
        """
        cr_1 = self.client.get('/rest/v2/datasets/1').data
        cr_1.pop('id')
        cr_1.pop('identifier')
        cr_1.pop('data_catalog') # causes an error
        response = self.client.post('/rest/v2/datasets', [cr_1, cr_1], format='json')
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)

        response = self.client.get('/rest/v2/apierrors')
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)

        response = self.client.get('/rest/v2/apierrors/%s' % response.data[0]['identifier'])
        self._assert_fields_presence(response)
        self.assertEqual('other' in response.data, True, response.data)
        self.assertEqual('bulk_request' in response.data['other'], True, response.data)
        self.assertEqual('data_row_count' in response.data['other'], True, response.data)

    def test_api_permitted_only_to_metax_user(self):
        # uses testuser by default
        self._use_http_authorization()
        response = self.client.get('/rest/v2/apierrors')
        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN)
        response = self.client.get('/rest/v2/apierrors/123')
        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN)
        response = self.client.delete('/rest/v2/apierrors/123')
        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN)
        response = self.client.post('/rest/v2/apierrors/flush_errors')
        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN)
