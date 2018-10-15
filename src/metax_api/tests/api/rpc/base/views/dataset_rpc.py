# This file is part of the Metax API service
#
# Copyright 2017-2018 Ministry of Education and Culture, Finland
#
# :author: CSC - IT Center for Science Ltd., Espoo Finland <servicedesk@csc.fi>
# :license: MIT

from django.core.management import call_command
from rest_framework import status
from rest_framework.test import APITestCase
import responses

from metax_api.tests.utils import TestClassUtils, get_test_oidc_token, test_data_file_path


class DatasetRPCTests(APITestCase, TestClassUtils):

    @classmethod
    def setUpClass(cls):
        """
        Loaded only once for test cases inside this class.
        """
        super().setUpClass()
        call_command('loaddata', test_data_file_path, verbosity=0)

    def setUp(self):
        super().setUp()
        self.create_end_user_data_catalogs()

    @responses.activate
    def test_get_minimal_dataset_template(self):
        """
        Retrieve and use a minimal dataset template example from the api.
        """

        # query param type is missing, should return error and description what to do.
        response = self.client.get('/rpc/datasets/get_minimal_dataset_template')
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)

        # test preventing typos
        response = self.client.get('/rpc/datasets/get_minimal_dataset_template?type=wrong')
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)

        # test minimal dataset for service use
        response = self.client.get('/rpc/datasets/get_minimal_dataset_template?type=service')
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertTrue('metadata_provider_org' in response.data)
        self.assertTrue('metadata_provider_user' in response.data)
        self._use_http_authorization(username='testuser')
        response = self.client.post('/rest/datasets', response.data, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED)

        # test minimal dataset for end user use
        response = self.client.get('/rpc/datasets/get_minimal_dataset_template?type=enduser')
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertTrue('metadata_provider_org' not in response.data)
        self.assertTrue('metadata_provider_user' not in response.data)
        self._use_http_authorization(method='bearer', token=get_test_oidc_token())
        self._mock_token_validation_succeeds()
        response = self.client.post('/rest/datasets', response.data, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED)
