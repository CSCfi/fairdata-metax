# This file is part of the Metax API service
#
# Copyright 2017-2018 Ministry of Education and Culture, Finland
#
# :author: CSC - IT Center for Science Ltd., Espoo Finland <servicedesk@csc.fi>
# :license: MIT

from django.conf import settings
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

    def test_set_preservation_identifier(self):
        self._set_http_authorization('service')

        # Parameter 'identifier' is required
        response = self.client.post('/rpc/datasets/set_preservation_identifier')
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)

        # Nonexisting identifier should return 404
        response = self.client.post('/rpc/datasets/set_preservation_identifier?identifier=nonexisting')
        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND)

        # Create ida data catalog
        dc = self._get_object_from_test_data('datacatalog', requested_index=0)
        dc_id = settings.IDA_DATA_CATALOG_IDENTIFIER
        dc['catalog_json']['identifier'] = dc_id
        self.client.post('/rest/datacatalogs', dc, format="json")

        # Test OK ops

        # Create new ida cr without doi
        cr_json = self.client.get('/rest/datasets/1').data
        cr_json.pop('preservation_identifier', None)
        cr_json.pop('identifier')
        cr_json['research_dataset'].pop('preferred_identifier', None)
        cr_json['data_catalog'] = dc_id
        cr_json['research_dataset']['issued'] = '2018-01-01'
        cr_json['research_dataset']['publisher'] = {
            '@type': 'Organization',
            'name': { 'en': 'publisher' }
        }

        response = self.client.post('/rest/datasets?pid_type=urn', cr_json, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)
        identifier = response.data['identifier']

        # Verify rpc api returns the same doi as the one that is set to the datasets' preservation identifier
        response = self.client.post(f'/rpc/datasets/set_preservation_identifier?identifier={identifier}')
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)

        response2 = self.client.get(f'/rest/datasets/{identifier}')
        self.assertEqual(response2.status_code, status.HTTP_200_OK, response2.data)
        self.assertEqual(response.data, response2.data['preservation_identifier'], response2.data)
