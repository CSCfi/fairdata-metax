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

from metax_api.tests.api.rest.base.views.datasets.write import CatalogRecordApiWriteCommon
from metax_api.models import CatalogRecord
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

        # Return 400 if request is not correct datacite format
        response2.data['research_dataset'].pop('issued')
        response = self.client.put(f'/rest/datasets/{identifier}', response2.data, format="json")
        self.assertEqual(response2.status_code, status.HTTP_200_OK, response2.data)

        response = self.client.post(f'/rpc/datasets/set_preservation_identifier?identifier={identifier}')
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST, response.data)


class ChangeCumulativeStateRPC(CatalogRecordApiWriteCommon):

    """
    This class tests different cumulative state transitions. Different parent class is needed
    to get use of cr_test_data.
    """

    def _create_cumulative_dataset(self, state):
        self.cr_test_data['cumulative_state'] = state

        response = self.client.post('/rest/datasets', self.cr_test_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)
        self.assertEqual(response.data['cumulative_state'], state, response.data)

        return response.data

    def _update_cr_cumulative_state(self, identifier, state, result=status.HTTP_204_NO_CONTENT):
        url = '/rpc/datasets/change_cumulative_state?identifier=%s&cumulative_state=%d'

        response = self.client.post(url % (identifier, state), format="json")
        self.assertEqual(response.status_code, result, response.data)

    def _get_cr(self, identifier):
        response = self.client.get('/rest/datasets/%s' % identifier, format="json")
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)

        return response.data

    def _assert_file_counts(self, new_version):
        new_count = CatalogRecord.objects.get(pk=new_version['id']).files.count()
        old_count = CatalogRecord.objects.get(pk=new_version['previous_dataset_version']['id']).files.count()
        self.assertEqual(new_count, old_count, 'file count between versions should match')

    def test_transitions_from_NO(self):
        """
        Transition from non-cumulative to active is allowed but to closed it is not.
        New version is created if non-cumulative dataset is marked actively cumulative.
        """
        cr_orig = self._create_cumulative_dataset(0)
        orig_preferred_identifier = cr_orig['research_dataset']['preferred_identifier']
        orig_record_count = CatalogRecord.objects.all().count()
        self._update_cr_cumulative_state(cr_orig['identifier'], 2, status.HTTP_400_BAD_REQUEST)

        self._update_cr_cumulative_state(cr_orig['identifier'], 1)
        self.assertEqual(CatalogRecord.objects.all().count(), orig_record_count + 1)

        # get updated dataset
        old_version = self._get_cr(cr_orig['identifier'])
        self.assertEqual(old_version['cumulative_state'], 0, 'original status should not changed')
        self.assertTrue('next_dataset_version' in old_version, 'should have new dataset')

        # cannot change old dataset cumulative_status
        self._update_cr_cumulative_state(old_version['identifier'], 2, status.HTTP_400_BAD_REQUEST)

        # new version of the dataset should have new cumulative state
        new_version = self._get_cr(old_version['next_dataset_version']['identifier'])
        self.assertTrue(new_version['research_dataset']['preferred_identifier'] != orig_preferred_identifier)
        self.assertEqual(new_version['cumulative_state'], 1, 'new version should have changed status')
        self._assert_file_counts(new_version)

    def test_transitions_from_YES(self):
        cr = self._create_cumulative_dataset(1)
        orig_record_count = CatalogRecord.objects.all().count()
        self._update_cr_cumulative_state(cr['identifier'], 0, status.HTTP_400_BAD_REQUEST)
        self.assertEqual(CatalogRecord.objects.all().count(), orig_record_count)

        # active to non-active cumulation is legal
        self._update_cr_cumulative_state(cr['identifier'], 2)
        cr = self._get_cr(cr['identifier'])
        self.assertEqual(cr['cumulative_state'], 2, 'dataset should have changed status')

    def test_transitions_from_CLOSED(self):
        cr = self._create_cumulative_dataset(1)
        orig_record_count = CatalogRecord.objects.all().count()
        self._update_cr_cumulative_state(cr['identifier'], 2)
        cr = self._get_cr(cr['identifier'])
        self.assertEqual(cr['date_cumulation_ended'], cr['date_modified'], cr)

        self._update_cr_cumulative_state(cr['identifier'], 0, status.HTTP_400_BAD_REQUEST)

        # changing to active cumulative dataset creates a new version
        self._update_cr_cumulative_state(cr['identifier'], 1)
        self.assertEqual(CatalogRecord.objects.all().count(), orig_record_count + 1)
        old_version = self._get_cr(cr['identifier'])

        # Old dataset should not have changed
        self.assertEqual(old_version['cumulative_state'], 2, 'original status should not changed')
        self.assertTrue('next_dataset_version' in old_version, 'should have new dataset')

        # new data
        new_version = self._get_cr(old_version['next_dataset_version']['identifier'])
        self.assertEqual(new_version['cumulative_state'], 1, 'new version should have changed status')
        self.assertTrue('date_cumulation_ended' not in new_version, new_version)
        self._assert_file_counts(new_version)
