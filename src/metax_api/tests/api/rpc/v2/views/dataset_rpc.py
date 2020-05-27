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
from metax_api.models import CatalogRecordV2, DataCatalog
from metax_api.tests.utils import (
    TestClassUtils,
    get_test_oidc_token,
    test_data_file_path,
)
from metax_api.utils import (
    get_tz_aware_now_without_micros,
)


END_USER_ALLOWED_DATA_CATALOGS = settings.END_USER_ALLOWED_DATA_CATALOGS


def create_end_user_catalogs():
    dc = DataCatalog.objects.get(pk=1)
    catalog_json = dc.catalog_json
    for identifier in END_USER_ALLOWED_DATA_CATALOGS:
        catalog_json['identifier'] = identifier
        dc = DataCatalog.objects.create(
            catalog_json=catalog_json,
            date_created=get_tz_aware_now_without_micros(),
            catalog_record_services_create='testuser,api_auth_user,metax',
            catalog_record_services_edit='testuser,api_auth_user,metax'
        )


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
        response = self.client.get('/rpc/v2/datasets/get_minimal_dataset_template')
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)

        # test preventing typos
        response = self.client.get('/rpc/v2/datasets/get_minimal_dataset_template?type=wrong')
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)

        # test minimal dataset for service use
        response = self.client.get('/rpc/v2/datasets/get_minimal_dataset_template?type=service')
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertTrue('metadata_provider_org' in response.data)
        self.assertTrue('metadata_provider_user' in response.data)
        self._use_http_authorization(username='testuser')
        response = self.client.post('/rest/v2/datasets', response.data, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED)

        # test minimal dataset for end user use
        response = self.client.get('/rpc/v2/datasets/get_minimal_dataset_template?type=enduser')
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertTrue('metadata_provider_org' not in response.data)
        self.assertTrue('metadata_provider_user' not in response.data)
        self._use_http_authorization(method='bearer', token=get_test_oidc_token())
        self._mock_token_validation_succeeds()
        response = self.client.post('/rest/v2/datasets', response.data, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED)

    def test_set_preservation_identifier(self):
        self._set_http_authorization('service')

        # Parameter 'identifier' is required
        response = self.client.post('/rpc/v2/datasets/set_preservation_identifier')
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)

        # Nonexisting identifier should return 404
        response = self.client.post('/rpc/v2/datasets/set_preservation_identifier?identifier=nonexisting')
        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND)

        # Create ida data catalog
        dc = self._get_object_from_test_data('datacatalog', requested_index=0)
        dc_id = settings.IDA_DATA_CATALOG_IDENTIFIER
        dc['catalog_json']['identifier'] = dc_id
        self.client.post('/rest/v2/datacatalogs', dc, format="json")

        # Test OK ops

        # Create new ida cr without doi
        cr_json = self.client.get('/rest/v2/datasets/1').data
        cr_json.pop('preservation_identifier', None)
        cr_json.pop('identifier')
        cr_json['research_dataset'].pop('preferred_identifier', None)
        cr_json['data_catalog'] = dc_id
        cr_json['research_dataset']['issued'] = '2018-01-01'
        cr_json['research_dataset']['publisher'] = {
            '@type': 'Organization',
            'name': { 'en': 'publisher' }
        }

        response = self.client.post('/rest/v2/datasets?pid_type=urn', cr_json, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)
        identifier = response.data['identifier']

        # Verify rpc api returns the same doi as the one that is set to the datasets' preservation identifier
        response = self.client.post(f'/rpc/v2/datasets/set_preservation_identifier?identifier={identifier}')
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)

        response2 = self.client.get(f'/rest/v2/datasets/{identifier}')
        self.assertEqual(response2.status_code, status.HTTP_200_OK, response2.data)
        self.assertEqual(response.data, response2.data['preservation_identifier'], response2.data)

        # Return 400 if request is not correct datacite format
        response2.data['research_dataset'].pop('issued')
        response = self.client.put(f'/rest/v2/datasets/{identifier}', response2.data, format="json")
        self.assertEqual(response2.status_code, status.HTTP_200_OK, response2.data)

        response = self.client.post(f'/rpc/v2/datasets/set_preservation_identifier?identifier={identifier}')
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST, response.data)


class ChangeCumulativeStateRPC(CatalogRecordApiWriteCommon):

    """
    This class tests different cumulative state transitions. Different parent class is needed
    to get use of cr_test_data.
    """

    def _create_cumulative_dataset(self, state):
        self.cr_test_data['cumulative_state'] = state

        response = self.client.post('/rest/v2/datasets', self.cr_test_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)
        self.assertEqual(response.data['cumulative_state'], state, response.data)

        return response.data

    def _update_cr_cumulative_state(self, identifier, state, result=status.HTTP_204_NO_CONTENT):
        url = '/rpc/v2/datasets/change_cumulative_state?identifier=%s&cumulative_state=%d'

        response = self.client.post(url % (identifier, state), format="json")
        self.assertEqual(response.status_code, result, response.data)

        return response.data

    def _get_cr(self, identifier):
        response = self.client.get('/rest/v2/datasets/%s' % identifier, format="json")
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)

        return response.data

    def _assert_file_counts(self, new_version):
        new_count = CatalogRecordV2.objects.get(pk=new_version['id']).files.count()
        old_count = CatalogRecordV2.objects.get(pk=new_version['previous_dataset_version']['id']).files.count()
        self.assertEqual(new_count, old_count, 'file count between versions should match')

    def test_transitions_from_NO(self):
        """
        Transition from non-cumulative to active is allowed but to closed it is not.
        New version is created if non-cumulative dataset is marked actively cumulative.
        """
        cr_orig = self._create_cumulative_dataset(0)
        orig_preferred_identifier = cr_orig['research_dataset']['preferred_identifier']
        orig_record_count = CatalogRecordV2.objects.all().count()
        self._update_cr_cumulative_state(cr_orig['identifier'], 2, status.HTTP_400_BAD_REQUEST)

        self._update_cr_cumulative_state(cr_orig['identifier'], 1, status.HTTP_200_OK)
        self.assertEqual(CatalogRecordV2.objects.all().count(), orig_record_count + 1)

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
        orig_record_count = CatalogRecordV2.objects.all().count()
        self._update_cr_cumulative_state(cr['identifier'], 0, status.HTTP_400_BAD_REQUEST)
        self.assertEqual(CatalogRecordV2.objects.all().count(), orig_record_count)

        # active to non-active cumulation is legal
        self._update_cr_cumulative_state(cr['identifier'], 2)
        cr = self._get_cr(cr['identifier'])
        self.assertEqual(cr['cumulative_state'], 2, 'dataset should have changed status')

    def test_transitions_from_CLOSED(self):
        cr = self._create_cumulative_dataset(1)
        orig_record_count = CatalogRecordV2.objects.all().count()
        self._update_cr_cumulative_state(cr['identifier'], 2)
        cr = self._get_cr(cr['identifier'])
        self.assertEqual(cr['date_cumulation_ended'], cr['date_modified'], cr)

        self._update_cr_cumulative_state(cr['identifier'], 0, status.HTTP_400_BAD_REQUEST)

        # changing to active cumulative dataset creates a new version
        self._update_cr_cumulative_state(cr['identifier'], 1, status.HTTP_200_OK)
        self.assertEqual(CatalogRecordV2.objects.all().count(), orig_record_count + 1)
        old_version = self._get_cr(cr['identifier'])

        # Old dataset should not have changed
        self.assertEqual(old_version['cumulative_state'], 2, 'original status should not changed')
        self.assertTrue('next_dataset_version' in old_version, 'should have new dataset')

        # new data
        new_version = self._get_cr(old_version['next_dataset_version']['identifier'])
        self.assertEqual(new_version['cumulative_state'], 1, 'new version should have changed status')
        self.assertTrue('date_cumulation_ended' not in new_version, new_version)
        self._assert_file_counts(new_version)

    def test_correct_response_data(self):
        """
        Tests that correct information is set to response.
        """
        cr = self._create_cumulative_dataset(0)
        return_data = self._update_cr_cumulative_state(cr['identifier'], 1, status.HTTP_200_OK)
        self.assertTrue('new_version_created' in return_data, 'new_version_created should be returned')
        new_version_identifier = return_data['new_version_created']['identifier']
        cr = self._get_cr(cr['identifier'])
        self.assertEqual(cr['next_dataset_version']['identifier'], new_version_identifier)

        new_cr = self._get_cr(new_version_identifier)
        return_data = self._update_cr_cumulative_state(new_cr['identifier'], 2)
        self.assertEqual(return_data, None, 'when new version is not created, return should be None')


class CatalogRecordVersionHandling(CatalogRecordApiWriteCommon):

    """
    New dataset versions are not created automatically when changing files of a dataset.
    New dataset versions can only be created by explicitly calling related RPC API.
    """

    def test_create_new_version(self):
        """
        A new dataset version can be created for datasets in data catalogs that support versioning.
        """
        response = self.client.post('/rpc/v2/datasets/create_new_version?identifier=1', format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)

        next_version_identifier = response.data.get('identifier')

        response = self.client.get('/rest/v2/datasets/1', format="json")
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        self.assertEqual(response.data.get('next_dataset_version', {}).get('identifier'), next_version_identifier)

        response2 = self.client.get('/rest/v2/datasets/%s' % next_version_identifier, format="json")
        self.assertEqual(response2.status_code, status.HTTP_200_OK, response2.data)
        self.assertEqual(
            response2.data.get('previous_dataset_version', {}).get('identifier'), response.data['identifier']
        )

    def test_delete_new_version_draft(self):
        """
        Ensure a new version that is created into draft state can be deleted, and is permanently deleted.
        """
        response = self.client.post('/rpc/v2/datasets/create_new_version?identifier=1', format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)

        next_version_identifier = response.data.get('identifier')

        response = self.client.delete('/rest/v2/datasets/%s' % next_version_identifier, format="json")
        self.assertEqual(response.status_code, status.HTTP_204_NO_CONTENT, response.data)

        num_found = CatalogRecordV2.objects_unfiltered.filter(identifier=next_version_identifier).count()
        self.assertEqual(num_found, 0, 'draft should have been permanently deleted')
        self.assertEqual(CatalogRecordV2.objects.get(pk=1).next_dataset_version, None)

    def test_version_already_exists(self):
        """
        If a dataset already has a next version, then a new version cannot be created.
        """
        response = self.client.post('/rpc/v2/datasets/create_new_version?identifier=1', format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)

        response = self.client.post(
            '/rpc/v2/datasets/create_new_version?identifier=1', format="json"
        )
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST, response.data)
        self.assertEqual('already has a next version' in response.data['detail'][0], True, response.data)

    def test_new_version_removes_deprecated_files(self):
        """
        If a new version is created from a deprecated dataset, then the new version should have deprecated=False
        status, and all files that are no longer available should be removed from the dataset.
        """
        original_cr = CatalogRecordV2.objects.get(pk=1)

        response = self.client.delete('/rest/v2/files/1', format="json")
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)

        response = self.client.post('/rpc/v2/datasets/create_new_version?identifier=1', format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)

        new_cr = CatalogRecordV2.objects.get(pk=response.data['id'])
        self.assertEqual(new_cr.deprecated, False)
        self.assertTrue(len(new_cr.research_dataset['files']) < len(original_cr.research_dataset['files']))
        self.assertTrue(new_cr.files.count() < original_cr.files(manager='objects_unfiltered').count())

    @responses.activate
    def test_authorization(self):
        """
        Creating a new dataset version should have the same authorization rules as when normally editing a dataset.
        """

        # service use should be OK
        response = self.client.post('/rpc/v2/datasets/create_new_version?identifier=2', format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)

        # test with end user, should fail
        self.token = get_test_oidc_token(new_proxy=True)
        self._mock_token_validation_succeeds()
        self._use_http_authorization(method='bearer', token=self.token)

        response = self.client.post('/rpc/v2/datasets/create_new_version?identifier=1', format="json")
        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN, response.data)

        # change owner, try again. should be OK
        cr = CatalogRecordV2.objects.get(pk=1)
        cr.metadata_provider_user = self.token['CSCUserName']
        cr.editor = None
        cr.force_save()

        response = self.client.post('/rpc/v2/datasets/create_new_version?identifier=1', format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)


class CatalogRecordPublishing(CatalogRecordApiWriteCommon):

    def test_publish_new_dataset_draft(self):

        response = self.client.post('/rest/v2/datasets?draft', self.cr_test_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)

        cr_id = response.data['id']

        response = self.client.post('/rpc/v2/datasets/publish_dataset?identifier=%d' % cr_id, format="json")
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        self.assertTrue(response.data.get('preferred_identifier') is not None)

        response = self.client.get('/rest/v2/datasets/%d' % cr_id, format="json")
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        self.assertEqual(response.data['state'], 'published')
        self.assertEqual(
            response.data['research_dataset']['preferred_identifier'] == response.data['identifier'], False
        )

    @responses.activate
    def test_authorization(self):
        """
        Test authorization for publishing draft datasets. Note: General visibility of draft datasets has
        been tested elsewhere,so authorization failure is not tested here since unauthorized people
        should not even see the records.
        """

        # test with service
        response = self.client.post('/rest/v2/datasets?draft', self.cr_test_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)

        response = self.client.post(
            '/rpc/v2/datasets/publish_dataset?identifier=%d' % response.data['id'], format="json"
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)

        # test with end user
        self.token = get_test_oidc_token(new_proxy=True)
        self._mock_token_validation_succeeds()
        self._use_http_authorization(method='bearer', token=self.token)
        create_end_user_catalogs()

        self.cr_test_data['data_catalog'] = END_USER_ALLOWED_DATA_CATALOGS[0]
        self.cr_test_data['research_dataset'].pop('files', None)
        self.cr_test_data['research_dataset'].pop('directories', None)

        response = self.client.post('/rest/v2/datasets?draft', self.cr_test_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)

        response = self.client.post(
            '/rpc/v2/datasets/publish_dataset?identifier=%d' % response.data['id'], format="json"
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
