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

from metax_api.tests.api.rest.base.views.datasets.write import CatalogRecordApiWriteAssignFilesCommon, \
    CatalogRecordApiWriteCommon
from metax_api.models import CatalogRecord, Directory
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

        return response.data

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

        self._update_cr_cumulative_state(cr_orig['identifier'], 1, status.HTTP_200_OK)
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
        self._update_cr_cumulative_state(cr['identifier'], 1, status.HTTP_200_OK)
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


class RefreshDirectoryContent(CatalogRecordApiWriteAssignFilesCommon):

    url = '/rpc/datasets/refresh_directory_content?cr_identifier=%s&dir_identifier=%s'

    def test_refresh_adds_new_files(self):
        self._add_directory(self.cr_test_data, '/TestExperiment')
        response = self.client.post('/rest/datasets', self.cr_test_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)
        cr_id = response.data['identifier']
        dir_id = response.data['research_dataset']['directories'][0]['identifier']

        # freeze two files to /TestExperiment/Directory_2
        self._freeze_files_to_root()
        response = self.client.post(self.url % (cr_id, dir_id), format="json")
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)

        new_version = CatalogRecord.objects.get(id=response.data['new_version_created']['id'])
        self.assertEqual(new_version.files.count(), new_version.previous_dataset_version.files.count() + 2)

        # freeze two files to /TestExperiment/Directory_2/Group_3
        self._freeze_new_files()
        response = self.client.post(self.url % (new_version.identifier, dir_id), format="json")
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)

        new_version = CatalogRecord.objects.get(id=response.data['new_version_created']['id'])
        self.assertEqual(new_version.files.count(), new_version.previous_dataset_version.files.count() + 2)

    def test_adding_parent_dir_allows_refreshes_to_child_dirs(self):
        """
        When parent directory is added to dataset, refreshes to child directories are also possible.
        """
        self._add_directory(self.cr_test_data, '/TestExperiment/Directory_2')
        response = self.client.post('/rest/datasets', self.cr_test_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)
        cr_id = response.data['identifier']

        self._freeze_new_files()
        frozen_dir = Directory.objects.filter(directory_path='/TestExperiment/Directory_2/Group_3').first()

        response = self.client.post(self.url % (cr_id, frozen_dir.identifier), format="json")
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)

        new_version = CatalogRecord.objects.get(id=response.data['new_version_created']['id'])
        self.assertEqual(new_version.files.count(), new_version.previous_dataset_version.files.count() + 2)

    def test_refresh_adds_new_files_multiple_locations(self):
        self._add_directory(self.cr_test_data, '/TestExperiment/Directory_2')
        response = self.client.post('/rest/datasets', self.cr_test_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)
        cr_id = response.data['identifier']
        dir_id = response.data['research_dataset']['directories'][0]['identifier']

        self._freeze_new_files()
        self._freeze_files_to_root()
        response = self.client.post(self.url % (cr_id, dir_id), format="json")
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)

        new_version = CatalogRecord.objects.get(id=response.data['new_version_created']['id'])
        self.assertEqual(new_version.files.count(), new_version.previous_dataset_version.files.count() + 4)

    def test_refresh_adds_no_new_files_from_upper_dirs(self):
        """
        Include parent/subdir and freeze files to parent. Should be no changes in the dataset.
        """
        self._add_directory(self.cr_test_data, '/TestExperiment/Directory_2/Group_2')
        response = self.client.post('/rest/datasets', self.cr_test_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)
        cr_id = response.data['identifier']
        dir_id = response.data['research_dataset']['directories'][0]['identifier']
        file_count_before = CatalogRecord.objects.get(identifier=cr_id).files.count()

        self._freeze_files_to_root()
        response = self.client.post(self.url % (cr_id, dir_id), format="json")
        self.assertEqual(response.status_code, status.HTTP_204_NO_CONTENT, response.data)

        cr_after = CatalogRecord.objects.get(identifier=cr_id)
        self.assertEqual(cr_after.next_dataset_version, None, 'should not have new dataset version')
        self.assertEqual(cr_after.files.count(), file_count_before, 'No new files should be added')

    def test_refresh_with_cumulative_state_yes(self):
        """
        When dataset has cumulation active, files are added to dataset but no new version is created.
        """
        self._add_directory(self.cr_test_data, '/TestExperiment/Directory_2')
        self.cr_test_data['cumulative_state'] = 1
        response = self.client.post('/rest/datasets', self.cr_test_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)
        cr_id = response.data['identifier']
        dir_id = response.data['research_dataset']['directories'][0]['identifier']
        file_count_before = CatalogRecord.objects.get(identifier=cr_id).files.count()

        self._freeze_new_files()
        self._freeze_files_to_root()
        response = self.client.post(self.url % (cr_id, dir_id), format="json")
        self.assertEqual(response.status_code, status.HTTP_204_NO_CONTENT, response.data)

        cr_after = CatalogRecord.objects.get(identifier=cr_id)
        self.assertEqual(cr_after.next_dataset_version, None, 'should not have new dataset version')
        self.assertEqual(cr_after.files.count(), file_count_before + 4)

    def test_refreshing_deprecated_dataset_is_not_allowed(self):
        self._add_directory(self.cr_test_data, '/TestExperiment/Directory_2')
        response = self.client.post('/rest/datasets', self.cr_test_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)
        cr_id = response.data['identifier']
        dir_id = response.data['research_dataset']['directories'][0]['identifier']

        removed_file_id = CatalogRecord.objects.get(identifier=cr_id).files.all()[0].id
        response = self.client.delete(f'/rest/files/{removed_file_id}')
        self.assertEqual(response.status_code, status.HTTP_200_OK)

        response = self.client.get(f'/rest/datasets/{cr_id}')
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        depr_cr = response.data
        self._freeze_new_files()
        response = self.client.post(self.url % (depr_cr['identifier'], dir_id), format="json")
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST, response.data)

    def test_adding_files_from_non_assigned_dir_is_not_allowed(self):
        """
        Only allow adding files from directories which paths are included in the research dataset.
        """
        self._add_directory(self.cr_test_data, '/SecondExperiment/Data')
        response = self.client.post('/rest/datasets', self.cr_test_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)
        cr_id = response.data['identifier']

        # create another dataset so that dir /SecondExperiment/Data_Config will be created
        self._add_directory(self.cr_test_data, '/SecondExperiment/Data_Config')
        response = self.client.post('/rest/datasets', self.cr_test_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)
        dir_id = response.data['research_dataset']['directories'][1]['identifier']

        response = self.client.post(self.url % (cr_id, dir_id), format="json")
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST, response.data)
        self.assertTrue('not included' in response.data['detail'], response.data)
