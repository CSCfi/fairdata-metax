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
from metax_api.models import CatalogRecord, File
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


class FixDeprecatedTests(APITestCase, TestClassUtils):
    """
    Tests for fix_deprecated api. Tests remove files/directories from database and then checks that
    fix_deprecated api removes those files/directories from the given dataset.
    """

    def setUp(self):
        super().setUp()
        call_command('loaddata', test_data_file_path, verbosity=0)
        self._use_http_authorization()

    def _get_next_dataset_version(self, identifier):
        """
        Returns next dataset version for dataset <identifier>
        """
        response = self.client.get('/rest/datasets/%s' % identifier)
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        self.assertTrue('next_dataset_version' in response.data, 'new dataset should be created')
        response = self.client.get('/rest/datasets/%s' % response.data['next_dataset_version']['identifier'])
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)

        return response.data

    def test_fix_deprecated_files(self):
        file_count_before = CatalogRecord.objects.get(pk=1).files.count()

        # delete file from dataset
        deleted_file = File.objects.get(pk=1)
        response = self.client.delete('/rest/files/%s' % deleted_file.identifier)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        response = self.client.get('/rest/datasets/1')
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertTrue(response.data['deprecated'], 'dataset should be deprecated')
        identifier = response.data['identifier']

        # fix deprecated dataset
        response = self.client.post('/rpc/datasets/fix_deprecated?identifier=%s' % identifier)
        self.assertEqual(response.status_code, status.HTTP_204_NO_CONTENT, response.data)

        # ensure that new dataset version is not deprecated, removed file is deleted from research_dataset
        # and dataset files contain only the non-removed file
        new_cr_version = self._get_next_dataset_version(identifier)
        file_count_after = CatalogRecord.objects.get(pk=new_cr_version['id']).files.count()
        self.assertEqual(new_cr_version['deprecated'], False, 'deprecated flag should be fixed')
        self.assertEqual(file_count_after, file_count_before - 1, 'new file count should be smaller than before delete')
        self.assertTrue(deleted_file.identifier not in
            [ f['identifier'] for f in new_cr_version['research_dataset']['files'] ])

    def test_fix_deprecated_directories(self):
        """
        This test performs following steps:
        1. Freeze a new file and add the parent directory of this file to a dataset
        2. Unfreeze the new file, which makes the directory to be deleted and the dataset to be deprecated
        3. When calling the fix_deprecated api, new dataset version is created with deprecated flag False
            and deleted directory is removed from research_dataset. Ensures that file count is correct.
        """
        # freeze a new file
        new_file = self._get_object_from_test_data('file', requested_index=0)
        new_file.update({
            "checksum": {
                "value": "checksumvalue",
                "algorithm": "sha2",
                "checked": "2017-05-23T10:07:22.559656Z",
            },
            "file_name": "totally_new.tiff",
            "file_path": "/world_class_experiment/totally_new.tiff",
            "project_identifier": "testirojekti",
            "identifier": "test:file:tiff",
            "file_storage": self._get_object_from_test_data('filestorage', requested_index=0)
        })
        response = self.client.post('/rest/files', new_file, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)
        file_in_dir_identifier = response.data['identifier']
        response = self.client.get('/rest/directories/%s' % response.data['parent_directory']['identifier'])
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        dir_identifier = response.data['identifier']

        # add/describe the parent directory of the newly added file to the dataset
        response = self.client.get('/rest/datasets/1')
        response.data['research_dataset']['directories'] = [
            {
                "identifier": dir_identifier,
                "title": "new dir for this dataset",
                "use_category": {
                    "identifier": "method"
                }
            }
        ]
        response = self.client.put('/rest/datasets/1', response.data, format="json")
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        cr_with_dir = self._get_next_dataset_version(response.data['identifier'])
        file_count_with_dir = CatalogRecord.objects.get(identifier=cr_with_dir['identifier']).files.count()

        # delete/unfreeze the file contained by described directory
        response = self.client.delete('/rest/files/%s' % file_in_dir_identifier)
        self.assertEqual(response.status_code, status.HTTP_200_OK)

        # fix deprecated dataset
        response = self.client.post('/rpc/datasets/fix_deprecated?identifier=%s' % cr_with_dir['identifier'])
        self.assertEqual(response.status_code, status.HTTP_204_NO_CONTENT, response.data)

        # ensure old dataset is unchanged
        response = self.client.get('/rest/datasets/%s' % cr_with_dir['identifier'])
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data['research_dataset']['directories']), 1,
            'old dataset version directories should still contain the removed directory')
        self.assertTrue(response.data['deprecated'], 'old dataset version deprecated flag should not be changed')

        # ensure the new dataset is correct
        new_cr_version = self._get_next_dataset_version(cr_with_dir['identifier'])
        file_count_wo_dir = CatalogRecord.objects.get(identifier=new_cr_version['identifier']).files.count()
        self.assertEqual(new_cr_version['deprecated'], False, 'deprecated flag should be fixed')
        self.assertEqual(file_count_wo_dir, file_count_with_dir - 1, 'new file count should be on smaller than before')
        self.assertTrue('directories' not in new_cr_version['research_dataset'])
