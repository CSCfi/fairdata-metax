# This file is part of the Metax API service
#
# Copyright 2017-2020 Ministry of Education and Culture, Finland
#
# :author: CSC - IT Center for Science Ltd., Espoo Finland <servicedesk@csc.fi>
# :license: MIT

from django.conf import settings as django_settings
from rest_framework import status
import responses

from metax_api.models import CatalogRecordV2 as CR, DataCatalog
from metax_api.tests.utils import get_test_oidc_token
from metax_api.utils import get_tz_aware_now_without_micros

from .write import (
    CatalogRecordApiWriteAssignFilesCommon,
    CatalogRecordApiWriteCommon,
)


END_USER_ALLOWED_DATA_CATALOGS = django_settings.END_USER_ALLOWED_DATA_CATALOGS


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


class CatalogRecordApiWriteAssignFilesCommonV2(CatalogRecordApiWriteAssignFilesCommon):

    """
    Note !!!

    These common helper methods inherit from V1 class CatalogRecordApiWriteAssignFilesCommon.
    """

    def _add_file(self, ds, path, with_metadata=False):

        super()._add_file(ds, path)

        if with_metadata:
            return

        files_and_dirs = self._research_dataset_or_file_changes(ds)

        # else: addition entry only, which will not be persisted. keep only identifier

        files_and_dirs['files'][-1] = { 'identifier': files_and_dirs['files'][-1]['identifier'] }

    def _exclude_file(self, ds, path):
        self._add_file(ds, path)

        files_and_dirs = self._research_dataset_or_file_changes(ds)

        files_and_dirs['files'][-1]['exclude'] = True

        assert len(files_and_dirs['files'][-1]) == 2

    def _add_directory(self, ds, path, project=None, with_metadata=False):
        super()._add_directory(ds, path)

        if with_metadata:
            return

        files_and_dirs = self._research_dataset_or_file_changes(ds)

        # else: addition entry only, which will not be persisted. keep only identifier

        files_and_dirs['directories'][-1] = { 'identifier': files_and_dirs['directories'][-1]['identifier'] }

    def _exclude_directory(self, ds, path):
        self._add_directory(ds, path)

        files_and_dirs = self._research_dataset_or_file_changes(ds)

        files_and_dirs['directories'][-1]['exclude'] = True

        assert len(files_and_dirs['directories'][-1]) == 2


class CatalogRecordFileHandling(CatalogRecordApiWriteAssignFilesCommonV2):

    def _set_token_authentication(self):
        create_end_user_catalogs()
        self.token = get_test_oidc_token()
        self.token['group_names'].append('IDA01:testproject')
        self._use_http_authorization(method='bearer', token=self.token)
        self._mock_token_validation_succeeds()

    def _create_draft(self):
        self.cr_test_data['research_dataset'].pop('files', None)
        self.cr_test_data['research_dataset'].pop('directories', None)
        response = self.client.post('/rest/v2/datasets?draft', self.cr_test_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)
        return response.data['id']

    @responses.activate
    def test_authorization(self):
        """
        When adding files to a dataset, the user must have membership of the related files' project.
        If changing files of an existing dataset, the user must have membership of the related files' project,
        in addition to being the owner of the dataset.
        """
        self._set_token_authentication()

        for with_metadata in (False, True):
            self._add_file(self.cr_test_data, '/TestExperiment/Directory_1/file_05.txt')
            self._add_file(self.cr_test_data, '/TestExperiment/Directory_1/file_06.txt', with_metadata=with_metadata)
            response = self.client.post('/rest/v2/datasets', self.cr_test_data, format="json")
            self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)

    @responses.activate
    def test_retrieve_dataset_file_projects(self):
        """
        A helper api for retrieving current file projects of a dataset. There should always be
        only 0 or 1 projects.
        """
        self._set_token_authentication()

        self._add_file(self.cr_test_data, '/TestExperiment/Directory_1/file_05.txt')
        response = self.client.post('/rest/v2/datasets', self.cr_test_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)

        cr_id = response.data['id']

        # user owns dataset
        response = self.client.get('/rest/v2/datasets/%d/projects' % cr_id, format="json")
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        self.assertEqual(len(response.data), 1, response.data)

        # user is authenticated, but does not own dataset
        response = self.client.get('/rest/v2/datasets/1/projects', format="json")
        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN, response.data)

        # anonymous user
        self.client._credentials = {}
        response = self.client.get('/rest/v2/datasets/%d/projects' % cr_id, format="json")
        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN, response.data)

    def test_include_user_metadata_parameter(self):
        """
        When retrieving datasets, by default the "user metadata", or "dataset-specific file metadata" stored
        in research_dataset.files and research_dataset.directories should not be returned.
        """
        response = self.client.get('/rest/v2/datasets/1', format="json")
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        self.assertEqual('files' in response.data['research_dataset'], False, response.data)

        response = self.client.get('/rest/v2/datasets/1?include_user_metadata', format="json")
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        self.assertEqual('files' in response.data['research_dataset'], True, response.data)

    def test_create_files_are_saved(self):
        """
        A very simple "add two individual files" test. Only entries with dataset-specific
        metadata should be persisted in research_dataset.files.
        """
        self._add_file(self.cr_test_data, '/TestExperiment/Directory_1/file_05.txt')
        self._add_file(self.cr_test_data, '/TestExperiment/Directory_1/file_06.txt', with_metadata=True)
        response = self.client.post('/rest/v2/datasets?include_user_metadata', self.cr_test_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)
        self.assertEqual(len(response.data['research_dataset']['files']), 1)
        self.assert_file_count(response.data, 2)
        self.assert_total_files_byte_size(response.data, self._single_file_byte_size * 2)

    def test_create_directories_are_saved(self):
        """
        A very simple "add two individual directories" test. Only entries with dataset-specific
        metadata should be persisted in research_dataset.directories.
        """
        self._add_directory(self.cr_test_data, '/TestExperiment/Directory_1/Group_1')
        self._add_directory(self.cr_test_data, '/TestExperiment/Directory_1/Group_2', with_metadata=True)
        response = self.client.post('/rest/v2/datasets?include_user_metadata', self.cr_test_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)
        self.assertEqual(len(response.data['research_dataset']['directories']), 1)
        self.assert_file_count(response.data, 4)
        self.assert_total_files_byte_size(response.data, self._single_file_byte_size * 4)

    def test_create_exclude_files(self):
        """
        Add directory of files, but exclude one file.
        """
        self._exclude_file(self.cr_test_data, '/TestExperiment/Directory_1/Group_1/file_01.txt')
        self._add_directory(self.cr_test_data, '/TestExperiment/Directory_1')
        response = self.client.post('/rest/v2/datasets', self.cr_test_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)
        self.assert_file_count(response.data, 5)
        self.assert_total_files_byte_size(response.data, self._single_file_byte_size * 5)

    def test_create_exclude_directories(self):
        """
        Add directory of files, but exclude one sub directory.
        """
        self._add_directory(self.cr_test_data, '/TestExperiment/Directory_1')
        self._exclude_directory(self.cr_test_data, '/TestExperiment/Directory_1/Group_1')
        response = self.client.post('/rest/v2/datasets', self.cr_test_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)
        self.assert_file_count(response.data, 4)
        self.assert_total_files_byte_size(response.data, self._single_file_byte_size * 4)

    def test_update_add_and_exclude_files(self):
        """
        Add and excldue files to an existing draft dataset.

        It should be possible to add or exclude files any number of times on a draft dataset.
        """
        cr_id = self._create_draft()

        file_changes = {}

        self._add_file(file_changes, '/TestExperiment/Directory_1/Group_1/file_01.txt')
        response = self.client.post('/rest/v2/datasets/%d/files' % cr_id, file_changes, format="json")
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        self.assertEqual(response.data.get('files_added'), 1, response.data)
        self.assert_file_count(cr_id, 1)

        # executing the same request with same file entry should make no difference
        response = self.client.post('/rest/v2/datasets/%d/files' % cr_id, file_changes, format="json")
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        self.assertEqual(response.data.get('files_added'), 0, response.data)
        self.assert_file_count(cr_id, 1)

        # adding a directory should add files that are not already added
        file_changes = {}

        self._add_directory(file_changes, '/TestExperiment/Directory_1/Group_1')
        response = self.client.post('/rest/v2/datasets/%d/files' % cr_id, file_changes, format="json")
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        self.assertEqual(response.data.get('files_added'), 1, response.data)
        self.assert_file_count(cr_id, 2)

        # add even more files
        file_changes = {}

        self._add_directory(file_changes, '/TestExperiment/Directory_1')
        response = self.client.post('/rest/v2/datasets/%d/files' % cr_id, file_changes, format="json")
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        self.assertEqual(response.data.get('files_added'), 4, response.data)
        self.assert_file_count(cr_id, 6)

        # exclude previously added files from one directroy
        file_changes = {}

        self._exclude_directory(file_changes, '/TestExperiment/Directory_1/Group_1')
        response = self.client.post('/rest/v2/datasets/%d/files' % cr_id, file_changes, format="json")
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        self.assertEqual(response.data.get('files_removed'), 2, response.data)
        self.assert_file_count(cr_id, 4)

        # exclude all previously added files, but keep one file by adding an "add file" entry
        file_changes = {}

        self._add_file(file_changes, '/TestExperiment/Directory_1/Group_2/file_03.txt')
        self._exclude_directory(file_changes, '/TestExperiment')
        response = self.client.post('/rest/v2/datasets/%d/files' % cr_id, file_changes, format="json")
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        self.assertEqual(response.data.get('files_added'), 0, response.data)
        self.assertEqual(response.data.get('files_removed'), 3, response.data)
        self.assert_file_count(cr_id, 1)

    def test_files_can_be_added_once_after_publishing(self):
        """
        First update from 0 to n files should be allowed even for a published dataset, and
        without needing to create new dataset versions, so this is permitted. Subsequent
        file changes will requiree creating a new draft version first.
        """
        response = self.client.post('/rest/v2/datasets', self.cr_test_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)

        cr_id = response.data['id']

        self.assert_file_count(cr_id, 0)

        file_changes = {}

        self._add_file(file_changes, '/TestExperiment/Directory_1/Group_1/file_02.txt')
        response = self.client.post('/rest/v2/datasets/%d/files' % cr_id, file_changes, format="json")
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        self.assertEqual(response.data.get('files_added'), 1, response.data)

        # try to add a second time. should give an error
        file_changes = {}

        self._add_file(file_changes, '/TestExperiment/Directory_1/Group_1/file_02.txt')
        response = self.client.post('/rest/v2/datasets/%d/files' % cr_id, file_changes, format="json")
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST, response.data)
        self.assertEqual('Changing files of a published dataset' in response.data['detail'][0], True, response.data)

    def test_prevent_changing_files_on_deprecated_datasets(self):
        cr = CR.objects.get(pk=1)
        cr.deprecated = True
        cr.force_save()

        file_changes = {
            'files': [{ 'identifier': 'some file' }]
        }

        response = self.client.post('/rest/v2/datasets/1/files', file_changes, format="json")
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST, response.data)
        self.assertEqual('Changing files of a deprecated' in response.data['detail'][0], True, response.data)

    def test_directory_entries_are_processed_in_order(self):
        """
        Directory entries should executed in the order they are given in the request body.
        """
        # excluding should do nothing, since "add directory" entry is later
        self._exclude_directory(self.cr_test_data, '/TestExperiment/Directory_1/Group_1')
        self._add_directory(self.cr_test_data, '/TestExperiment/Directory_1')
        response = self.client.post('/rest/v2/datasets', self.cr_test_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)
        self.assert_file_count(response.data, 6)

        self.cr_test_data['research_dataset'].pop('directories')

        # exclusion should now have effect, since it is last
        self._add_directory(self.cr_test_data, '/TestExperiment/Directory_1')
        self._exclude_directory(self.cr_test_data, '/TestExperiment/Directory_1/Group_1')
        response = self.client.post('/rest/v2/datasets', self.cr_test_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)
        self.assert_file_count(response.data, 4)

    def test_allow_file_changes_only_on_drafts(self):
        self._add_file(self.cr_test_data, '/TestExperiment/Directory_1/Group_1/file_01.txt')
        response = self.client.post('/rest/v2/datasets', self.cr_test_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)

        file_changes = {}
        self._add_file(file_changes, '/TestExperiment/Directory_1/Group_1/file_02.txt')
        response = self.client.post('/rest/v2/datasets/%d/files' % response.data['id'], file_changes, format="json")
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST, response.data)
        self.assertEqual('Changing files of a published dataset' in response.data['detail'][0], True, response.data)


class CatalogRecordDatasetSpecificFileMetadata(CatalogRecordApiWriteAssignFilesCommonV2):

    def test_retrieve_file_metadata_only(self):

        cr_id = 11

        # retrieve all "user metadata" of adataset
        response = self.client.get('/rest/v2/datasets/%d/files/user_metadata' % cr_id, format="json")
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)

        cr = CR.objects.only('research_dataset').get(pk=cr_id)

        for object_type in ('files', 'directories'):
            self.assertEqual(
                len(cr.research_dataset[object_type]),
                len(response.data[object_type]),
                response.data
            )

        files_and_dirs = response.data

        params = [
            {
                # a pid of single file
                'pid': 'pid:urn:2',
                'directory': 'false',
                'expect_to_find': True,
            },
            {
                # a pid of a directory
                'pid': 'pid:urn:dir:2',
                'directory': 'true',
                'expect_to_find': True,
            },
            {
                # a pid of a directory not part of this dataset
                'pid': 'should not be found',
                'directory': 'true',
                'expect_to_find': False,
            }
        ]

        for p in params:

            # retrieve a single metadata entry
            response = self.client.get(
                '/rest/v2/datasets/%d/files/%s/user_metadata?directory=%s' % (cr_id, p['pid'], p['directory']),
                format="json"
            )
            if p['expect_to_find'] is True:
                self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
                self.assertEqual('identifier' in response.data, True, response.data)
            else:
                self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND, response.data)
                continue

            if p['directory'] == 'true':
                object_type = 'directories'
            else:
                object_type = 'files'

            for obj in files_and_dirs[object_type]:
                if obj['identifier'] == response.data['identifier']:
                    if p['expect_to_find'] is True:
                        self.assertEqual(obj, response.data, response.data)
                        break
                    else:
                        self.fail('pid %s should not have been found' % p['pid'])
            else:
                if p['expect_to_find'] is True:
                    self.fail('Retrieved object %s was not found in research_dataset file data?' % p['pid'])

    def test_dataset_files_schema(self):
        """
        Ensure new schema file dataset_files_schema.json is used.
        """
        response = self.client.post('/rest/v2/datasets', self.cr_test_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)

        cr_id = response.data['id']

        # use a non-schema field
        file_changes = {}
        self._add_file(file_changes, '/TestExperiment/Directory_1/Group_1/file_02.txt', with_metadata=True)
        file_changes['files'][0]['some_unexpected_file'] = 'should raise error'

        response = self.client.post('/rest/v2/datasets/%d/files' % cr_id, file_changes, format="json")
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST, response.data)
        self.assertEqual('is not valid' in str(response.data['detail'][0]), True, response.data)

        # various mandatory fields missing
        for mandatory_field in ('identifier', 'use_category'):
            file_changes = {}
            self._add_file(file_changes, '/TestExperiment/Directory_1/Group_1/file_02.txt', with_metadata=True)
            file_changes['files'][0].pop(mandatory_field)

            response = self.client.post('/rest/v2/datasets/%d/files' % cr_id, file_changes, format="json")
            self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST, response.data)
            self.assertEqual('is not valid' in str(response.data['detail'][0]), True, response.data)

    def test_update_metadata_only(self):
        self._add_file(self.cr_test_data, '/TestExperiment/Directory_1/Group_1/file_02.txt')
        response = self.client.post('/rest/v2/datasets', self.cr_test_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)
        self.assertEqual('files' in response.data['research_dataset'], False, response.data)

        cr_id = response.data['id']

        # add metadata for one file

        file_changes = {}
        self._add_file(file_changes, '/TestExperiment/Directory_1/Group_1/file_02.txt', with_metadata=True)
        file_changes['files'][0]['title'] = 'New title'

        response = self.client.put('/rest/v2/datasets/%d/files/user_metadata' % cr_id, file_changes, format="json")
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        self.assertEqual(
            CR.objects.only('research_dataset').get(pk=cr_id).research_dataset['files'][0]['title'],
            file_changes['files'][0]['title']
        )

        # add metadata for two directories

        file_changes = {}
        self._add_directory(file_changes, '/TestExperiment', with_metadata=True)
        self._add_directory(file_changes, '/TestExperiment/Directory_1', with_metadata=True)
        file_changes['directories'][0]['title']       = 'New dir title'
        file_changes['directories'][0]['description'] = 'New dir description'
        file_changes['directories'][1]['title']       = 'New dir title 2'
        file_changes['directories'][1]['description'] = 'New dir description 2'

        file_count_before = CR.objects.get(pk=cr_id).files.count()

        response = self.client.put('/rest/v2/datasets/%d/files/user_metadata' % cr_id, file_changes, format="json")
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)

        cr = CR.objects.only('research_dataset').get(pk=cr_id)

        self.assertEqual(
            cr.files.count(), file_count_before, 'operation should only update metadata, but not add files'
        )

        for index in (0, 1):
            for field in ('title', 'description'):
                self.assertEqual(
                    cr.research_dataset['directories'][index][field],
                    file_changes['directories'][index][field]
                )

        # update only one field using patch

        file_changes = {}
        self._add_directory(file_changes, '/TestExperiment', with_metadata=True)
        file_changes['directories'][0] = {
            'identifier': file_changes['directories'][0]['identifier'],
            'title': 'Changed dir title'
        }

        response = self.client.patch('/rest/v2/datasets/%d/files/user_metadata' % cr_id, file_changes, format="json")
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        self.assertEqual(
            CR.objects.only('research_dataset').get(pk=cr_id).research_dataset['directories'][0]['title'],
            file_changes['directories'][0]['title']
        )

        # remove metadata entry. it should be ok that there are normal metadata-addition entries included
        # in the request body too.

        file_changes = {}
        self._add_directory(file_changes, '/TestExperiment/Directory_1/Group_1', with_metadata=True)
        self._add_directory(file_changes, '/TestExperiment')
        file_changes['directories'][-1]['delete'] = True

        entry_to_delete = file_changes['directories'][-1]['identifier']

        response = self.client.put('/rest/v2/datasets/%d/files/user_metadata' % cr_id, file_changes, format="json")
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)

        cr = CR.objects.only('research_dataset').get(pk=cr_id)

        self.assertEqual(
            entry_to_delete in [ dr['identifier'] for dr in cr.research_dataset['directories'] ],
            False
        )

        # dont allow adding metadata entries for files that are not actually included in the dataset
        file_changes = {}
        self._add_file(file_changes, '/TestExperiment/Directory_1/Group_2/file_03.txt', with_metadata=True)
        non_existing_file = file_changes['files'][-1]['identifier']

        response = self.client.put('/rest/v2/datasets/%d/files/user_metadata' % cr_id, file_changes, format="json")
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST, response.data)
        self.assertEqual('are not included' in response.data['detail'][0], True, response.data)
        self.assertEqual(non_existing_file in response.data['data'], True, response.data)


class CatalogRecordFileHandlingCumulativeDatasets(CatalogRecordApiWriteAssignFilesCommonV2):

    """
    Cumulative datasets should allow adding new files to a published dataset,
    but prevent removing files.
    """

    def setUp(self):
        super().setUp()
        self.cr_test_data.pop('files', None)
        self.cr_test_data.pop('directories', None)
        self.cr_test_data['cumulative_state'] = CR.CUMULATIVE_STATE_YES
        self._add_file(self.cr_test_data, '/TestExperiment/Directory_1/file_05.txt')

        response = self.client.post('/rest/v2/datasets', self.cr_test_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)
        self.cr_id = response.data['id']

    def test_add_files_to_cumulative_dataset(self):
        """
        Adding files to an existing cumulative dataset should be ok.
        """
        file_data = {}
        self._add_file(file_data, '/TestExperiment/Directory_1/file_06.txt')

        response = self.client.post('/rest/v2/datasets/%d/files' % self.cr_id, file_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        cr = CR.objects.get(pk=self.cr_id)
        self.assertEqual(cr.files.count(), 2)
        self.assertEqual(cr.date_last_cumulative_addition, cr.date_modified)

    def test_exclude_files_to_cumulative_dataset(self):
        """
        Excluding files from an existing cumulative dataset should be prevented.
        """
        file_data = {}
        self._exclude_file(file_data, '/TestExperiment/Directory_1/file_05.txt')

        response = self.client.post('/rest/v2/datasets/%d/files' % self.cr_id, file_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST, response.data)
        self.assertEqual('Excluding files from a cumulative' in response.data['detail'][0], True, response.data)
        self.assertEqual(CR.objects.get(pk=self.cr_id).files.count(), 1)


class CatalogRecordPublishing(CatalogRecordApiWriteCommon):

    """
    todo move to rpc api v2 tests directory
    """

    def test_publish_draft(self):

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


class CatalogRecordVersionHandling(CatalogRecordApiWriteCommon):

    """
    New dataset versions are not created automatically when changing files of a dataset.
    New dataset versions can only be created by explicitly calling related RPC API.

    todo move to rpc api v2 tests directory
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
        original_cr = CR.objects.get(pk=1)

        response = self.client.delete('/rest/v2/files/1', format="json")
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)

        response = self.client.post('/rpc/v2/datasets/create_new_version?identifier=1', format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)

        new_cr = CR.objects.get(pk=response.data['id'])
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
        cr = CR.objects.get(pk=1)
        cr.metadata_provider_user = self.token['CSCUserName']
        cr.editor = None
        cr.force_save()

        response = self.client.post('/rpc/v2/datasets/create_new_version?identifier=1', format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)
