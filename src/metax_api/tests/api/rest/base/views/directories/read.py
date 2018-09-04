# This file is part of the Metax API service
#
# Copyright 2017-2018 Ministry of Education and Culture, Finland
#
# :author: CSC - IT Center for Science Ltd., Espoo Finland <servicedesk@csc.fi>
# :license: MIT

from django.db.models import Sum
from django.core.management import call_command
from rest_framework import status
from rest_framework.test import APITestCase
import responses

from metax_api.models import CatalogRecord, Directory
from metax_api.tests.utils import get_test_oidc_token, test_data_file_path, TestClassUtils


class DirectoryApiReadCommon(APITestCase, TestClassUtils):
    @classmethod
    def setUpClass(cls):
        """
        Loaded only once for test cases inside this class.
        """
        call_command('loaddata', test_data_file_path, verbosity=0)
        super(DirectoryApiReadCommon, cls).setUpClass()

    def setUp(self):
        dir_from_test_data = self._get_object_from_test_data('directory')
        self.identifier = dir_from_test_data['identifier']
        self.pk = dir_from_test_data['id']
        self._use_http_authorization()


class DirectoryApiReadBasicTests(DirectoryApiReadCommon):
    def test_read_directory_list(self):
        response = self.client.get('/rest/directories')
        self.assertEqual(response.status_code, 501)

    def test_read_directory_details_by_pk(self):
        response = self.client.get('/rest/directories/%s' % self.pk)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(hasattr(response, 'data'), True, 'Request response object is missing attribute \'data\'')
        self.assertEqual('directory_name' in response.data.keys(), True)
        self.assertEqual(response.data['identifier'], self.identifier)

    def test_read_directory_details_by_identifier(self):
        response = self.client.get('/rest/directories/%s' % self.identifier)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(hasattr(response, 'data'), True, 'Request response object is missing attribute \'data\'')
        self.assertEqual('directory_name' in response.data.keys(), True)
        self.assertEqual(response.data['identifier'], self.identifier)

    def test_read_directory_details_not_found(self):
        response = self.client.get('/rest/directories/shouldnotexist')
        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND)


class DirectoryApiReadFileBrowsingTests(DirectoryApiReadCommon):

    """
    Test generic file browsing, should always return all existing files in a dir.
    """

    def test_read_directory_get_files(self):
        """
        Test browsing files
        """
        response = self.client.get('/rest/directories/2/files')
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data['directories']), 1)
        self.assertEqual(response.data['directories'][0]['id'], 3)
        self.assertEqual(response.data['directories'][0]['parent_directory']['id'], 2)
        self.assertEqual(len(response.data['files']), 0)

        response = self.client.get('/rest/directories/3/files')
        self.assertEqual(len(response.data['directories']), 1)
        self.assertEqual(response.data['directories'][0]['id'], 4)
        self.assertEqual(response.data['directories'][0]['parent_directory']['id'], 3)
        self.assertEqual(len(response.data['files']), 5)
        self.assertEqual(response.data['files'][0]['parent_directory']['id'], 3)
        self.assertEqual(response.data['files'][4]['parent_directory']['id'], 3)

        response = self.client.get('/rest/directories/4/files')
        self.assertEqual(len(response.data['directories']), 1)
        self.assertEqual(response.data['directories'][0]['parent_directory']['id'], 4)
        self.assertEqual(len(response.data['files']), 5)
        self.assertEqual(response.data['files'][0]['parent_directory']['id'], 4)
        self.assertEqual(response.data['files'][4]['parent_directory']['id'], 4)

        response = self.client.get('/rest/directories/5/files')
        self.assertEqual(len(response.data['directories']), 1)
        self.assertEqual(len(response.data['files']), 0)

        response = self.client.get('/rest/directories/6/files')
        self.assertEqual(len(response.data['directories']), 0)
        self.assertEqual(len(response.data['files']), 10)
        self.assertEqual(response.data['files'][0]['parent_directory']['id'], 6)
        self.assertEqual(response.data['files'][9]['parent_directory']['id'], 6)

    def test_read_directory_get_files_recursively(self):
        """
        Test query parameter 'recursive'.
        """

        # without depth, returns from depth=1, which should contain no files
        response = self.client.get('/rest/directories/1/files?recursive')
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data), 0)

        # dir id 1 (the root) contains 0 files, but recursively 20
        response = self.client.get('/rest/directories/1/files?recursive=true&depth=*')
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data), 20)

        # dir id 3 contains 5 files, but recursively 20
        response = self.client.get('/rest/directories/3/files?recursive=true&depth=*')
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data), 20)

        # dir id 4 contains 5 files, but recursively 15
        response = self.client.get('/rest/directories/4/files?recursive=true&depth=*')
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data), 15)

        # dir id 5 contains 0 files
        response = self.client.get('/rest/directories/5/files?recursive=true&depth=*')
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data), 10)

        # dir id 6 contains 10 files
        response = self.client.get('/rest/directories/6/files?recursive=true&depth=*')
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data), 10)

    def test_read_directory_get_files_file_not_found(self):
        response = self.client.get('/rest/directories/not_found/files')
        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND)

    def test_read_directory_get_project_root_directory(self):
        response = self.client.get('/rest/directories/root?project=project_x')
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data['id'], 1)
        self.assertEqual('directories' in response.data, True)
        self.assertEqual('files' in response.data, True)
        self.assertEqual(len(response.data['directories']), 1)
        self.assertEqual(response.data['directories'][0]['id'], 2)

    def test_read_directory_get_project_root_directory_not_found(self):
        response = self.client.get('/rest/directories/root?project=project_xyz')
        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND)

    def test_read_directory_get_project_root_directory_parameter_missing(self):
        response = self.client.get('/rest/directories/root')
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual('required' in response.data['detail'], True, response.data)

    def test_read_directory_get_files_by_path(self):
        dr = Directory.objects.get(pk=2)
        response = self.client.get('/rest/directories/files?path=%s&project=%s' %
            (dr.directory_path, dr.project_identifier))
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data['directories']), 1)
        self.assertEqual(response.data['directories'][0]['id'], 3)
        self.assertEqual(response.data['directories'][0]['parent_directory']['id'], 2)
        self.assertEqual(len(response.data['files']), 0)

    def test_read_directory_get_files_by_path_not_found(self):
        response = self.client.get('/rest/directories/files?path=%s&project=%s' %
            ('doesnotexist', 'doesnotexist'))
        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND)

    def test_read_directory_get_files_by_path_check_parameters(self):
        response = self.client.get('/rest/directories/files')
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        response = self.client.get('/rest/directories/files?path=something')
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        response = self.client.get('/rest/directories/files?project=something')
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)

    def test_read_directory_recursively_with_max_depth(self):
        """
        Should return a flat list of files, three directories deep
        """
        response = self.client.get('/rest/directories/2/files?recursive=true&depth=3')
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data), 10)

    def test_read_directory_recursively_with_dirs_only_and_max_depth(self):
        """
        Should return a directory hierarchy, three directories deep, with no files at all.
        """
        response = self.client.get('/rest/directories/2/files?recursive=true&directories_only=true&depth=3')
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual('directories' in response.data, True)
        self.assertEqual('directories' in response.data['directories'][0], True)
        self.assertEqual('directories' in response.data['directories'][0]['directories'][0], True)

    def test_read_directory_recursively_with_no_depth(self):
        """
        recursive=true with no depth specified should not return everything, but instead depth=1
        by default.

        Using parameter directories_only=true to easier count the depth.
        """
        response = self.client.get('/rest/directories/3/files?recursive=true&directories_only=true')
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual('directories' in response.data, True)
        self.assertEqual('directories' in response.data['directories'][0], True)

    def test_read_directory_return_directories_only(self):
        response = self.client.get('/rest/directories/3/files?directories_only')
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data['directories']), 1)
        self.assertEqual('files' in response.data, False)

    def test_read_directory_with_include_parent(self):
        response = self.client.get('/rest/directories/3/files?include_parent')
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data['directories']), 1)
        self.assertEqual(len(response.data['files']), 5)
        self.assertEqual(response.data.get('id', None), 3)


class DirectoryApiReadFileBrowsingRetrieveSpecificFieldsTests(DirectoryApiReadCommon):

    def test_retrieve_requested_directory_fields_only(self):
        response = self.client.get('/rest/directories/3/files?directory_fields=identifier,directory_path')
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data['directories'][0].keys()), 2)
        self.assertEqual('identifier' in response.data['directories'][0], True)
        self.assertEqual('directory_path' in response.data['directories'][0], True)

    def test_retrieve_directory_byte_size_and_file_count(self):
        """
        There is some additional logic involved in retrieving byte_size and file_count, which warrants
        targeted tests for just those fields.
        """
        response = self.client.get('/rest/directories/3/files?directory_fields=identifier,byte_size')
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data['directories'][0].keys()), 2)
        self.assertEqual('identifier' in response.data['directories'][0], True)
        self.assertEqual('byte_size' in response.data['directories'][0], True)

        response = self.client.get('/rest/directories/3/files?directory_fields=identifier,file_count')
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data['directories'][0].keys()), 2)
        self.assertEqual('identifier' in response.data['directories'][0], True)
        self.assertEqual('file_count' in response.data['directories'][0], True)

    def test_retrieve_requested_file_fields_only(self):
        response = self.client.get('/rest/directories/3/files?file_fields=identifier,file_path')
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data['files'][0].keys()), 2)
        self.assertEqual('identifier' in response.data['files'][0], True)
        self.assertEqual('file_path' in response.data['files'][0], True)

    def test_retrieve_requested_file_and_directory_fields_only(self):
        response = self.client.get('/rest/directories/3/files?file_fields=identifier&directory_fields=id')
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data['files'][0].keys()), 1)
        self.assertEqual('identifier' in response.data['files'][0], True)
        self.assertEqual(len(response.data['directories'][0].keys()), 1)
        self.assertEqual('id' in response.data['directories'][0], True)


class DirectoryApiReadCatalogRecordFileBrowsingTests(DirectoryApiReadCommon):

    """
    Test browsing files in the context of a specific CatalogRecord. Should always
    only dispaly those files that were selected for that CR, and only those dirs,
    that contained suchs files, or would contain such files further down the tree.
    """

    def test_read_directory_for_catalog_record(self):
        """
        Test query parameter 'cr_identifier'.
        """
        response = self.client.get('/rest/directories/3/files?cr_identifier=%s'
            % CatalogRecord.objects.get(pk=1).identifier)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual('directories' in response.data, True)
        self.assertEqual('files' in response.data, True)
        self.assertEqual(len(response.data['directories']), 0)
        self.assertEqual(len(response.data['files']), 2)

    def test_read_directory_for_catalog_record_not_found(self):
        """
        Not found cr_identifier should raise 400 instead of 404, which is raised when the
        directory itself is not found. the error contains details about the 400.
        """
        response = self.client.get('/rest/directories/3/files?cr_identifier=notexisting')
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)

    def test_read_directory_for_catalog_record_directory_does_not_exist(self):
        """
        A directory may have files in a project, but those files did not necessarily exist
        or were not selected for a specific CR.
        """

        # should be OK...
        response = self.client.get('/rest/directories/4/files')
        self.assertEqual(response.status_code, status.HTTP_200_OK)

        # ... but should not contain any files FOR THIS CR
        response = self.client.get('/rest/directories/4/files?cr_identifier=%s'
            % CatalogRecord.objects.get(pk=1).identifier)
        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND)

    def test_read_directory_for_catalog_record_recursively(self):
        """
        Test query parameter 'cr_identifier' with 'recursive'.
        """
        response = self.client.get('/rest/directories/1/files?recursive&cr_identifier=%s&depth=*'
            % CatalogRecord.objects.get(pk=1).identifier)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data), 2)

        # not found cr_identifier should raise 400 instead of 404, which is raised when the
        # directory itself is not found. the error contains details about the 400
        response = self.client.get('/rest/directories/1/files?recursive&cr_identifier=notexisting')
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)

    def test_directory_byte_size_and_file_count(self):
        """
        Test byte size and file count are calculated correctly for directories when browsing files
        in the context of a single record.
        """
        def _assert_dir_calculations(cr, dr):
            """
            Assert directory numbers received from browsing-api matches what exists in the db when
            making a reasonably fool-proof query of files by directory path
            """
            self.assertEqual('byte_size' in dr, True)
            self.assertEqual('file_count' in dr, True)

            byte_size = cr.files.filter(file_path__startswith='%s/' % dr['directory_path']) \
                .aggregate(Sum('byte_size'))['byte_size__sum']

            file_count = cr.files.filter(file_path__startswith='%s/' % dr['directory_path']).count()

            self.assertEqual(dr['byte_size'], byte_size, 'path: %s' % dr['directory_path'])
            self.assertEqual(dr['file_count'], file_count, 'path: %s' % dr['directory_path'])

        # prepare a new test dataset which contains a directory from testdata, which contains a decent
        # qty of files and complexity
        dr = Directory.objects.get(directory_path='/prj_112_root')
        cr_with_dirs = CatalogRecord.objects.get(pk=13)
        cr_data = response = self.client.get('/rest/datasets/1').data
        cr_data.pop('id')
        cr_data.pop('identifier')
        cr_data['research_dataset'].pop('preferred_identifier')
        cr_data['research_dataset']['directories'] = [{
            'identifier': dr.identifier,
            'title': 'test dir',
            'use_category': {
                'identifier': cr_with_dirs.research_dataset['directories'][0]['use_category']['identifier']
            }
        }]
        self._use_http_authorization(username='metax')
        cr_data = response = self.client.post('/rest/datasets', cr_data, format='json').data
        cr = CatalogRecord.objects.get(pk=cr_data['id'])

        # begin tests

        # test: browse the file api, and receive a list of sub-directories
        response = self.client.get('/rest/directories/%d/files?cr_identifier=%s' % (dr.id, cr.identifier))
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)

        for directory in response.data['directories']:
            _assert_dir_calculations(cr, directory)

        # test: browse with ?include_parent=true to get the dir directly that was added to the dataset
        response = self.client.get('/rest/directories/%d/files?cr_identifier=%s&include_parent'
            % (dr.id, cr.identifier))
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        _assert_dir_calculations(cr, response.data)


class DirectoryApiReadCatalogRecordFileBrowsingRetrieveSpecificFieldsTests(DirectoryApiReadCommon):

    def setUp(self):
        super().setUp()
        CatalogRecord.objects.get(pk=12).calculate_directory_byte_sizes_and_file_counts()

    def test_retrieve_requested_directory_fields_only(self):
        response = self.client.get('/rest/datasets/12?file_details&directory_fields=identifier,directory_path')
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data['research_dataset']['directories'][0]['details'].keys()), 2)
        self.assertEqual('identifier' in response.data['research_dataset']['directories'][0]['details'], True)
        self.assertEqual('directory_path' in response.data['research_dataset']['directories'][0]['details'], True)

    def test_retrieve_directory_byte_size_and_file_count(self):
        """
        There is some additional logic involved in retrieving byte_size and file_count, which warrants
        targeted tests for just those fields.
        """
        response = self.client.get('/rest/datasets/12?file_details&directory_fields=identifier,byte_size')
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data['research_dataset']['directories'][0]['details'].keys()), 2)
        self.assertEqual('identifier' in response.data['research_dataset']['directories'][0]['details'], True)
        self.assertEqual('byte_size' in response.data['research_dataset']['directories'][0]['details'], True)

        response = self.client.get('/rest/datasets/12?file_details&directory_fields=identifier,file_count')
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data['research_dataset']['directories'][0]['details'].keys()), 2)
        self.assertEqual('identifier' in response.data['research_dataset']['directories'][0]['details'], True)
        self.assertEqual('file_count' in response.data['research_dataset']['directories'][0]['details'], True)

    def test_retrieve_requested_file_fields_only(self):
        response = self.client.get('/rest/datasets/12?file_details&file_fields=identifier,file_path')
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data['research_dataset']['files'][0]['details'].keys()), 2)
        self.assertEqual('identifier' in response.data['research_dataset']['files'][0]['details'], True)
        self.assertEqual('file_path' in response.data['research_dataset']['files'][0]['details'], True)

    def test_retrieve_requested_file_and_directory_fields_only(self):
        response = self.client.get('/rest/datasets/12?file_details&file_fields=identifier&directory_fields=id')
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data['research_dataset']['files'][0]['details'].keys()), 1)
        self.assertEqual('identifier' in response.data['research_dataset']['files'][0]['details'], True)
        self.assertEqual(len(response.data['research_dataset']['directories'][0]['details'].keys()), 1)
        self.assertEqual('id' in response.data['research_dataset']['directories'][0]['details'], True)


class DirectoryApiReadEndUserAccess(DirectoryApiReadCommon):

    '''
    Test End User Access permissions when browsing files using /rest/directories api.

    Note: In these tests, the token by default does not have correct project groups.
    Token project groups are only made valid by calling _update_token_with_project_of_directory().
    '''

    def setUp(self):
        super().setUp()
        self.token = get_test_oidc_token()
        self._mock_token_validation_succeeds()

    def _update_token_with_project_of_directory(self, dir_id):
        proj = Directory.objects.get(pk=dir_id).project_identifier
        self.token['group_names'].append('fairdata:IDA01:%s' % proj)
        self._use_http_authorization(method='bearer', token=self.token)

    @responses.activate
    def test_user_can_browse_files_from_their_projects(self):
        '''
        Ensure users can only read files from /rest/directories owned by them.
        '''
        self._use_http_authorization(method='bearer', token=self.token)

        # first read files without project access - should fail
        response = self.client.get('/rest/directories/1')
        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN)
        response = self.client.get('/rest/directories/1/files')
        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN, response.data)

        # set user to same project as previous files and try again. should now succeed
        self._update_token_with_project_of_directory(1)

        response = self.client.get('/rest/directories/1')
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        response = self.client.get('/rest/directories/1/files')
        self.assertEqual(response.status_code, status.HTTP_200_OK)

    @responses.activate
    def test_browsing_by_project_and_file_path_is_protected(self):
        self._use_http_authorization(method='bearer', token=self.token)

        dr = Directory.objects.get(pk=2)
        response = self.client.get('/rest/directories/files?path=%s&project=%s' %
            (dr.directory_path, dr.project_identifier))
        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN)

        self._update_token_with_project_of_directory(2)

        response = self.client.get('/rest/directories/files?path=%s&project=%s' %
            (dr.directory_path, dr.project_identifier))
        self.assertEqual(response.status_code, status.HTTP_200_OK)

    @responses.activate
    def test_browsing_in_cr_context_does_not_check_permissions(self):
        '''
        Browsing files in a cr context should not check project membership, since
        those files are then already public.
        '''
        self._use_http_authorization(method='bearer', token=self.token)
        response = self.client.get('/rest/directories/3/files?cr_identifier=%s'
            % CatalogRecord.objects.get(pk=1).identifier)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
