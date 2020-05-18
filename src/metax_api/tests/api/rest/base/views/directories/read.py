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
from django.db import transaction

from metax_api.models import CatalogRecord, Directory, File
from metax_api.models.catalog_record import ACCESS_TYPES
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

    def _create_test_dirs(self, count):
        count = count + 1
        with transaction.atomic():
            for n in range(1, count):
                f = self._get_new_file_data(str(n))
                self.client.post('/rest/files', f, format="json")

    def _get_dirs_files_ids(self, url):
        file_data = self.client.get(url).data
        if isinstance(file_data, dict):
            return {key: [f['id'] for f in file_data[key]] for key in file_data.keys()
                if key in ['directories', 'files']}
        else:
            return [f['id'] for f in file_data]

    def _get_new_file_data(self, file_n):
        from_test_data = self._get_object_from_test_data('file', requested_index=0)

        path = '/prj_112_root/science_data_C/phase_2/2017/10/dir' + file_n + '/file_' + file_n
        identifier = 'urn:nbn:fi:100' + file_n

        from_test_data.update({
            "checksum": {
                "value": "habeebit",
                "algorithm": "SHA-256",
                "checked": "2017-05-23T10:07:22.559656Z",
            },
            "file_name": "tiedosto_name_" + file_n,
            "file_path": path,
            "identifier": identifier,
            "file_storage": self._get_object_from_test_data('filestorage', requested_index=0),
            'parent_directory': 24,
            'project_identifier': 'research_project_112'
        })
        del from_test_data['id']
        return from_test_data

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
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
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
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.content)
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
        self.assertEqual('required' in response.data['detail'][0], True, response.data)

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

    def test_read_directory_files_sorted_by_file_path(self):
        response = self.client.get('/rest/directories/3/files')
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data['files'][0]['file_path'], '/project_x_FROZEN/Experiment_X/file_name_1')
        self.assertEqual(response.data['files'][1]['file_path'], '/project_x_FROZEN/Experiment_X/file_name_2')
        self.assertEqual(response.data['files'][2]['file_path'], '/project_x_FROZEN/Experiment_X/file_name_3')

        response = self.client.get('/rest/directories/3/files?pagination&limit=2&offset=2')
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data['results']['files'][0]['file_path'],
            '/project_x_FROZEN/Experiment_X/file_name_2')
        self.assertEqual(response.data['results']['files'][1]['file_path'],
            '/project_x_FROZEN/Experiment_X/file_name_3')

        response = self.client.get('/rest/directories/3/files?cr_identifier=2')
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data['files'][0]['file_path'], '/project_x_FROZEN/Experiment_X/file_name_3')
        self.assertEqual(response.data['files'][1]['file_path'], '/project_x_FROZEN/Experiment_X/file_name_4')

        response = self.client.get('/rest/directories/3/files?recursive')
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data[0]['file_path'], '/project_x_FROZEN/Experiment_X/file_name_1')
        self.assertEqual(response.data[1]['file_path'], '/project_x_FROZEN/Experiment_X/file_name_2')
        self.assertEqual(response.data[2]['file_path'], '/project_x_FROZEN/Experiment_X/file_name_3')

        response = self.client.get('/rest/directories/3/files?recursive&cr_identifier=2')
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data[0]['file_path'], '/project_x_FROZEN/Experiment_X/file_name_3')
        self.assertEqual(response.data[1]['file_path'], '/project_x_FROZEN/Experiment_X/file_name_4')

    def test_read_directory_directories_sorted_by_directory_path(self):

        response = self.client.get('/rest/directories/8/files')
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data['directories'][0]['directory_path'],
            '/prj_112_root/other')
        self.assertEqual(response.data['directories'][1]['directory_path'],
            '/prj_112_root/random_folder', response.data)
        self.assertEqual(response.data['directories'][2]['directory_path'],
            '/prj_112_root/science_data_A')
        self.assertEqual(response.data['directories'][3]['directory_path'],
            '/prj_112_root/science_data_B')
        self.assertEqual(response.data['directories'][4]['directory_path'],
            '/prj_112_root/science_data_C')

        response = self.client.get('/rest/directories/8/files?pagination&limit=2&offset=2')
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data['results']['directories'][0]['directory_path'],
            '/prj_112_root/science_data_A')
        self.assertEqual(response.data['results']['directories'][1]['directory_path'],
            '/prj_112_root/science_data_B')

        response = self.client.get('/rest/directories/8/files?directories_only')
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data['directories'][0]['directory_path'],
            '/prj_112_root/other')
        self.assertEqual(response.data['directories'][1]['directory_path'],
            '/prj_112_root/random_folder', response.data)
        self.assertEqual(response.data['directories'][2]['directory_path'],
            '/prj_112_root/science_data_A')
        self.assertEqual(response.data['directories'][3]['directory_path'],
            '/prj_112_root/science_data_B')
        self.assertEqual(response.data['directories'][4]['directory_path'],
            '/prj_112_root/science_data_C')

        response = self.client.get('/rest/directories/8/files?cr_identifier=13')
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        self.assertEqual(response.data['directories'][0]['directory_path'],
            '/prj_112_root/other')
        self.assertEqual(response.data['directories'][1]['directory_path'],
            '/prj_112_root/science_data_A')
        self.assertEqual(response.data['directories'][2]['directory_path'],
            '/prj_112_root/science_data_B')
        self.assertEqual(response.data['directories'][3]['directory_path'],
            '/prj_112_root/science_data_C')


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
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
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
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        self.assertEqual(len(response.data['files'][0].keys()), 1)
        self.assertEqual('identifier' in response.data['files'][0], True)
        self.assertEqual(len(response.data['directories'][0].keys()), 1)
        self.assertEqual('id' in response.data['directories'][0], True)

    def test_not_retrieving_not_allowed_directory_fields(self):
        from metax_api.api.rest.base.serializers import DirectorySerializer, FileSerializer

        allowed_dir_fields = set(DirectorySerializer.Meta.fields)
        allowed_file_fields = set(FileSerializer.Meta.fields)

        response = self.client.get('/rest/directories/3/files?file_fields=parent,id&directory_fields=;;drop db;')

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertTrue(any(field in response.data['files'][0].keys() for field in allowed_file_fields))
        self.assertTrue(any(field in response.data['directories'][0].keys() for field in allowed_dir_fields))

        response = self.client.get('/rest/directories/3/files?file_fields=parent&directory_fields=or')
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)

        response = self.client.get('/rest/directories/3/files?file_fields=parent')
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)

        response = self.client.get('/rest/directories/3/files?directory_fields=or')
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)

class DirectoryApiReadCatalogRecordFileBrowsingTests(DirectoryApiReadCommon):

    """
    Test browsing files in the context of a specific CatalogRecord. Should always
    only dispaly those files that were selected for that CR, and only those dirs,
    that contained suchs files, or would contain such files further down the tree.
    """

    def test_read_directory_catalog_record_and_not_catalog_record_not_ok(self):
        """
        Test query parameter 'cr_identifier' and 'not_cr_identifier' can not be queried together.
        """
        response = self.client.get('/rest/directories/3/files?cr_identifier=1&not_cr_identifier=2')
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertTrue("one query parameter of 'cr_identifier' and 'not_cr_identifier'" in response.data['detail'][0])

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
        for f in response.data['files']:
            self.assertTrue(f['parent_directory']['id'], 1)

    def test_read_directory_for_not_catalog_record(self):
        """
        Test query parameter 'not_cr_identifier'.
        """
        response = self.client.get('/rest/directories/3/files?not_cr_identifier=2')
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        self.assertEqual(len(response.data['files']), 3, response.data)
        for f in response.data['files']:
            self.assertNotEqual(f['parent_directory']['id'], 2, response.data)
        self.assertEqual(len(response.data['directories']), 1, response.data)
        self.assertNotEqual(response.data['directories'][0]['parent_directory']['id'], 2)

    def test_read_directory_for_catalog_record_not_found(self):
        """
        Not found cr_identifier should raise 400 instead of 404, which is raised when the
        directory itself is not found. the error contains details about the 400.
        """
        response = self.client.get('/rest/directories/3/files?cr_identifier=notexisting')
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)

    def test_read_directory_for_not_catalog_record_not_found(self):
        """
        Not found cr_identifier should raise 400 instead of 404, which is raised when the
        directory itself is not found. the error contains details about the 400.
        """
        response = self.client.get('/rest/directories/3/files?not_cr_identifier=notexisting')
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)

    def test_read_directory_for_catalog_record_directory_does_not_exist(self):
        """
        A directory may have files in a project, but those files did not necessarily exist
        or were not selected for a specific CR.
        """

        # should be OK...
        response = self.client.get('/rest/directories/4/files')
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data['directories']), 1)
        self.assertEqual(len(response.data['files']), 5)

        # ... but should not contain any files FOR THIS CR
        response = self.client.get('/rest/directories/4/files?cr_identifier=%s'
            % CatalogRecord.objects.get(pk=1).identifier)
        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND)

        # ... and should contain files ALL BUT THIS CR
        response = self.client.get('/rest/directories/4/files?not_cr_identifier=%s'
            % CatalogRecord.objects.get(pk=1).identifier)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data['directories']), 1)
        self.assertEqual(len(response.data['files']), 5)

    def test_read_directory_for_catalog_record_recursively(self):
        """
        Test query parameters 'cr_identifier' with 'recursive'.
        """
        response = self.client.get('/rest/directories/1/files?recursive&cr_identifier=%s&depth=*'
            % CatalogRecord.objects.get(pk=1).identifier)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        file_list = list(File.objects.filter(record__pk=1).values_list('id', flat=True))
        self.assertEqual(len(response.data), len(file_list))
        for f in response.data:
            self.assertTrue(f['id'] in file_list)

        # not found cr_identifier should raise 400 instead of 404, which is raised when the
        # directory itself is not found. the error contains details about the 400
        response = self.client.get('/rest/directories/1/files?recursive&cr_identifier=notexisting')
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)

    def test_read_directory_for_not_catalog_record_recursively(self):
        """
        Test query parameters 'not_cr_identifier' with 'recursive'.
        """
        file_recursive = self.client.get('/rest/directories/1/files?recursive&depth=*').data
        file_list = list(File.objects.filter(record__pk=1).values_list('id', flat=True))
        response = self.client.get('/rest/directories/1/files?recursive&depth=*&not_cr_identifier=%s'
            % CatalogRecord.objects.get(pk=1).identifier)
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        self.assertEqual(len(response.data), len(file_recursive) - len(file_list))
        for f in response.data:
            self.assertTrue(f['id'] not in file_list)

        # not found not_cr_identifier should raise 400 instead of 404, which is raised when the
        # directory itself is not found. the error contains details about the 400
        response = self.client.get('/rest/directories/1/files?recursive&not_cr_identifier=notexisting')
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


class DirectoryApiReadCatalogRecordFileBrowsingAuthorizationTests(DirectoryApiReadCommon):
    """
    Test browsing files in the context of a specific CatalogRecord from authorization perspective
    """

    # THE OK TESTS

    def test_returns_ok_for_open_catalog_record_if_no_authorization(self):
        open_cr_json = self.get_open_cr_with_files_and_dirs_from_api_with_file_details()

        # Verify /rest/directories/<dir_id>/files?cr_identifier=cr_id returns dir files even without authorization for
        # open catalog record
        self._assert_ok(open_cr_json, 'no')

    def test_returns_ok_for_login_catalog_record_if_no_authorization(self):
        login_cr_json = self.get_open_cr_with_files_and_dirs_from_api_with_file_details(use_login_access_type=True)

        # Verify /rest/directories/<dir_id>/files?cr_identifier=cr_id returns dir files even without authorization for
        # login catalog record
        self._assert_ok(login_cr_json, 'no')

    def test_returns_ok_for_open_catalog_record_if_service_authorization(self):
        open_cr_json = self.get_open_cr_with_files_and_dirs_from_api_with_file_details()

        # Verify /rest/directories/<dir_id>/files?cr_identifier=cr_id returns dir files with service authorization for
        # open catalog record
        self._assert_ok(open_cr_json, 'service')

    def test_returns_ok_for_login_catalog_record_if_service_authorization(self):
        login_cr_json = self.get_open_cr_with_files_and_dirs_from_api_with_file_details(use_login_access_type=True)

        # Verify /rest/directories/<dir_id>/files?cr_identifier=cr_id returns dir files with service authorization for
        # login catalog record
        self._assert_ok(login_cr_json, 'service')

    @responses.activate
    def test_returns_ok_for_open_catalog_record_if_owner_authorization(self):
        self.create_end_user_data_catalogs()
        open_cr_json = self.get_open_cr_with_files_and_dirs_from_api_with_file_details(True)

        # Verify /rest/directories/<dir_id>/files?cr_identifier=cr_id returns dir files with owner authorization for
        # owner-owned open catalog record
        self._assert_ok(open_cr_json, 'owner')

    @responses.activate
    def test_returns_ok_for_login_catalog_record_if_owner_authorization(self):
        self.create_end_user_data_catalogs()
        login_cr_json = self.get_open_cr_with_files_and_dirs_from_api_with_file_details(True)

        # Verify /rest/directories/<dir_id>/files?cr_identifier=cr_id returns dir files with owner authorization for
        # owner-owned login_cr_json catalog record
        self._assert_ok(login_cr_json, 'owner')

    def test_returns_ok_for_restricted_catalog_record_if_service_authorization(self):
        restricted_cr_json = self.get_restricted_cr_with_files_and_dirs_from_api_with_file_details()

        # Verify /rest/directories/<dir_id>/files?cr_identifier=cr_id returns dir files with service authorization for
        # restricted catalog record
        self._assert_ok(restricted_cr_json, 'service')

    @responses.activate
    def test_returns_ok_for_restricted_catalog_record_if_owner_authorization(self):
        self.create_end_user_data_catalogs()
        restricted_cr_json = self.get_restricted_cr_with_files_and_dirs_from_api_with_file_details(True)

        # Verify /rest/directories/<dir_id>/files?cr_identifier=cr_id returns dir files with owner authorization for
        # owner-owned restricted catalog record
        self._assert_ok(restricted_cr_json, 'owner')

    def test_returns_ok_for_embargoed_catalog_record_if_available_reached_and_no_authorization(self):
        available_embargoed_cr_json = self.get_embargoed_cr_with_files_and_dirs_from_api_with_file_details(True)

        # Verify /rest/directories/<dir_id>/files?cr_identifier=cr_id returns dir files without authorization
        # for embargoed catalog record whose embargo date has been reached
        self._assert_ok(available_embargoed_cr_json, 'no')

    # THE FORBIDDEN TESTS

    def test_returns_forbidden_for_restricted_catalog_record_if_no_authorization(self):
        restricted_cr_json = self.get_restricted_cr_with_files_and_dirs_from_api_with_file_details()

        # Verify /rest/directories/<dir_id>/files?cr_identifier=cr_id returns forbidden without authorization
        # for restricted catalog record
        self._assert_forbidden(restricted_cr_json, 'no')

    def test_returns_forbidden_for_embargoed_catalog_record_if_available_not_reached_and_no_authorization(self):
        not_available_embargoed_cr_json = self.get_embargoed_cr_with_files_and_dirs_from_api_with_file_details(
            False)

        # Verify /rest/directories/<dir_id>/files?cr_identifier=cr_id returns forbidden without authorization
        # for embargoed catalog record whose embargo date has not been reached
        # Deactivate credentials
        self._assert_forbidden(not_available_embargoed_cr_json, 'no')

    def _assert_forbidden(self, cr_json, credentials_type):
        dir_id = cr_json['research_dataset']['directories'][0]['identifier']
        cr_id = cr_json['identifier']
        self._set_http_authorization(credentials_type)
        response = self.client.get('/rest/directories/{0}/files?cr_identifier={1}'.format(dir_id, cr_id))
        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN)

        response = self.client.get('/rest/directories/{0}/files?not_cr_identifier={1}'.format(dir_id, cr_id))
        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN)

    def _assert_ok(self, cr_json, credentials_type):
        dir_file_amt = cr_json['research_dataset']['directories'][0]['details']['file_count']
        dir_id = cr_json['research_dataset']['directories'][0]['identifier']
        cr_id = cr_json['identifier']
        self._set_http_authorization(credentials_type)
        response = self.client.get('/rest/directories/{0}/files?cr_identifier={1}&recursive&depth=*'
                                   .format(dir_id, cr_id))
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        self.assertEqual(len(response.data), dir_file_amt)

        response = self.client.get('/rest/directories/{0}/files?not_cr_identifier={1}&recursive&depth=*'
                                   .format(dir_id, cr_id))
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        self.assertEqual(len(response.data), 0)

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
        self.token['group_names'].append('IDA01:%s' % proj)
        self._use_http_authorization(method='bearer', token=self.token)

    @responses.activate
    def test_user_can_browse_files_from_their_projects(self):
        '''
        Ensure users can only read files from /rest/directories owned by them.
        '''
        self._use_http_authorization(method='bearer', token=self.token)

        # first read files without project access - should fail
        response = self.client.get('/rest/directories/1')
        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN, response.data)
        response = self.client.get('/rest/directories/1/files')
        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN, response.data)

        # set user to same project as previous files and try again. should now succeed
        self._update_token_with_project_of_directory(1)

        response = self.client.get('/rest/directories/1')
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        response = self.client.get('/rest/directories/1/files')
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)

    @responses.activate
    def test_browsing_by_project_and_file_path_is_protected(self):
        self._use_http_authorization(method='bearer', token=self.token)

        dr = Directory.objects.get(pk=2)
        response = self.client.get('/rest/directories/files?path=%s&project=%s' %
            (dr.directory_path, dr.project_identifier))
        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN, response.data)

        self._update_token_with_project_of_directory(2)
        response = self.client.get('/rest/directories/files?path=%s&project=%s' %
            (dr.directory_path, dr.project_identifier))
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)

    @responses.activate
    def test_browsing_in_cr_context(self):
        '''
        Cr with open access type should be available for any end-user api user. Browsing files for a cr with restricted
        access type should be forbidden for non-owner (or service) user.
        '''
        cr = CatalogRecord.objects.get(pk=1)
        self._use_http_authorization(method='bearer', token=self.token)
        response = self.client.get('/rest/directories/3/files?cr_identifier={0}'.format(cr.identifier))
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)

        cr.research_dataset['access_rights']['access_type']['identifier'] = ACCESS_TYPES['restricted']
        cr.force_save()

        response = self.client.get('/rest/directories/3/files?cr_identifier={0}'.format(cr.identifier))
        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN)

    @responses.activate
    def test_browsing_in_not_cr_context(self):
        '''
        Cr with open access type should be available for any end-user api user. Browsing files for a cr with restricted
        access type should be forbidden for non-owner (or service) user.
        '''
        cr = CatalogRecord.objects.get(pk=1)
        self._use_http_authorization(method='bearer', token=self.token)
        response = self.client.get('/rest/directories/3/files?not_cr_identifier={0}'.format(cr.identifier))
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)

        cr.research_dataset['access_rights']['access_type']['identifier'] = ACCESS_TYPES['restricted']
        cr.force_save()

        response = self.client.get('/rest/directories/3/files?not_cr_identifier={0}'.format(cr.identifier))
        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN, response.data)


class DirectoryApiReadPaginationTests(DirectoryApiReadCommon):

    """
    Test paginated directory and file browsing.
    Should return directories and/or files depending on limit and offset parameters. Defaul is ofcet is 10
    """

    def setUp(self):
        self._use_http_authorization()
        self._create_test_dirs(14)

    def test_read_directory_with_default_limit_pagination(self):
        """
        Test browsing files with pagination
        """
        file_dict = self._get_dirs_files_ids('/rest/directories/24/files')

        response = self.client.get('/rest/directories/24/files?pagination')
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data['results']['directories']), 10)
        self.assertEqual(len(response.data['results']['files']), 0)
        self.assertEqual(response.data['results']['directories'][0]['id'], file_dict['directories'][0])
        self.assertEqual(response.data['results']['directories'][9]['id'], file_dict['directories'][9])

        next_link = response.data['next'].split('http://testserver')[1]
        response = self.client.get(next_link)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data['results']['directories']), 4)
        self.assertEqual(len(response.data['results']['files']), 6)
        self.assertEqual(response.data['results']['directories'][0]['id'], file_dict['directories'][10])
        self.assertEqual(response.data['results']['directories'][3]['id'], file_dict['directories'][13])
        self.assertEqual(response.data['results']['files'][0]['id'], file_dict['files'][0])
        self.assertEqual(response.data['results']['files'][5]['id'], file_dict['files'][5])

        next_link = response.data['next'].split('http://testserver')[1]
        response = self.client.get(next_link)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data['results']['directories']), 0)
        self.assertEqual(len(response.data['results']['files']), 10)
        self.assertEqual(response.data['results']['files'][0]['id'], file_dict['files'][6])
        self.assertEqual(response.data['results']['files'][9]['id'], file_dict['files'][15])

        prev_link = response.data['previous'].split('http://testserver')[1]
        response = self.client.get(prev_link)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data['results']['directories']), 4)
        self.assertEqual(len(response.data['results']['files']), 6)
        self.assertEqual(response.data['results']['directories'][0]['id'], file_dict['directories'][10])
        self.assertEqual(response.data['results']['directories'][3]['id'], file_dict['directories'][13])
        self.assertEqual(response.data['results']['files'][0]['id'], file_dict['files'][0])
        self.assertEqual(response.data['results']['files'][5]['id'], file_dict['files'][5])

    def test_read_directory_with_custom_limit_pagination(self):
        file_dict = self._get_dirs_files_ids('/rest/directories/24/files')

        response = self.client.get('/rest/directories/24/files?limit=4&offset=12&pagination')
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data['results']['directories']), 2)
        self.assertEqual(response.data['results']['directories'][0]['id'], file_dict['directories'][12])
        self.assertEqual(response.data['results']['directories'][1]['id'], file_dict['directories'][13])
        self.assertEqual(len(response.data['results']['files']), 2)
        self.assertEqual(response.data['results']['files'][0]['id'], file_dict['files'][0])
        self.assertEqual(response.data['results']['files'][1]['id'], file_dict['files'][1])

        next_link = response.data['next'].split('http://testserver')[1]
        prev_link = response.data['previous'].split('http://testserver')[1]

        response = self.client.get(next_link)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data['results']['directories']), 0)
        self.assertEqual(len(response.data['results']['files']), 4)
        self.assertEqual(response.data['results']['files'][0]['id'], file_dict['files'][2])
        self.assertEqual(response.data['results']['files'][3]['id'], file_dict['files'][5])

        response = self.client.get(prev_link)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data['results']['directories']), 4)
        self.assertEqual(len(response.data['results']['files']), 0)
        self.assertEqual(response.data['results']['directories'][0]['id'], file_dict['directories'][8])
        self.assertEqual(response.data['results']['directories'][3]['id'], file_dict['directories'][11])

    def test_read_directory_with_recursive_and_pagination(self):
        '''
        Query with recursive flag must return only files as a list
        '''
        file_list = self._get_dirs_files_ids('/rest/directories/24/files?recursive')

        response = self.client.get('/rest/directories/24/files?recursive&pagination')
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data['results']), 10)
        self.assertEqual(response.data['results'][0]['id'], file_list[0])
        self.assertEqual(response.data['results'][9]['id'], file_list[9])

        next_link = response.data['next'].split('http://testserver')[1]
        response = self.client.get(next_link)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data['results']), 10)
        self.assertEqual(response.data['results'][0]['id'], file_list[10])
        self.assertEqual(response.data['results'][9]['id'], file_list[19])

        prev_link = response.data['previous'].split('http://testserver')[1]
        response = self.client.get(prev_link)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data['results']), 10)
        self.assertEqual(response.data['results'][0]['id'], file_list[0])
        self.assertEqual(response.data['results'][9]['id'], file_list[9])

    def test_read_directory_with_dirs_only_and_pagination(self):
        '''
        Query with directories_only flag must return only directories
        '''
        file_dict = self._get_dirs_files_ids('/rest/directories/24/files?directories_only')['directories']

        response = self.client.get('/rest/directories/24/files?directories_only&pagination=true')
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        self.assertEqual(len(response.data['results']['directories']), 10)
        self.assertEqual(response.data['results']['directories'][0]['id'], file_dict[0])
        self.assertEqual(response.data['results']['directories'][9]['id'], file_dict[9])

        next_link = response.data['next'].split('http://testserver')[1]
        response = self.client.get(next_link)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data['results']['directories']), 4)
        self.assertEqual(response.data['results']['directories'][0]['id'], file_dict[10])
        self.assertEqual(response.data['results']['directories'][3]['id'], file_dict[13])

        prev_link = response.data['previous'].split('http://testserver')[1]
        response = self.client.get(prev_link)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data['results']['directories']), 10)
        self.assertEqual(response.data['results']['directories'][0]['id'], file_dict[0])
        self.assertEqual(response.data['results']['directories'][9]['id'], file_dict[9])

    def test_read_directory_with_parent_and_pagination(self):
        '''
        Query with directories_only flag must return only directories
        '''
        file_dict = self._get_dirs_files_ids('/rest/directories/24/files?include_parent')

        response = self.client.get('/rest/directories/24/files?include_parent&pagination=true')
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data['results']['directories']), 10)
        self.assertEqual(response.data['results']['directories'][0]['id'], file_dict['directories'][0])
        self.assertEqual(response.data['results']['directories'][9]['id'], file_dict['directories'][9])
        self.assertEqual(response.data['results']['id'], 24)
        self.assertEqual(response.data['results']['directory_name'], "10")

        next_link = response.data['next'].split('http://testserver')[1]
        response = self.client.get(next_link)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data['results']['directories']), 4)
        self.assertEqual(len(response.data['results']['files']), 6)
        self.assertEqual(response.data['results']['directories'][0]['id'], file_dict['directories'][10])
        self.assertEqual(response.data['results']['directories'][3]['id'], file_dict['directories'][13])
        self.assertEqual(response.data['results']['files'][0]['id'], file_dict['files'][0])
        self.assertEqual(response.data['results']['files'][5]['id'], file_dict['files'][5])
        self.assertEqual(response.data['results']['id'], 24)
        self.assertEqual(response.data['results']['directory_name'], "10")
