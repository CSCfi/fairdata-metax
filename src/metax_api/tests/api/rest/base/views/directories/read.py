from django.db.models import Sum
from django.core.management import call_command
from rest_framework import status
from rest_framework.test import APITestCase

from metax_api.models import CatalogRecord, Directory
from metax_api.services import FileService
from metax_api.tests.utils import test_data_file_path, TestClassUtils


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


class DirectoryApiReadCatalogRecordFileBrowsingTests(DirectoryApiReadCommon):

    """
    Test browsing files in the context of a specific CatalogRecord. Should always
    only dispaly those files that were selected for that CR, and only those dirs,
    that contained suchs files, or would contain such files further down the tree.
    """

    def test_read_directory_for_catalog_record(self):
        """
        Test query parameter 'metadata_version_identifier'.
        """
        metadata_version_identifier = CatalogRecord.objects.get(pk=1).metadata_version_identifier

        response = self.client.get('/rest/directories/3/files?metadata_version_identifier=%s'
                                   % metadata_version_identifier)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual('directories' in response.data, True)
        self.assertEqual('files' in response.data, True)
        self.assertEqual(len(response.data['directories']), 0)
        self.assertEqual(len(response.data['files']), 2)

    def test_read_directory_for_catalog_record_not_found(self):
        """
        Not found metadata_version_identifier should raise 400 instead of 404, which is raised when the
        directory itself is not found. the error contains details about the 400.
        """
        response = self.client.get('/rest/directories/3/files?metadata_version_identifier=notexisting')
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)

    def test_read_directory_for_catalog_record_directory_does_not_exist(self):
        """
        A directory may have files in a project, but those files did not necessarily exist
        or were not selected for a specific CR.
        """

        # should be OK...
        response = self.client.get('/rest/directories/4/files')
        self.assertEqual(response.status_code, status.HTTP_200_OK)

        metadata_version_identifier = CatalogRecord.objects.get(pk=1).metadata_version_identifier
        # ... but should not contain any files FOR THIS CR
        response = self.client.get('/rest/directories/4/files?metadata_version_identifier=%s'
                                   % metadata_version_identifier)
        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND)

    def test_read_directory_for_catalog_record_recursively(self):
        """
        Test query parameter 'metadata_version_identifier' with 'recursive'.
        """
        metadata_version_identifier = CatalogRecord.objects.get(pk=1).metadata_version_identifier
        response = self.client.get('/rest/directories/1/files?recursive&metadata_version_identifier=%s&depth=*'
                                   % metadata_version_identifier)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data), 2)

        # not found metadata_version_identifier should raise 400 instead of 404, which is raised when the
        # directory itself is not found. the error contains details about the 400
        response = self.client.get('/rest/directories/1/files?recursive&metadata_version_identifier=notexisting')
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)

    def test_directory_byte_size_and_file_count(self):
        """
        Test byte size and file count are calculated correctly for directories when browsing files
        in the context of a single record.
        """
        dr = Directory.objects.get(pk=2)
        cr = CatalogRecord.objects.get(pk=1)

        # calculate all directory byte sizes and file counts to real values, from their default 0 values
        FileService.calculate_project_directory_byte_sizes_and_file_counts(dr.project_identifier)

        byte_size = cr.files.filter(file_path__startswith='%s/' % dr.directory_path) \
            .aggregate(Sum('byte_size'))['byte_size__sum']
        file_count = cr.files.filter(file_path__startswith='%s/' % dr.directory_path).count()

        response = self.client.get(
            '/rest/directories/%d/files?metadata_version_identifier=%s' % (dr.id, cr.metadata_version_identifier))
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        self.assertEqual('directories' in response.data, True)
        self.assertEqual('byte_size' in response.data['directories'][0], True)
        self.assertEqual('file_count' in response.data['directories'][0], True)
        self.assertEqual(response.data['directories'][0]['byte_size'], byte_size)
        self.assertEqual(response.data['directories'][0]['file_count'], file_count)

        # browse the sub dir, with ?include_parent=true for added verification
        dr = Directory.objects.get(pk=response.data['directories'][0]['id'])

        byte_size = cr.files.filter(file_path__startswith='%s/' % dr.directory_path) \
            .aggregate(Sum('byte_size'))['byte_size__sum']
        file_count = cr.files.filter(file_path__startswith='%s/' % dr.directory_path).count()

        response = self.client.get(
            '/rest/directories/%d/files?metadata_version_identifier=%s&include_parent'
            % (dr.id, cr.metadata_version_identifier))
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        self.assertEqual('byte_size' in response.data, True)
        self.assertEqual('file_count' in response.data, True)
        self.assertEqual(response.data['byte_size'], byte_size)
        self.assertEqual(response.data['file_count'], file_count)
