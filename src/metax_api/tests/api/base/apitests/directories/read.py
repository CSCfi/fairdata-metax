from django.core.management import call_command
from rest_framework import status
from rest_framework.test import APITestCase

from metax_api.models import CatalogRecord, Directory
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

        # dir id 1 (the root) contains 0 files, but recursively 20
        response = self.client.get('/rest/directories/1/files?recursive')
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data), 20)

        # dir id 3 contains 5 files, but recursively 20
        response = self.client.get('/rest/directories/3/files?recursive')
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data), 20)

        # dir id 4 contains 5 files, but recursively 15
        response = self.client.get('/rest/directories/4/files?recursive')
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data), 15)

        # dir id 5 contains 0 files
        response = self.client.get('/rest/directories/5/files?recursive')
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data), 10)

        # dir id 6 contains 10 files
        response = self.client.get('/rest/directories/6/files?recursive')
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


class DirectoryApiReadCatalogRecordFileBrowsingTests(DirectoryApiReadCommon):

    """
    Test browsing files in the context of a specific CatalogRecord. Should always
    only dispaly those fiels that were selected for that CR, and only those dirs,
    that contained suchs files, or would contain such files further down the tree.
    """

    def test_read_directory_for_catalog_record(self):
        """
        Test query parameter 'urn_identifier'.
        """
        urn_identifier = CatalogRecord.objects.get(pk=1).urn_identifier

        response = self.client.get('/rest/directories/3/files?urn_identifier=%s' % urn_identifier)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual('directories' in response.data, True)
        self.assertEqual('files' in response.data, True)
        self.assertEqual(len(response.data['directories']), 0)
        self.assertEqual(len(response.data['files']), 2)

    def test_read_directory_for_catalog_record_not_found(self):
        """
        Not found urn_identifier should raise 400 instead of 404, which is raised when the
        directory itself is not found. the error contains details about the 400.
        """
        response = self.client.get('/rest/directories/3/files?urn_identifier=notexisting')
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)

    def test_read_directory_for_catalog_record_directory_does_not_exist(self):
        """
        A directory may have files in a project, but those files did not necessarily exist
        or were not selected for a specific CR.
        """

        # should be OK...
        response = self.client.get('/rest/directories/4/files')
        self.assertEqual(response.status_code, status.HTTP_200_OK)

        urn_identifier = CatalogRecord.objects.get(pk=1).urn_identifier
        # ... but should not contain any files FOR THIS CR
        response = self.client.get('/rest/directories/4/files?urn_identifier=%s' % urn_identifier)
        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND)

    def test_read_directory_for_catalog_record_recursively(self):
        """
        Test query parameter 'urn_identifier' with 'recursive'.
        """
        urn_identifier = CatalogRecord.objects.get(pk=1).urn_identifier
        response = self.client.get('/rest/directories/1/files?recursive&urn_identifier=%s' % urn_identifier)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data), 2)

        # not found urn_identifier should raise 400 instead of 404, which is raised when the
        # directory itself is not found. the error contains details about the 400
        response = self.client.get('/rest/directories/1/files?recursive&urn_identifier=notexisting')
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
