# This file is part of the Metax API service
#
# Copyright 2017-2018 Ministry of Education and Culture, Finland
#
# :author: CSC - IT Center for Science Ltd., Espoo Finland <servicedesk@csc.fi>
# :license: MIT

from copy import deepcopy
from os.path import dirname

from django.db.models import Sum
from django.core.management import call_command
from rest_framework import status
from rest_framework.test import APITestCase
import responses

from metax_api.models import CatalogRecord, Directory, File
from metax_api.services import RedisCacheService as cache
from metax_api.tests.utils import get_test_oidc_token, test_data_file_path, TestClassUtils


class FileApiWriteCommon(APITestCase, TestClassUtils):
    @classmethod
    def setUpClass(cls):
        """
        Loaded only once for test cases inside this class.
        """
        call_command('loaddata', test_data_file_path, verbosity=0)
        super(FileApiWriteCommon, cls).setUpClass()

    def setUp(self):
        """
        Reloaded for every test case
        """
        call_command('loaddata', test_data_file_path, verbosity=0)
        file_from_test_data = self._get_object_from_test_data('file')
        self.identifier = file_from_test_data['identifier']
        self.pidentifier = file_from_test_data['project_identifier']
        self.file_name = file_from_test_data['file_name']

        """
        New data that is sent to the server for POST, PUT, PATCH requests. Modified
        slightly as approriate for different purposes
        """
        self.test_new_data = self._get_new_test_data()
        self.second_test_new_data = self._get_second_new_test_data()
        self._use_http_authorization()

    def _get_new_test_data(self):
        from_test_data = self._get_object_from_test_data('file', requested_index=0)
        from_test_data.update({
            "checksum": {
                "value": "habeebit",
                "algorithm": "sha2",
                "checked": "2017-05-23T10:07:22.559656Z",
            },
            "file_name": "file_name_1",
            "file_path": from_test_data['file_path'].replace('/some/path', '/some/other_path'),
            "identifier": "urn:nbn:fi:csc-ida201401200000000001",
            "file_storage": self._get_object_from_test_data('filestorage', requested_index=0)
        })
        from_test_data['file_path'] = from_test_data['file_path'].replace('/Experiment_X/', '/test/path/')
        from_test_data['project_identifier'] = 'test_project_identifier'
        del from_test_data['id']
        return from_test_data

    def _get_second_new_test_data(self):
        from_test_data = self._get_new_test_data()
        from_test_data["identifier"] = "urn:nbn:fi:csc-ida201401200000000002"
        self._change_file_path(from_test_data, "file_name_2")
        return from_test_data

    def _count_dirs_from_path(self, file_path):
        expected_dirs_count = 1
        dir_name = dirname(file_path)
        while dir_name != '/':
            dir_name = dirname(dir_name)
            expected_dirs_count += 1
        return expected_dirs_count

    def _check_project_root_byte_size_and_file_count(self, project_identifier):
        """
        A rather simple test to fetch the root directory of a project, and verify that the
        root's calculated total byte size and file count match what exists in the db.
        """
        byte_size = File.objects.filter(project_identifier=project_identifier) \
            .aggregate(Sum('byte_size'))['byte_size__sum']
        file_count = File.objects.filter(project_identifier=project_identifier).count()

        response = self.client.get('/rest/directories/root?project=%s' % project_identifier)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data['byte_size'], byte_size)
        self.assertEqual(response.data['file_count'], file_count)

    def _change_file_path(self, file, new_name):
        file['file_path'] = file['file_path'].replace(file['file_name'], new_name)
        file['file_name'] = new_name


class FileApiWriteReferenceDataValidationTests(FileApiWriteCommon):

    @classmethod
    def setUpClass(cls):
        """
        Loaded only once for test cases inside this class.
        """
        call_command('updatereferencedata', verbosity=0)
        super(FileApiWriteReferenceDataValidationTests, cls).setUpClass()

    def setUp(self):
        super().setUp()
        ffv_refdata = cache.get('reference_data')['reference_data']['file_format_version']

        # File format version entry in reference data that has some output_format_version
        self.ff_with_version = None
        # File format version entry in reference data that has same input_file_format than ff_with_versions but
        # different output_format_version
        self.ff_with_different_version = None
        # File format version entry in reference data which has not output_format_version
        self.ff_without_version = None

        for ffv_obj in ffv_refdata:
            if self.ff_with_different_version is None and self.ff_with_version is not None:
                if ffv_obj['input_file_format'] == self.ff_with_version['input_file_format']:
                    self.ff_with_different_version = ffv_obj
            if self.ff_with_version is None and ffv_obj['output_format_version']:
                self.ff_with_version = ffv_obj
            if self.ff_without_version is None and not ffv_obj['output_format_version']:
                self.ff_without_version = ffv_obj

        self.assertTrue(self.ff_with_version['output_format_version'] != '')
        self.assertTrue(self.ff_with_different_version['output_format_version'] != '')
        self.assertTrue(self.ff_with_version['input_file_format'] ==
                        self.ff_with_different_version['input_file_format'])
        self.assertTrue(self.ff_with_version['output_format_version'] !=
                        self.ff_with_different_version['output_format_version'])
        self.assertTrue(self.ff_without_version['output_format_version'] == '')

    def test_file_format_version_with_invalid_file_format_when_format_version_given_1(self):
        self.test_new_data['file_characteristics']['format_version'] = 'any'
        response = self.client.post('/rest/files', self.test_new_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual('file_characteristics' in response.data.keys(), True)
        self.assertEqual('file_characteristics.file_format' in response.data['file_characteristics'], True)

    def test_file_format_version_with_invalid_file_format_when_format_version_given_2(self):
        self.test_new_data['file_characteristics']['file_format'] = 'nonexisting'
        self.test_new_data['file_characteristics']['format_version'] = 'any'
        response = self.client.post('/rest/files', self.test_new_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual('file_characteristics' in response.data.keys(), True)
        self.assertEqual('file_characteristics.file_format' in response.data['file_characteristics'], True)

    def test_file_format_version_with_invalid_format_version_when_file_format_has_versions_1(self):
        self.test_new_data['file_characteristics']['file_format'] = self.ff_with_version['input_file_format']
        response = self.client.post('/rest/files', self.test_new_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual('file_characteristics' in response.data.keys(), True)
        self.assertEqual('file_characteristics.format_version' in response.data['file_characteristics'], True)

    def test_file_format_version_with_invalid_format_version_when_file_format_has_versions_2(self):
        self.test_new_data['file_characteristics']['file_format'] = self.ff_with_version['input_file_format']
        self.test_new_data['file_characteristics']['format_version'] = 'nonexisting'
        response = self.client.post('/rest/files', self.test_new_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual('file_characteristics' in response.data.keys(), True)
        self.assertEqual('file_characteristics.format_version' in response.data['file_characteristics'], True)

    def test_file_format_version_with_empty_format_version_when_file_format_has_no_version_1(self):
        self.test_new_data['file_characteristics']['file_format'] = self.ff_without_version['input_file_format']
        response = self.client.post('/rest/files', self.test_new_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED)

    def test_file_format_version_with_empty_format_version_when_file_format_has_no_version_2(self):
        self.test_new_data['file_characteristics']['file_format'] = self.ff_without_version['input_file_format']
        self.test_new_data['file_characteristics']['format_version'] = ''
        response = self.client.post('/rest/files', self.test_new_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED)

    def test_file_format_version_with_valid_file_format_and_valid_file_version_1(self):
        self.test_new_data['file_characteristics']['file_format'] = self.ff_with_version['input_file_format']
        self.test_new_data['file_characteristics']['format_version'] = self.ff_with_version['output_format_version']
        response = self.client.post('/rest/files', self.test_new_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED)

    def test_file_format_version_with_valid_file_format_and_valid_file_version_2(self):
        self.test_new_data['file_characteristics']['file_format'] = self.ff_with_version['input_file_format']
        self.test_new_data['file_characteristics']['format_version'] = \
            self.ff_with_different_version['output_format_version']
        response = self.client.post('/rest/files', self.test_new_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED)

    # update tests

    def test_file_characteristics_is_validated_on_update(self):
        """
        Ensure validation also works when updating existing files.
        """
        self.test_new_data['file_characteristics']['file_format'] = self.ff_without_version['input_file_format']
        response = self.client.post('/rest/files', self.test_new_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)

        response = self.client.put('/rest/files/%s' % response.data['identifier'], response.data, format="json")
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)


class FileApiWriteCreateTests(FileApiWriteCommon):
    #
    #
    #
    # create apis
    #
    #
    #

    def test_create_file(self):
        # note: leading and trailing whitespace must be preserved.
        newly_created_file_name = "   MX  .201015_Suomessa_tavattavat_ruokasammakot_ovat_väritykseltään_vaihtelevia_" \
            "osa_on_ruskeita,_osa_kirkkaankin_vihreitä._Vihersammakoiden_silmät_ovat_kohtalaisen_korkealla_päälae" \
            "lla._Sammakkolampi.fi_CC-BY-NC-4.0_thumb.jpg.meta   "
        self.test_new_data['file_name'] = newly_created_file_name
        self.test_new_data['identifier'] = 'urn:nbn:fi:csc-thisisanewurn'

        response = self.client.post('/rest/files', self.test_new_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)
        self.assertEqual('file_name' in response.data.keys(), True)
        self.assertEqual(response.data['file_name'], newly_created_file_name)
        self._check_project_root_byte_size_and_file_count(response.data['project_identifier'])

    def test_create_file_error_identifier_exists(self):
        # first ok
        response = self.client.post('/rest/files', self.test_new_data, format="json")
        # second should give error
        response = self.client.post('/rest/files', self.test_new_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual('identifier' in response.data.keys(), True)
        self.assertEqual('already exists' in response.data['identifier'][0], True)

    def test_allow_creating_previously_deleted_file(self):
        """
        It should be possible to delete a file, and then create the exact same file again
        without letting the removed file conflict.
        """
        response = self.client.post('/rest/files', self.test_new_data, format="json")
        response = self.client.delete('/rest/files/%d' % response.data['id'], format="json")

        response = self.client.post('/rest/files', self.test_new_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)

    def test_create_file_error_json_validation(self):
        self.test_new_data['identifier'] = 'urn:nbn:fi:csc-thisisanewurn'
        self.test_new_data['file_characteristics'] = {
            "application_name": "Application Name",
            "description": "A nice description 0000000010",
            "metadata_modified": 12345,
            "file_created": "2014-01-17T08:19:31Z",
            "encoding": "utf-8",
            "title": "A title 0000000010"
        }

        response = self.client.post('/rest/files', self.test_new_data, format="json")

        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual('file_characteristics' in response.data.keys(), True,
                         'The error should concern the field file_characteristics')
        self.assertEqual('metadata_modified' in response.data['file_characteristics'][0], True,
                         'The error should contain the name of the erroneous field')
        self.assertEqual('Json path:' in response.data['file_characteristics'][0], True,
                         'The error should contain the json path')

    #
    # create list operations
    #

    def test_create_file_list(self):
        self.test_new_data['identifier'] = 'urn:nbn:fi:csc-thisisanewurn'
        self._change_file_path(self.test_new_data, 'one_file.txt')

        self.second_test_new_data['identifier'] = 'urn:nbn:fi:csc-thisisanewurnalso'
        self._change_file_path(self.second_test_new_data, 'two_file.txt')

        response = self.client.post('/rest/files', [self.test_new_data, self.second_test_new_data], format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)
        self.assertEqual('failed' in response.data.keys(), True)
        self.assertEqual('success' in response.data.keys(), True)
        self.assertEqual('object' in response.data['success'][0].keys(), True)
        self.assertEqual(len(response.data['failed']), 0, response.data['failed'])
        self.assertEqual(len(response.data['success']), 2)
        self._check_project_root_byte_size_and_file_count(response.data['success'][0]['object']['project_identifier'])

    def test_create_file_list_error_one_fails(self):
        newly_created_file_name = 'newly_created_file_name'
        self.test_new_data['file_name'] = newly_created_file_name
        self.test_new_data['identifier'] = 'urn:nbn:fi:csc-thisisanewurn'
        # same as above - should fail
        self.second_test_new_data['identifier'] = 'urn:nbn:fi:csc-thisisanewurn'

        response = self.client.post('/rest/files', [self.test_new_data, self.second_test_new_data], format="json")

        """
        List response looks like
        {
            'success': [
                { 'object': object },
                more objects...
            ],
            'failed': [
                {
                    'object': object,
                    'errors': {'field': ['message'], 'otherfiled': ['message']}
                },
                more objects...
            ]
        }
        """
        self.assertEqual(response.status_code, status.HTTP_201_CREATED)
        self.assertEqual('success' in response.data.keys(), True)
        self.assertEqual('failed' in response.data.keys(), True)
        self.assertEqual('object' in response.data['failed'][0].keys(), True)
        self.assertEqual('file_name' in response.data['failed'][0]['object'].keys(), True)
        self.assertEqual('identifier' in response.data['failed'][0]['errors'], True,
                         'The error should have been about an already existing identifier')

    def test_parameter_ignore_already_exists_errors(self):
        newly_created_file_name = 'newly_created_file_name'
        self.test_new_data['file_name'] = newly_created_file_name
        self.test_new_data['identifier'] = 'urn:nbn:fi:csc-thisisanewurn'
        # same as above - should cause an error.
        self.second_test_new_data['identifier'] = 'urn:nbn:fi:csc-thisisanewurn'

        response = self.client.post('/rest/files?ignore_already_exists_errors',
            [self.test_new_data, self.second_test_new_data], format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)
        self.assertEqual(len(response.data), 2)
        self.assertEqual('already exists' in response.data['success'][1]['object']['detail'], True)

    def test_create_file_list_error_all_fail(self):
        newly_created_file_name = 'newly_created_file_name'
        self.test_new_data['file_name'] = newly_created_file_name
        # identifier is a required field, should fail
        self.test_new_data['identifier'] = None
        self.second_test_new_data['identifier'] = None

        response = self.client.post('/rest/files', [self.test_new_data, self.second_test_new_data], format="json")
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual('success' in response.data.keys(), True)
        self.assertEqual('failed' in response.data.keys(), True)
        self.assertEqual('object' in response.data['failed'][0].keys(), True)
        self.assertEqual(len(response.data['success']), 0)
        self.assertEqual(len(response.data['failed']), 2)


class FileApiWriteCreateDirectoriesTests(FileApiWriteCommon):

    """
    Only checking directories related stuff in these tests
    """

    def test_create_file_hierarchy_from_single_file(self):
        """
        Create, from a single file, a file hierarchy for a project which has 0 files
        or directories created previously.
        """

        f = self._form_complex_list_from_test_file()[0]
        file_path = '/project_y_FROZEN/Experiment_1/path/of/lonely/file_and_this_also_has_to_support' \
            'veryverylooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooo' \
            'ooooooooooooooooooooooooooooooooooooooongdirectorynames/%s'
        f['file_path'] = file_path % f['file_name']
        f['identifier'] = 'abc123111'

        response = self.client.post('/rest/files', f, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)
        self.assertEqual('date_created' in response.data, True)
        self.assertEqual('parent_directory' in response.data, True)

        dirs_count = Directory.objects.filter(project_identifier='project_y').count()
        dirs_created_count = self._count_dirs_from_path(f['file_path'])
        self.assertEqual(dirs_count, dirs_created_count)

    def test_create_file_append_to_existing_directory(self):
        """
        Appending a file to an existing file hierarchy should not cause any other
        changes in any other directories.

        Note: Targeting project_x, which exists in pre-generated test data.
        """
        project_identifier = 'project_x'
        dir_count_before = Directory.objects.filter(project_identifier=project_identifier).count()
        file_count_before = Directory.objects.filter(project_identifier=project_identifier,
            directory_path='/project_x_FROZEN/Experiment_X/Phase_1').first().files.all().count()

        f = self._form_complex_list_from_test_file()[0]
        f['file_path'] = '/project_x_FROZEN/Experiment_X/Phase_1/%s' % f['file_name']
        f['identifier'] = '%s-111' % f['file_path']
        f['project_identifier'] = project_identifier

        response = self.client.post('/rest/files', f, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)
        self.assertEqual('date_created' in response.data, True)
        self.assertEqual('parent_directory' in response.data, True)

        dir_count_after = Directory.objects.filter(project_identifier=project_identifier).count()
        file_count_after = Directory.objects.filter(project_identifier=project_identifier,
            directory_path='/project_x_FROZEN/Experiment_X/Phase_1').first().files.all().count()
        self.assertEqual(dir_count_before, dir_count_after)
        self.assertEqual(file_count_after - file_count_before, 1)

    def test_create_file_hierarchy_from_file_list_with_no_existing_files(self):
        """
        Create a file hierarchy for a project which has 0 files or directories created previously.

        Here, a directory /project_y_FROZEN/Experiment_1 is "frozen"
        """
        experiment_1_file_list = self._form_complex_list_from_test_file()

        response = self.client.post('/rest/files', experiment_1_file_list, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)
        self.assertEqual('success' in response.data.keys(), True)
        self.assertEqual(len(response.data['success']), 12)
        self.assertEqual(len(response.data['failed']), 0)

        dirs_dict = self._assert_directory_parent_dirs('project_y')
        self._assert_file_parent_dirs(dirs_dict, response)

    def test_create_file_hierarchy_from_file_list_with_existing_files(self):
        """
        Create a file hierarchy for a project which already has files or directories
        created previously.

        Here the interesting part is, the top-most dir in the file list should find
        an existing directory, which it can use as its parent dir.

        Here, a directory /project_y_FROZEN/Experiment_2/Phase_1/Data is "frozen",
        when /project_y_FROZEN already exists.
        """

        # setup db to have pre-existing dirs
        experiment_1_file_list = self._form_complex_list_from_test_file()
        response = self.client.post('/rest/files', experiment_1_file_list, format="json")

        # form new test data
        experiment_2_file_list = self._form_complex_list_from_test_file()

        for i, f in enumerate(experiment_2_file_list):
            f['file_path'] = f['file_path'].replace('/project_y_FROZEN/Experiment_1',
                                                    '/project_y_FROZEN/Experiment_2/Phase_1/Data')
            f['identifier'] = '%s-%d' % (f['file_path'], i)

        response = self.client.post('/rest/files', experiment_2_file_list, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)
        self.assertEqual('success' in response.data.keys(), True)
        self.assertEqual(len(response.data['success']), 12)
        self.assertEqual(len(response.data['failed']), 0)

        dirs_dict = self._assert_directory_parent_dirs('project_y')
        self._assert_file_parent_dirs(dirs_dict, response)

    def test_append_files_to_existing_directory(self):
        """
        Append some files to an existing directory.

        Here, 5 files are added to directory /project_y_FROZEN/Experiment_2/
        """

        # setup db to have pre-existing dirs
        experiment_1_file_list = self._form_complex_list_from_test_file()
        response = self.client.post('/rest/files', experiment_1_file_list, format="json")

        # form new test data, and trim it down a bit
        experiment_2_file_list = self._form_complex_list_from_test_file()
        while len(experiment_2_file_list) > 5:
            experiment_2_file_list.pop()

        for i, f in enumerate(experiment_2_file_list):
            f['file_path'] = '/project_y_FROZEN/Experiment_2/%s' % f['file_name']
            f['identifier'] = '%s-%d' % (f['file_path'], i)

        response = self.client.post('/rest/files', experiment_2_file_list, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED)
        self.assertEqual('success' in response.data.keys(), True)
        self.assertEqual(len(response.data['failed']), 0, response.data['failed'])
        self.assertEqual(len(response.data['success']), 5)

        dirs_dict = self._assert_directory_parent_dirs('project_y')
        self._assert_file_parent_dirs(dirs_dict, response)

    def test_append_one_file_to_existing_directory(self):
        """
        Append one file to an existing directory.

        Here, 1 file is added to directory /project_y_FROZEN/Experiment_2/
        """

        # setup db to have pre-existing dirs
        experiment_1_file_list = self._form_complex_list_from_test_file()
        response = self.client.post('/rest/files', experiment_1_file_list, format="json")

        # form new test data, but use just the first item
        experiment_2_file_list = self._form_complex_list_from_test_file()[0:1]

        for i, f in enumerate(experiment_2_file_list):
            f['file_path'] = '/project_y_FROZEN/Experiment_2/%s' % f['file_name']
            f['identifier'] = '%s-%d' % (f['file_path'], i)

        response = self.client.post('/rest/files', experiment_2_file_list, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED)
        self.assertEqual('success' in response.data.keys(), True)
        self.assertEqual(len(response.data['success']), 1)
        self.assertEqual(len(response.data['failed']), 0)

        dirs_dict = self._assert_directory_parent_dirs('project_y')
        self._assert_file_parent_dirs(dirs_dict, response)

    def test_create_file_hierarchy_error_file_list_has_invalid_data(self):
        """
        If even one file is missing file_path or project_identifier, the
        request is immediately terminated. Creating files for multiple projects
        in a single request is also not permitted.
        """
        experiment_1_file_list = self._form_complex_list_from_test_file()
        experiment_1_file_list[0].pop('file_path')
        response = self.client.post('/rest/files', experiment_1_file_list, format="json")
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual('file_path' in response.data, True)
        self.assertEqual('required parameter' in response.data['file_path'][0], True)

        experiment_1_file_list = self._form_complex_list_from_test_file()
        experiment_1_file_list[0].pop('project_identifier')
        response = self.client.post('/rest/files', experiment_1_file_list, format="json")
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual('project_identifier' in response.data, True)
        self.assertEqual('required parameter' in response.data['project_identifier'][0], True)

        experiment_1_file_list = self._form_complex_list_from_test_file()
        experiment_1_file_list[0]['project_identifier'] = 'second_project'
        response = self.client.post('/rest/files', experiment_1_file_list, format="json")
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual('project_identifier' in response.data, True)
        self.assertEqual('multiple projects' in response.data['project_identifier'][0], True)

    def _assert_directory_parent_dirs(self, project_identifier):
        """
        Check dirs created during the request have parent dirs as expected.
        """
        dirs_dict = {}

        for d in Directory.objects.filter(project_identifier=project_identifier):
            dirs_dict[d.directory_path] = {
                'dir_id': d.id,
                'parent_dir_id': d.parent_directory and d.parent_directory.id or None
            }

        for dir_path, ids in dirs_dict.items():
            if dir_path == '/':
                self.assertEqual(ids['parent_dir_id'], None, 'root dir \'/\' should not have a parent directory')
                continue
            expected_parent_dir_path = dirname(dir_path)
            self.assertEqual(ids['parent_dir_id'], dirs_dict[expected_parent_dir_path]['dir_id'],
                             'parent dir not as expected.')

        return dirs_dict

    def _assert_file_parent_dirs(self, dirs_dict, response):
        """
        Check files have parent dirs as expected.
        """
        for entry in response.data['success']:
            f = entry['object']
            excpected_parent_dir_path = dirname(f['file_path'])
            self.assertEqual(f['parent_directory']['id'], dirs_dict[excpected_parent_dir_path]['dir_id'],
                             'parent dir not as expected.')

    def _form_complex_list_from_test_file(self):
        """
        "complex" list. Notice the leading and trailing whitespace in directories Group_1 and Group_3.
        """
        dir_data = [
            {
                "file_name": "uudehdko.png",
                "file_path": "/project_y_FROZEN/Experiment_1/Group_1    /Results/uudehdko.png",
            },
            {
                "file_name": "uusi.png",
                "file_path": "/project_y_FROZEN/Experiment_1/Group_1    /Results/uusi.png",
            },
            {
                "file_name": "path.png",
                "file_path": "/project_y_FROZEN/Experiment_1/Group_1    /path.png",
            },
            {
                "file_name": "b_path.png",
                "file_path": "/project_y_FROZEN/Experiment_1/b_path.png",
            },
            {
                "file_name": "everything_that_can_go_wrong_will_go_wrong.png",
                "file_path": "/project_y_FROZEN/Experiment_1/Group_2/everything_that_can_go_wrong_will_go_wrong.png",
            },
            {
                "file_name": "pathx.png",
                "file_path": "/project_y_FROZEN/Experiment_1/pathx.png",
            },
            {
                "file_name": "kansio.png",
                "file_path": "/project_y_FROZEN/Experiment_1/Group_1    /Results/Important/kansio.png",
            },
            {
                "file_name": "some.png",
                "file_path": "/project_y_FROZEN/Experiment_1/some.png",
            },
            {
                "file_name": "aa_toka.png",
                "file_path": "/project_y_FROZEN/Experiment_1/aa_toka.png",
            },
            {
                "file_name": "aa_eka.png",
                "file_path": "/project_y_FROZEN/Experiment_1/aa_eka.png",
            },
            {
                "file_name": "kissa.png",
                "file_path": "/project_y_FROZEN/Experiment_1/   Group_3/2017/01/kissa.png",
            },
            {
                "file_name": "ekaa.png",
                "file_path": "/project_y_FROZEN/Experiment_1/ekaa.png",
            },
        ]

        template = self.test_new_data
        template.pop('id', None)
        template.pop('identifier', None)
        template.pop('project_identifier', None)
        template.pop('parent_directory', None)
        template.pop('date_created', None)
        template.pop('date_modified', None)
        template.pop('service_created', None)

        files = []
        for i, d in enumerate(dir_data):
            files.append(deepcopy(template))
            files[-1].update(d, identifier='pid:urn:test:file:%d' % i, project_identifier='project_y')

        return files


class FileApiWriteUpdateTests(FileApiWriteCommon):
    """
    update operations PUT
    """

    def test_update_file(self):
        f = self.client.get('/rest/files/1').data
        f['file_format'] = 'csv'
        response = self.client.put('/rest/files/%s' % f['identifier'], f, format="json")
        self.assertEqual(response.status_code, status.HTTP_200_OK)

    def test_prevent_file_path_update_after_create(self):
        f = self.client.get('/rest/files/1').data
        f['file_path'] = '%s_bak' % f['file_path']
        response = self.client.put('/rest/files/%s' % f['identifier'], f, format="json")
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)

    def test_update_file_error_required_fields(self):
        """
        Field 'project_identifier' is missing, which should result in an error, since PUT
        replaces an object and requires all 'required' fields to be present.
        """
        self.test_new_data.pop('project_identifier')
        response = self.client.put('/rest/files/%s' % self.identifier, self.test_new_data, format="json")

        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual('project_identifier' in response.data.keys(), True,
                         'Error for field \'project_identifier\' is missing from response.data')

    def test_update_file_not_found(self):
        response = self.client.put('/rest/files/doesnotexist', self.test_new_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND)

    def test_update_file_allowed_projects_ok(self):
        f = self.client.get('/rest/files/1').data
        response = self.client.put('/rest/files/%s?allowed_projects=%s' % (f['identifier'], f['project_identifier']),
                                f, format="json")
        self.assertEqual(response.status_code, status.HTTP_200_OK)

    def test_update_file_allowed_projects_fail(self):
        f = self.client.get('/rest/files/1').data
        response = self.client.put('/rest/files/%s?allowed_projects=nopermission' % f['identifier'], f, format="json")
        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN)

    def test_update_file_allowed_projects_not_dict(self):
        f = self.client.get('/rest/files/1').data
        response = self.client.put('/rest/files/%s?allowed_projects=%s' % (f['identifier'], f['project_identifier']),
                                [f], format="json")
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual('json' in response.data['detail'], True, 'Error regarding datatype')

    #
    # update list operations PUT
    #

    def test_file_update_list(self):
        f1 = self.client.get('/rest/files/1').data
        f2 = self.client.get('/rest/files/2').data
        new_file_format = 'changed-format'
        new_file_format_2 = 'changed-format-2'
        f1['file_format'] = new_file_format
        f2['file_format'] = new_file_format_2

        response = self.client.put('/rest/files', [f1, f2], format="json")
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)

        updated_file = File.objects.get(pk=1)
        self.assertEqual(updated_file.file_format, new_file_format)

    def test_file_update_list_error_one_fails(self):
        f1 = self.client.get('/rest/files/1').data
        f2 = self.client.get('/rest/files/2').data
        new_file_format = 'changed-format'
        f1['file_format'] = new_file_format
        # cant be null - should fail
        f2['file_frozen'] = None

        response = self.client.put('/rest/files', [f1, f2], format="json")
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        self.assertEqual(len(response.data['success']), 1, 'success list should be empty')
        self.assertEqual(len(response.data['failed']), 1, 'there should have been one failed element')
        self.assertEqual('file_frozen' in response.data['failed'][0]['errors'], True,
                         'error should be about file_characteristics missing')

        updated_file = File.objects.get(pk=1)
        self.assertEqual(updated_file.file_format, new_file_format)

    def test_file_update_list_error_key_not_found(self):
        f1 = self.client.get('/rest/files/1').data
        f2 = self.client.get('/rest/files/2').data
        new_file_format = 'changed-format'
        new_file_format_2 = 'changed-format-2'
        f1['file_format'] = new_file_format
        f2['file_format'] = new_file_format_2
        # has no lookup key - should fail
        f2.pop('id')
        f2.pop('identifier')

        response = self.client.put('/rest/files', [f1, f2], format="json")
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        self.assertEqual(len(response.data['success']), 1, 'success list should be empty')
        self.assertEqual(len(response.data['failed']), 1, 'there should have been one failed element')
        error_msg_of_failed_row = response.data['failed'][0]['errors']['detail'][0]
        self.assertEqual('identifying keys' in error_msg_of_failed_row, True,
                         'error should be about identifying keys missing')

        updated_file = File.objects.get(pk=1)
        self.assertEqual(updated_file.file_format, new_file_format)

    def test_file_update_list_allowed_projects_ok(self):
        # Both files in project 'project_x'
        f1 = self.client.get('/rest/files/1').data
        f2 = self.client.get('/rest/files/2').data

        response = self.client.put('/rest/files?allowed_projects=project_x,y,z', [f1, f2], format="json")
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)

    def test_file_update_list_allowed_projects_fail(self):
        # Files in projects 'project_x' and 'research_project_112'
        f1 = self.client.get('/rest/files/1').data
        f2 = self.client.get('/rest/files/39').data

        response = self.client.put('/rest/files?allowed_projects=project_x,y,z', [f1, f2], format="json")
        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN, response.data)

    def test_file_update_list_allowed_projects_empty_value(self):
        f1 = self.client.get('/rest/files/1').data
        response = self.client.put('/rest/files?allowed_projects=', [f1], format="json")
        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN, response.data)

    def test_file_update_list_allowed_projects_not_list(self):
        new_data_1 = {}
        new_data_1['identifier'] = "pid:urn:1"
        new_data_1['file_name'] = 'Nice_new_name'

        res = self.client.patch('/rest/files?allowed_projects=y,z,project_x', new_data_1, format="json")
        self.assertEqual(res.status_code, status.HTTP_400_BAD_REQUEST, res.data)


class FileApiWritePartialUpdateTests(FileApiWriteCommon):
    """
    update operations PATCH
    """

    def test_update_file_partial(self):
        new_data = {
            "file_name": "new_file_name",
        }
        response = self.client.patch('/rest/files/%s' % self.identifier, new_data, format="json")

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual('file_name' in response.data.keys(), True)
        self.assertEqual('file_path' in response.data.keys(), True, 'PATCH operation should return full content')
        self.assertEqual(response.data['file_name'], 'new_file_name', 'Field file_name was not updated')

    def test_update_partial_allowed_projects_ok(self):
        new_data = {
            "file_name": "new_file_name",
        }
        response = self.client.patch('/rest/files/%s?allowed_projects=%s' % (self.identifier, self.pidentifier),
            new_data, format="json")

        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        self.assertEqual(response.data['file_name'], 'new_file_name', response.data)

    def test_update_partial_allowed_projects_fail(self):
        new_data = {
            "file_name": "new_file_name",
        }
        response = self.client.patch('/rest/files/%s?allowed_projects=noproject' % self.identifier,
            new_data, format="json")

        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN, response.data)

    def test_update_partial_allowed_projects_not_dict(self):
        new_data = {
            "file_name": "new_file_name",
        }
        response = self.client.patch('/rest/files/%s?allowed_projects=%s' % (self.identifier, self.pidentifier),
            [new_data], format="json")

        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST, response.data)
        self.assertEqual('json' in response.data['detail'], True, 'Error regarding datatype')

    #
    # update list operations PATCH
    #

    def test_file_partial_update_list(self):
        new_project_identifier = 'changed-project-identifier'
        new_project_identifier_2 = 'changed-project-identifier-2'

        test_data = {}
        test_data['id'] = 1
        test_data['project_identifier'] = new_project_identifier

        second_test_data = {}
        second_test_data['id'] = 2
        second_test_data['project_identifier'] = new_project_identifier_2

        response = self.client.patch('/rest/files', [test_data, second_test_data], format="json")
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        self.assertEqual('success' in response.data, True, 'response.data should contain list of changed objects')
        self.assertEqual(len(response.data['success']), 2, 'response.data should contain 2 changed objects')
        self.assertEqual('file_characteristics' in response.data['success'][0]['object'], True,
                         'response.data should contain full objects')

        updated_file = File.objects.get(pk=1)
        self.assertEqual(updated_file.project_identifier, new_project_identifier, 'project_identifier did not update')

    def test_file_partial_update_list_allowed_projects_ok(self):
        new_data_1 = {}
        new_data_1['identifier'] = "pid:urn:1"
        new_data_1['file_name'] = 'Nice_new_name'

        new_data_2 = {}
        new_data_2['identifier'] = "pid:urn:2"
        new_data_2['file_name'] = 'Not_so_nice_name'

        res = self.client.patch('/rest/files?allowed_projects=y,z,project_x', [new_data_1, new_data_2], format="json")
        self.assertEqual(res.status_code, status.HTTP_200_OK, res.data)
        self.assertEqual(res.data['success'][0]['object']['file_name'], 'Nice_new_name', res.data)

    def test_file_partial_update_list_allowed_projects_fail(self):
        # Files in projects 'project_x' and 'research_project_112'
        f1 = self.client.get('/rest/files/1').data
        f2 = self.client.get('/rest/files/39').data

        response = self.client.patch('/rest/files?allowed_projects=project_x,y,z', [f1, f2], format="json")
        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN, response.data)

    def test_file_partial_update_list_allowed_projects_not_list(self):
        new_data_1 = {}
        new_data_1['identifier'] = "pid:urn:1"
        new_data_1['file_name'] = 'Nice_new_name'

        res = self.client.patch('/rest/files?allowed_projects=y,z,project_x', new_data_1, format="json")
        self.assertEqual(res.status_code, status.HTTP_400_BAD_REQUEST, res.data)

    def test_file_partial_update_list_allowed_projects_no_identifier(self):
        new_data_1 = {}
        new_data_1['file_name'] = 'Nice_new_name'

        new_data_2 = {}
        new_data_2['id'] = 23
        new_data_2['file_name'] = 'Not_so_nice_name'

        res = self.client.patch('/rest/files?allowed_projects=y,z,project_x', [new_data_1, new_data_2], format="json")
        self.assertEqual(res.status_code, status.HTTP_400_BAD_REQUEST, res.data)


class FileApiWriteDeleteTests(FileApiWriteCommon):
    #
    #
    #
    # delete apis
    #
    #
    #

    def test_delete_single_file_ok(self):
        dir_count_before = Directory.objects.all().count()
        response = self.client.delete('/rest/files/1')
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual('deleted_files_count' in response.data, True, response.data)
        self.assertEqual(response.data['deleted_files_count'], 1, response.data)
        dir_count_after = Directory.objects.all().count()
        self.assertEqual(dir_count_before, dir_count_after, 'no dirs should have been deleted')
        deleted_file = File.objects_unfiltered.get(pk=1)
        self._check_project_root_byte_size_and_file_count(deleted_file.project_identifier)
        self.assertEqual(deleted_file.date_modified, deleted_file.file_deleted, 'date_modified should be updated')

    def test_delete_single_file_ok_destroy_leading_dirs(self):
        project_identifier = 'project_z'
        test_data = deepcopy(self.test_new_data)
        test_data['file_path'] = '/project_z/some/path/here/%s' % test_data['file_name']
        test_data['project_identifier'] = project_identifier
        test_data['identifier'] = 'abc123'
        response = self.client.post('/rest/files', test_data, format='json')
        self.assertEqual(Directory.objects.filter(project_identifier=project_identifier).exists(), True)

        response = self.client.delete('/rest/files/%s' % response.data['id'])
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual('deleted_files_count' in response.data, True, response.data)
        self.assertEqual(response.data['deleted_files_count'], 1, response.data)

        self.assertEqual(Directory.objects.filter(project_identifier=project_identifier).exists(), False)

    def test_delete_single_file_404(self):
        response = self.client.delete('/rest/files/doesnotexist')
        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND)

    def test_bulk_delete_files_identifiers_not_found(self):
        """
        A bulk delete request to /files, but any of the identifiers provided are not found.
        Should return 404.
        """
        identifiers = ['nope', 'doesnotexist', 'stillno']
        response = self.client.delete('/rest/files', identifiers, format="json")
        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND, response.data)

    def test_bulk_delete_files_some_identifiers_not_found(self):
        """
        A bulk delete request to /files, but some of the identifiers provided are not found.
        Should be ok, delete those files that are found. Assumably those identifiers that were
        not found did not exist anyway, therefore no harm is done.
        """
        identifiers = ['nope', 'doesnotexist', 'stillno']
        identifiers.append(File.objects.get(pk=1).identifier)
        response = self.client.delete('/rest/files', identifiers, format="json")
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        removed = File.objects_unfiltered.get(pk=1).removed
        self.assertEqual(removed, True, 'file should have been removed')
        self._check_project_root_byte_size_and_file_count(File.objects_unfiltered.get(pk=1).project_identifier)

    def test_bulk_delete_files_in_single_directory_1(self):
        """
        A bulk delete request to /files, where the list of files does not contain a full
        directory and all its sub-directories.

        Only the files requested should be deleted, while leaving the rest of the directory
        tree intact.
        """
        all_files_count_before = File.objects.all().count()
        file_ids = [f.id for f in Directory.objects.get(pk=3).files.all()]

        response = self.client.delete('/rest/files', file_ids, format="json")
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)

        all_files_count_after = File.objects.all().count()
        self.assertEqual(all_files_count_after, all_files_count_before - len(file_ids))

    def test_bulk_delete_files_in_single_directory_2(self):
        """
        Same as above, but target another directory.
        """
        all_files_count_before = File.objects.all().count()
        file_ids = [f.id for f in Directory.objects.get(pk=4).files.all()]

        response = self.client.delete('/rest/files', file_ids, format="json")
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)

        all_files_count_after = File.objects.all().count()
        self.assertEqual(all_files_count_after, all_files_count_before - len(file_ids))

    def test_bulk_delete_file_list_one_file_id_missing(self):
        """
        Otherwise complete set of files, but from one dir one file is missing.
        Should leave the one file intact, while preserving the directory tree.
        """
        all_files_count_before = File.objects.filter(project_identifier='project_x').count()
        file_ids = [f.id for f in File.objects.filter(project_identifier='project_x')]

        # everything except the last file should be removed
        file_ids.pop()

        response = self.client.delete('/rest/files', file_ids, format="json")
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)

        all_files_after = File.objects.filter(project_identifier='project_x')
        self.assertEqual(all_files_after.count(), all_files_count_before - len(file_ids))

        expected_dirs_count = self._count_dirs_from_path(all_files_after[0].file_path)
        actual_dirs_count = Directory.objects.filter(project_identifier='project_x').count()
        self.assertEqual(actual_dirs_count, expected_dirs_count)

    def test_bulk_delete_files_from_root(self):
        """
        Delete all files in project_x. The top-most dir should be /project_x_FROZEN/Experiment_X,
        so the whole tree should end up being deleted.
        """
        files_to_remove_count = 20
        file_ids = File.objects.filter(project_identifier='project_x').values_list('id', flat=True)
        self.assertEqual(len(file_ids), files_to_remove_count)

        response = self.client.delete('/rest/files', file_ids, format="json")
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        self.assertEqual(response.data.get('deleted_files_count', None), files_to_remove_count, response.data)

        self._assert_files_available_and_removed('project_x', 0, files_to_remove_count)
        self.assertEqual(Directory.objects_unfiltered.filter(project_identifier='project_x').count(), 0,
                         'all dirs should have been permanently removed')

    def test_bulk_delete_sub_directory_1(self):
        """
        Delete from /project_x_FROZEN/Experiment_X/Phase_1, which should remove
        only 15 files.
        """
        files_to_remove_count = 15
        file_ids = [f.id for f in Directory.objects.get(pk=4).files.all()]
        file_ids += [f.id for f in Directory.objects.get(pk=6).files.all()]
        self.assertEqual(len(file_ids), files_to_remove_count)

        response = self.client.delete('/rest/files', file_ids, format="json")
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        self.assertEqual(response.data.get('deleted_files_count', None), files_to_remove_count, response.data)

        self._assert_files_available_and_removed('project_x', 5, files_to_remove_count)

        # these dirs should still be left:
        # /
        # /project_x_FROZEN
        # /project_x_FROZEN/Experiment_X (has 5 files)
        self.assertEqual(Directory.objects.filter(project_identifier='project_x').count(), 3)

    def test_bulk_delete_sub_directory_2(self):
        """
        Delete from /project_x_FROZEN/Experiment_X/Phase_1/2017/01, which should
        remove only 10 files.
        """
        files_to_remove_count = 10
        file_ids = [f.id for f in Directory.objects.get(pk=6).files.all()]
        self.assertEqual(len(file_ids), files_to_remove_count)

        response = self.client.delete('/rest/files', file_ids, format="json")
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        self.assertEqual(response.data.get('deleted_files_count', None), files_to_remove_count, response.data)

        self._assert_files_available_and_removed('project_x', 10, files_to_remove_count)

        # these dirs should still be left:
        # /
        # /project_x_FROZEN
        # /project_x_FROZEN/Experiment_X (5 files)
        # /project_x_FROZEN/Experiment_X/Phase_1 (5 files)
        self.assertEqual(Directory.objects.filter(project_identifier='project_x').count(), 4)

        # /project_x_FROZEN/Experiment_X/Phase_1/2017 <- this dir should be deleted, since
        # it only contained the 01-dir, which we specifically targeted for deletion
        self.assertEqual(Directory.objects.filter(
            project_identifier='project_x',
            directory_path='/project_x_FROZEN/Experiment_X/Phase_1/2017'
        ).count(), 0, 'dir should have been deleted')

    def _assert_files_available_and_removed(self, project_identifier, available, removed):
        """
        After deleting files, check qty of files retrievable by usual means is as expected,
        and qty of files retrievable from objects_unfiltered with removed=True is as expected.
        """
        self.assertEqual(File.objects.filter(project_identifier=project_identifier).count(), available,
                         'files should not be retrievable from removed=False scope')
        self.assertEqual(File.objects_unfiltered.filter(project_identifier=project_identifier, removed=True).count(),
                         removed,
                         'files should be retrievable from removed=True scope')

    def test_deleting_files_deprecates_datasets(self):
        for cr in CatalogRecord.objects.filter(deprecated=True):
            # ensure later assert is correct
            cr.deprecated = False
            cr.force_save()

        datasets_with_file = CatalogRecord.objects.filter(files__id=1).count()
        response = self.client.delete('/rest/files/1')
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(CatalogRecord.objects.filter(deprecated=True).count(), datasets_with_file)


class FileApiWriteRestoreTests(FileApiWriteCommon):

    def test_restore_files_ok(self):
        """
        Restore a few deleted files from directories, that still contain other files.
        Restored files should be appended to previously existing files.
        """
        response = self.client.delete('/rest/files/1')
        response = self.client.delete('/rest/files/2')
        response = self.client.delete('/rest/files/3')
        self.assertEqual(response.status_code, status.HTTP_200_OK)

        deleted_files = File.objects_unfiltered.filter(pk__in=[1, 2, 3]) \
            .values('identifier', 'parent_directory_id')

        response = self.client.post('/rest/files/restore', [f['identifier'] for f in deleted_files], format='json')
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual('restored_files_count' in response.data, True, response.data)
        self.assertEqual(response.data['restored_files_count'], 3, response.data)

        # ensure restored files are using previously existing directories
        old_parent_dirs = { f['parent_directory_id'] for f in deleted_files }
        files = File.objects.filter(pk__in=[1, 2, 3])
        for f in files:
            self.assertEqual(f.file_deleted, None)
            self.assertEqual(f.user_modified, None)
            self.assertEqual(f.parent_directory_id in old_parent_dirs, True)

    def test_restore_files_recreate_missing_directories(self):
        """
        Restore an entire project. Files should have new directories.
        """
        proj = File.objects.get(pk=1).project_identifier

        response = self.client.get('/rest/files?project_identifier=%s&fields=identifier&no_pagination=true'
            % proj, format='json')
        file_identifiers = [ f['identifier'] for f in response.data ]

        self.client.delete('/rest/files', file_identifiers, format='json')

        deleted_directory_ids = File.objects_unfiltered.filter(identifier__in=file_identifiers) \
            .values_list('parent_directory_id', flat=True)
        old_parent_dirs = { id for id in deleted_directory_ids }

        response = self.client.post('/rest/files/restore', file_identifiers, format='json')
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual('restored_files_count' in response.data, True, response.data)
        self.assertEqual(response.data['restored_files_count'], len(file_identifiers), response.data)

        # ensure restored files are using new directories
        files = File.objects.filter(identifier__in=file_identifiers)
        for f in files:
            self.assertEqual(f.parent_directory_id in old_parent_dirs, False)

    def test_check_parameter_is_string_list(self):
        response = self.client.post('/rest/files/restore', ['a', 'b', 1], format='json')
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)

    def test_check_files_belong_to_one_project(self):
        f1 = File.objects_unfiltered.get(pk=1)
        f2 = File.objects_unfiltered.filter().exclude(project_identifier=f1.project_identifier).first()
        response = self.client.delete('/rest/files/%d' % f1.id)
        response = self.client.delete('/rest/files/%d' % f2.id)
        response = self.client.post('/rest/files/restore', [ f1.identifier, f2.identifier ], format='json')
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)


class FileApiWriteXmlTests(FileApiWriteCommon):
    """
    /files/pid/xml related tests
    """

    def test_xml_api(self):
        content_type = 'application/xml'
        data = '<?xml version="1.0" encoding="utf-8"?><root><stuffia>tauhketta yeah</stuffia></root>'

        # create
        response = self.client.post('/rest/files/1/xml?namespace=breh', data, content_type=content_type)
        self.assertEqual(response.status_code in (200, 201, 204), True)
        self.assertEqual('<?xml' in response.data, True)

        response = self.client.post('/rest/files/1/xml?namespace=bruh', data, content_type=content_type)
        self.assertEqual(response.status_code in (200, 201, 204), True)
        self.assertEqual('<?xml' in response.data, True)

        # get list
        response = self.client.get('/rest/files/1/xml', content_type=content_type)
        self.assertEqual(response.status_code in (200, 201, 204), True)
        self.assertEqual(isinstance(response.data, list), True, response.data)
        self.assertEqual('breh' in response.data, True)

        # get single
        response = self.client.get('/rest/files/1/xml?namespace=breh', content_type=content_type)
        self.assertEqual(response.status_code in (200, 201, 204), True)
        self.assertEqual('<?xml' in response.data, True)

        # update
        data = '<?xml version="1.0" encoding="utf-8"?><root><stuffia>updated stuff</stuffia></root>'
        response = self.client.put('/rest/files/1/xml?namespace=breh', data, content_type=content_type)
        self.assertEqual(response.status_code in (200, 201, 204), True)

        # get updated again
        response = self.client.get('/rest/files/1/xml?namespace=breh', content_type=content_type, )
        self.assertEqual('updated stuff' in response.data, True)

        # delete
        response = self.client.delete('/rest/files/1/xml?namespace=breh', data, content_type=content_type)
        self.assertEqual(response.status_code in (200, 201, 204), True)

        response = self.client.delete('/rest/files/1/xml?namespace=bruh', data, content_type=content_type)
        self.assertEqual(response.status_code in (200, 201, 204), True)

        # get list
        response = self.client.get('/rest/files/1/xml', content_type=content_type)
        self.assertEqual(response.status_code in (200, 201, 204), True)


class FileApiWriteEndUserAccess(FileApiWriteCommon):

    def setUp(self):
        super().setUp()
        self.token = get_test_oidc_token()
        self._mock_token_validation_succeeds()

    @responses.activate
    def test_user_cant_create_files(self):
        '''
        Ensure users are unable to create new files.
        '''

        # ensure user belongs to same project
        self.token['group_names'].append('fairdata:IDA01:%s' % self.test_new_data['project_identifier'])
        self._use_http_authorization(method='bearer', token=self.token)

        response = self.client.post('/rest/files', self.test_new_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN)

    @responses.activate
    def test_user_can_only_update_permitted_file_fields(self):
        '''
        Ensure users are only able to modify permitted fields.
        '''
        # ensure user belongs to same project
        proj = File.objects.get(pk=1).project_identifier
        self.token['group_names'].append('fairdata:IDA01:%s' % proj)
        self._use_http_authorization(method='bearer', token=self.token)

        response = self.client.get('/rest/files/1', format="json")
        file = response.data
        original_file = deepcopy(file)
        file['byte_size'] = 200
        file['checksum']['value'] = 'changed'
        file['parent_directory'] = 1
        file['file_frozen'] = '3' + file['file_frozen'][1:]
        file['file_format'] = 'changed'
        file['file_name'] = 'changed'
        file['file_path'] = '/oh/no'
        file['file_storage'] = 2
        file['file_uploaded'] = '3' + file['file_uploaded'][1:]
        file['identifier'] = 'changed'
        file['open_access'] = True
        file['project_identifier'] = 'changed'
        file['service_modified'] = 'changed'
        file['service_created'] = 'changed'
        file['removed'] = True

        # the only field that should be changed
        file['file_characteristics'] = { 'title': 'new title'}

        response = self.client.put('/rest/files/1', file, format="json")
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        self.assertEqual(response.data['file_characteristics']['title'], 'new title', response.data)

        for key, value in response.data.items():
            try:
                if key in ('date_modified', 'file_modified'):
                    # these fields are changed by metax
                    continue
                elif key == 'file_characteristics':
                    # the field that should have been changed by the user
                    self.assertNotEqual(original_file[key], response.data[key])
                else:
                    # must not have changed
                    self.assertEqual(original_file[key], response.data[key])
            except KeyError as e:
                if e.args[0] == 'user_modified':
                    # added by metax
                    continue
                raise

    @responses.activate
    def test_user_can_update_files_in_their_projects(self):
        '''
        Ensure users can edit files in projects they are a member of.
        '''
        proj = File.objects.only('project_identifier').get(pk=1).project_identifier

        response = self.client.get('/rest/files?project_identifier=%s' % proj,
            format="json")

        file = response.data['results'][0]

        self.token['group_names'].append('fairdata:IDA01:%s' % proj)
        self._use_http_authorization(method='bearer', token=self.token)

        response = self.client.put('/rest/files/%s' % file['id'], file, format="json")
        self.assertEqual(response.status_code, status.HTTP_200_OK)

        response = self.client.put('/rest/files', [file], format="json")
        self.assertEqual(response.status_code, status.HTTP_200_OK)

    @responses.activate
    def test_user_cant_update_files_in_others_projects(self):
        '''
        Ensure users can not edit files in projects they are not a member of.
        '''
        proj = File.objects.only('project_identifier').get(pk=1).project_identifier

        response = self.client.get('/rest/files?project_identifier=%s' % proj,
            format="json")

        file = response.data['results'][0]

        self.token['group_names'] = ['no_files_for_this_project']
        self._use_http_authorization(method='bearer', token=self.token)

        response = self.client.put('/rest/files/%s' % file['id'], file, format="json")
        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN)

        response = self.client.put('/rest/files', [file], format="json")
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)


class FileApiWriteDryrunTest(FileApiWriteCommon):

    """
    Test query param ?dryrun=bool separately for post /rest/files api, due to special
    behavior in POST /rest/files.

    For other apis, the common test case is among views/common/write tests.
    """

    def test_dryrun(self):
        """
        Ensure query parameter ?dryrun=true returns same result as they normally would, but
        changes made during the request do not get saved in the db.
        """
        response = self.client.post('/rest/files?what&dryrun=true&other', self.test_new_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)
        self.assertEqual('id' in response.data, True)
        found = File.objects.filter(pk=response.data['id']).exists()
        self.assertEqual(found, False, 'file should not get truly created when using parameter dryrun')
