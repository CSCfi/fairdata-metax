from copy import deepcopy
from os.path import dirname

from django.db.models import Sum
from django.core.management import call_command
from rest_framework import status
from rest_framework.test import APITestCase

from metax_api.models import Directory, File
from metax_api.tests.utils import test_data_file_path, TestClassUtils

d = print


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


class FileApiWriteCreateTests(FileApiWriteCommon):
    #
    #
    #
    # create apis
    #
    #
    #

    def test_create_file(self):
        newly_created_file_name = 'newly_created_file_name'
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
        self.assertEqual('identifier' in response.data.keys(), True,
                         'The error should be about an already existing identifier')

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
        f['file_path'] = '/project_y_FROZEN/Experiment_1/path/of/lonely/file/%s' % f['file_name']
        f['identifier'] = '%s-111' % f['file_path']

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
        "complex" list
        """
        dir_data = [
            {
                "file_name": "uudehdko.png",
                "file_path": "/project_y_FROZEN/Experiment_1/Group_1/Results/uudehdko.png",
            },
            {
                "file_name": "uusi.png",
                "file_path": "/project_y_FROZEN/Experiment_1/Group_1/Results/uusi.png",
            },
            {
                "file_name": "path.png",
                "file_path": "/project_y_FROZEN/Experiment_1/Group_1/path.png",
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
                "file_path": "/project_y_FROZEN/Experiment_1/Group_1/Results/Important/kansio.png",
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
                "file_path": "/project_y_FROZEN/Experiment_1/Group_3/2017/01/kissa.png",
            },
            {
                "file_name": "ekaa.png",
                "file_path": "/project_y_FROZEN/Experiment_1/ekaa.png",
            },
        ]

        files = []

        for i, d in enumerate(dir_data):
            files.append(deepcopy(self.test_new_data))
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
        self._check_project_root_byte_size_and_file_count(File.objects_unfiltered.get(pk=1).project_identifier)

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
