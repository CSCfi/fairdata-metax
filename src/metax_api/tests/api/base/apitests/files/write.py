from copy import deepcopy
from os.path import dirname

from django.core.management import call_command
from rest_framework import status
from rest_framework.test import APITestCase

from metax_api.models import Directory, File, FileStorage
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
            "file_name": "new_file_1",
            "file_path": from_test_data['file_path'].replace('/some/path', '/some/other_path'),
            "identifier": "urn:nbn:fi:csc-ida201401200000000001",
            "file_storage": self._get_object_from_test_data('filestorage', requested_index=0)
        })
        from_test_data['file_path'] = from_test_data['file_path'].replace('/Experiment_X/', '/test/path/')
        from_test_data['project_identifier'] = 'test_project_identifier'
        return from_test_data

    def _get_second_new_test_data(self):
        from_test_data = self._get_new_test_data()
        from_test_data.update({
            "file_name": "new_file_2",
            "identifier": "urn:nbn:fi:csc-ida201401200000000002",
        })
        return from_test_data


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

    def test_create_file_error_identifier_exists(self):
        # first ok
        response = self.client.post('/rest/files', self.test_new_data, format="json")
        # second should give error
        response = self.client.post('/rest/files', self.test_new_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual('identifier' in response.data.keys(), True, 'The error should be about an already existing identifier')

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
        self.assertEqual('file_characteristics' in response.data.keys(), True, 'The error should concern the field file_characteristics')
        self.assertEqual('metadata_modified' in response.data['file_characteristics'][0], True, 'The error should contain the name of the erroneous field')
        self.assertEqual('Json path:' in response.data['file_characteristics'][0], True, 'The error should contain the json path')

    def test_create_file_dont_allow_file_storage_fields_update(self):
        self.test_new_data['identifier'] = 'urn:nbn:fi:csc-thisisanewurn'
        original_title = self.test_new_data['file_storage']['file_storage_json']['title']
        self.test_new_data['file_storage']['file_storage_json']['title'] = 'new title'

        response = self.client.post('/rest/files', self.test_new_data, format="json")

        self.assertEqual(response.status_code, status.HTTP_201_CREATED)
        self.assertEqual(response.data['file_storage']['file_storage_json']['title'], original_title)
        file_storage = FileStorage.objects.get(pk=response.data['file_storage']['id'])
        self.assertEqual(file_storage.file_storage_json['title'], original_title)

    #
    # create list operations
    #

    def test_create_file_list(self):
        newly_created_file_name = 'newly_created_file_name'
        self.test_new_data['file_name'] = newly_created_file_name
        self.test_new_data['identifier'] = 'urn:nbn:fi:csc-thisisanewurn'
        self.second_test_new_data['identifier'] = 'urn:nbn:fi:csc-thisisanewurnalso'

        response = self.client.post('/rest/files', [self.test_new_data, self.second_test_new_data], format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)
        self.assertEqual('success' in response.data.keys(), True)
        self.assertEqual('failed' in response.data.keys(), True)
        self.assertEqual('object' in response.data['success'][0].keys(), True)
        self.assertEqual(len(response.data['success']), 2)
        self.assertEqual(len(response.data['failed']), 0)

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
        self.assertEqual('identifier' in response.data['failed'][0]['errors'], True, 'The error should have been about an already existing identifier')

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

    def test_create_file_hierarchy_from_file_list_with_no_existing_files(self):
        """
        Create a file hierarchy for a project which has 0 files or directories created previously.

        Here, a directory /project_y_FROZEN/Experiment_1 is "frozen"
        """
        experiment_1_file_list = self._form_complex_list_from_test_file()

        response = self.client.post('/rest/files', experiment_1_file_list, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED)
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
            f['file_path'] = f['file_path'].replace('/project_y_FROZEN/Experiment_1', '/project_y_FROZEN/Experiment_2/Phase_1/Data')
            f['identifier'] = '%s-%d' % (f['file_path'], i)

        response = self.client.post('/rest/files', experiment_2_file_list, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED)
        self.assertEqual('success' in response.data.keys(), True)
        self.assertEqual(len(response.data['success']), 12)
        self.assertEqual(len(response.data['failed']), 0)

        dirs_dict = self._assert_directory_parent_dirs('project_y')
        self._assert_file_parent_dirs(dirs_dict, response)

    def test_create_file_hierarchy_error_file_list_has_invalid_data(self):
        """
        If even one file is missing file_path or project_identifier, the
        request is immediately terminated.
        """
        experiment_1_file_list = self._form_complex_list_from_test_file()
        experiment_1_file_list[0].pop('file_path')
        experiment_1_file_list[0].pop('project_identifier')

        response = self.client.post('/rest/files', experiment_1_file_list, format="json")
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual('file_path' in response.data, True)
        self.assertEqual('project_identifier' in response.data, True)
        self.assertEqual('required parameter' in response.data['file_path'][0], True)
        self.assertEqual('required parameter' in response.data['project_identifier'][0], True)

    def _assert_directory_parent_dirs(self, project_identifier):
        """
        Check dirs created during the request have parent dirs as expected.
        """
        dirs_dict = {}

        for d in Directory.objects.filter(project_identifier=project_identifier):
            dirs_dict[d.directory_path] = { 'dir_id': d.id, 'parent_dir_id': d.parent_directory and d.parent_directory.id or None }

        for dir_path, ids in dirs_dict.items():
            if dir_path.endswith('FROZEN'):
                self.assertEqual(ids['parent_dir_id'], None, 'FROZEN root dir should not have a parent directory')
                continue
            expected_parent_dir_path = dirname(dir_path)
            self.assertEqual(ids['parent_dir_id'], dirs_dict[expected_parent_dir_path]['dir_id'], 'parent dir not as expected.')

        return dirs_dict

    def _assert_file_parent_dirs(self, dirs_dict, response):
        """
        Check files have parent dirs as expected.
        """
        for entry in response.data['success']:
            f = entry['object']
            excpected_parent_dir_path = dirname(f['file_path'])
            self.assertEqual(f['parent_directory']['id'], dirs_dict[excpected_parent_dir_path]['dir_id'], 'parent dir not as expected.')

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
                "file_path": "/project_y_FROZEN/Experiment_1/image.png",
            },
            {
                "file_name": "path.png",
                "file_path": "/project_y_FROZEN/Experiment_1/Group_2/path.png",
            },
            {
                "file_name": "pathx.png",
                "file_path": "/project_y_FROZEN/Experiment_1/other_image.png",
            },
            {
                "file_name": "kansio.png",
                "file_path": "/project_y_FROZEN/Experiment_1/Group_1/Results/Important/important.png",
            },
            {
                "file_name": "some.png",
                "file_path": "/project_y_FROZEN/Experiment_1/an_image.png",
            },
            {
                "file_name": "aa_toka.png",
                "file_path": "/project_y_FROZEN/Experiment_1/an_image_also.png",
            },
            {
                "file_name": "aa_eka.png",
                "file_path": "/project_y_FROZEN/Experiment_1/image_as_well.png",
            },
            {
                "file_name": "kissa.png",
                "file_path": "/project_y_FROZEN/Experiment_1/Group_3/kissa.png",
            },
            {
                "file_name": "ekaa.png",
                "file_path": "/project_y_FROZEN/Experiment_1/an_image_layeth_here.png",
            }
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
        response = self.client.put('/rest/files/%s' % self.identifier, self.test_new_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_204_NO_CONTENT)
        self.assertEqual(len(response.data.keys()), 0, 'Returned dict should be empty')

    def test_update_file_error_required_fields(self):
        """
        Field 'project_identifier' is missing, which should result in an error, since PUT
        replaces an object and requires all 'required' fields to be present.
        """
        self.test_new_data.pop('project_identifier')
        response = self.client.put('/rest/files/%s' % self.identifier, self.test_new_data, format="json")

        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual('project_identifier' in response.data.keys(), True, 'Error for field \'project_identifier\' is missing from response.data')

    def test_update_file_dont_allow_file_storage_fields_update(self):
        original_title = self.test_new_data['file_storage']['file_storage_json']['title']
        self.test_new_data['file_storage']['file_storage_json']['title'] = 'new title'

        response = self.client.put('/rest/files/%s' % self.identifier, self.test_new_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_204_NO_CONTENT)
        file_storage = FileStorage.objects.get(pk=self.test_new_data['file_storage']['id'])
        self.assertEqual(file_storage.file_storage_json['title'], original_title)

    def test_update_file_not_found(self):
        response = self.client.put('/rest/files/doesnotexist', self.test_new_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND)

    #
    # update list operations PUT
    #

    def test_file_update_list(self):
        new_project_identifier = 'changed-project-identifier'
        new_project_identifier_2 = 'changed-project-identifier-2'
        self.test_new_data['id'] = 1
        self.test_new_data['project_identifier'] = new_project_identifier

        self.second_test_new_data['id'] = 2
        self.second_test_new_data['project_identifier'] = new_project_identifier_2

        response = self.client.put('/rest/files', [self.test_new_data, self.second_test_new_data], format="json")
        self.assertEqual(response.status_code, status.HTTP_204_NO_CONTENT, response.data)
        self.assertEqual(response.data, {}, 'response.data should be empty object, since all operations succeeded')

        updated_file = File.objects.get(pk=1)
        self.assertEqual(updated_file.project_identifier, new_project_identifier, 'project_identifier did not update')

    def test_file_update_list_error_one_fails(self):
        new_project_identifier = 'changed-project-identifier'
        self.test_new_data['id'] = 1
        self.test_new_data['project_identifier'] = new_project_identifier

        self.second_test_new_data['id'] = 2
        # cant be null - should fail
        self.second_test_new_data['file_frozen'] = None

        response = self.client.put('/rest/files', [self.test_new_data, self.second_test_new_data], format="json")
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        self.assertEqual(len(response.data['success']), 0, 'success list should be empty')
        self.assertEqual(len(response.data['failed']), 1, 'there should have been one failed element')
        self.assertEqual('file_frozen' in response.data['failed'][0]['errors'], True, 'error should be about file_characteristics missing')

        updated_file = File.objects.get(pk=1)
        self.assertEqual(updated_file.project_identifier, new_project_identifier, 'project_identifier did not update for first item')

    def test_file_update_list_error_key_not_found(self):
        new_project_identifier = 'changed-project-identifier'
        new_project_identifier_2 = 'changed-project-identifier-2'
        self.test_new_data['id'] = 1
        self.test_new_data['project_identifier'] = new_project_identifier

        # has no lookup key - should fail
        self.second_test_new_data.pop('id', False)
        self.second_test_new_data.pop('identifier')
        self.second_test_new_data['project_identifier'] = new_project_identifier_2

        response = self.client.put('/rest/files', [self.test_new_data, self.second_test_new_data], format="json")
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        self.assertEqual(len(response.data['success']), 0, 'success list should be empty')
        self.assertEqual(len(response.data['failed']), 1, 'there should have been one failed element')
        error_msg_of_failed_row = response.data['failed'][0]['errors']['detail'][0]
        self.assertEqual('identifying keys' in error_msg_of_failed_row, True, 'error should be about identifying keys missing')

        updated_file = File.objects.get(pk=1)
        self.assertEqual(updated_file.project_identifier, new_project_identifier, 'project_identifier did not update for first item')


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
        self.assertEqual('file_characteristics' in response.data['success'][0]['object'], True, 'response.data should contain full objects')

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

    def test_delete_file(self):
        url = '/rest/files/%s' % self.identifier
        response = self.client.delete(url)
        self.assertEqual(response.status_code, status.HTTP_204_NO_CONTENT)
        response = self.client.get(url)
        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND)

        try:
            deleted_file = File.objects_unfiltered.get(identifier=self.identifier)
        except File.DoesNotExist:
            raise Exception('Deleted file should not be deleted from the db, but marked as removed')

        self.assertEqual(deleted_file.removed, True)
        self.assertEqual(deleted_file.file_name, self.file_name)


class FileApiWriteXmlTests(FileApiWriteCommon):

    """
    /files/pid/xml related tests
    """

    def test_xml_api(self):
        headers = { 'Content-type': 'application/xml' }
        data = '<?xml version="1.0" encoding="utf-8"?><root><stuffia>tauhketta yeah</stuffia></root>'

        # create
        response = self.client.post('/rest/files/1/xml?namespace=breh', data, headers=headers, format='json')
        self.assertEqual(response.status_code in (200, 201, 204), True)
        self.assertEqual('<?xml' in response.data, True)

        response = self.client.post('/rest/files/1/xml?namespace=bruh', data, headers=headers, format='json')
        self.assertEqual(response.status_code in (200, 201, 204), True)
        self.assertEqual('<?xml' in response.data, True)

        # get list
        response = self.client.get('/rest/files/1/xml', headers=headers, format='json')
        self.assertEqual(response.status_code in (200, 201, 204), True)
        self.assertEqual(isinstance(response.data, list), True, response.data)
        self.assertEqual('breh' in response.data, True)

        # get single
        response = self.client.get('/rest/files/1/xml?namespace=breh', headers=headers, format='json')
        self.assertEqual(response.status_code in (200, 201, 204), True)
        self.assertEqual('<?xml' in response.data, True)

        # update
        data = '<?xml version="1.0" encoding="utf-8"?><root><stuffia>updated stuff</stuffia></root>'
        response = self.client.put('/rest/files/1/xml?namespace=breh', data, headers=headers, format='json')
        self.assertEqual(response.status_code in (200, 201, 204), True)

        # get updated again
        response = self.client.get('/rest/files/1/xml?namespace=breh', headers=headers, format='json')
        self.assertEqual('updated stuff' in response.data, True)

        # delete
        response = self.client.delete('/rest/files/1/xml?namespace=breh', data, headers=headers, format='json')
        self.assertEqual(response.status_code in (200, 201, 204), True)

        response = self.client.delete('/rest/files/1/xml?namespace=bruh', data, headers=headers, format='json')
        self.assertEqual(response.status_code in (200, 201, 204), True)

        # get list
        response = self.client.get('/rest/files/1/xml', headers=headers, format='json')
        self.assertEqual(response.status_code in (200, 201, 204), True)
