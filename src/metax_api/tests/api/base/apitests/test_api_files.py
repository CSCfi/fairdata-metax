from django.core.management import call_command
from rest_framework import status
from rest_framework.test import APITestCase

from metax_api.models import File, FileStorage
from metax_api.tests.utils import test_data_file_path, TestClassUtils

d = print

class FileApiReadTestV1(APITestCase, TestClassUtils):

    """
    Fields defined in FileSerializer
    """
    file_field_names = (
        'id',
        'byte_size',
        'checksum_algorithm',
        'checksum_checked',
        'checksum_value',
        'download_url',
        'file_deleted',
        'file_frozen',
        'file_format',
        'file_modified',
        'file_name',
        'file_path',
        'file_storage',
        'file_uploaded',
        'identifier',
        'file_characteristics',
        'file_characteristics_extension',
        'open_access',
        'project_identifier',
        'replication_path',
        'modified_by_user_id',
        'modified_by_api',
        'created_by_user_id',
        'created_by_api',
    )

    @classmethod
    def setUpClass(cls):
        """
        Loaded only once for test cases inside this class.
        """
        call_command('loaddata', test_data_file_path, verbosity=0)
        super(FileApiReadTestV1, cls).setUpClass()

    def setUp(self):
        file_from_test_data = self._get_object_from_test_data('file')
        self.identifier = file_from_test_data['identifier']
        self.pk = file_from_test_data['id']

    def test_read_file_list(self):
        response = self.client.get('/rest/files')
        self.assertEqual(response.status_code, status.HTTP_200_OK)

    def test_read_file_details_by_pk(self):
        response = self.client.get('/rest/files/%s' % self.pk)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(hasattr(response, 'data'), True, 'Request response object is missing attribute \'data\'')
        self.assertEqual('file_name' in response.data.keys(), True)
        self.assertEqual(response.data['identifier'], self.identifier)

    def test_read_file_details_by_identifier(self):
        response = self.client.get('/rest/files/%s' % self.identifier)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(hasattr(response, 'data'), True, 'Request response object is missing attribute \'data\'')
        self.assertEqual('file_name' in response.data.keys(), True)
        self.assertEqual(response.data['identifier'], self.identifier)

    def test_read_file_details_not_found(self):
        response = self.client.get('/rest/files/shouldnotexist')
        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND)

    def test_model_fields_as_expected(self):
        response = self.client.get('/rest/files/%s' % self.identifier)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        actual_received_fields = [field for field in response.data.keys()]
        self._test_model_fields_as_expected(self.file_field_names, actual_received_fields)


class FileApiWriteTestV1(APITestCase, TestClassUtils):

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

        self.assertEqual(response.status_code, status.HTTP_201_CREATED)
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
        self.assertEqual('field: metadata_modified' in response.data['file_characteristics'][0], True, 'The error should contain the name of the erroneous field')

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
        self.assertEqual(response.status_code, status.HTTP_201_CREATED)
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

    #
    #
    #
    # update apis
    #
    #
    #

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

    def test_update_file_partial(self):
        new_data = {
            "file_name": "new_file_name",
        }
        response = self.client.patch('/rest/files/%s' % self.identifier, new_data, format="json")

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual('file_name' in response.data.keys(), True)
        self.assertEqual('file_path' in response.data.keys(), True, 'PATCH operation should return full content')
        self.assertEqual(response.data['file_name'], 'new_file_name', 'Field file_name was not updated')

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
        self.second_test_new_data['file_characteristics'] = None

        response = self.client.put('/rest/files', [self.test_new_data, self.second_test_new_data], format="json")
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        self.assertEqual(len(response.data['success']), 0, 'success list should be empty')
        self.assertEqual(len(response.data['failed']), 1, 'there should have been one failed element')
        self.assertEqual('file_characteristics' in response.data['failed'][0]['errors'], True, 'error should be about file_characteristics missing')

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

    def _get_new_test_data(self):
        return {
            "open_access": False,
            "file_modified": "2017-05-23T14:41:59.507392Z",
            "created_by_api": "2017-05-23T10:07:22.559656Z",
            "checksum_value": "habeebit",
            "project_identifier": "my group",
            "identifier": "urn:nbn:fi:csc-ida201401200000000001",
            "download_url": "http://some.url.csc.fi/0000000001",
            "file_name": "new_file_name",
            "file_format": "html/text",
            "file_path": "/some/path/",
            "byte_size": 0,
            "modified_by_api": "2017-05-23T10:07:22.559656Z",
            "checksum_algorithm": "sha2",
            "replication_path": "empty",
            "checksum_checked": None,
            "file_storage": self._get_object_from_test_data('filestorage', requested_index=0),
            "file_characteristics": {
                "application_name": "Application Name",
                "description": "A nice description 0000000010",
                "metadata_modified": "2014-01-17T08:19:31Z",
                "file_created": "2014-01-17T08:19:31Z",
                "encoding": "utf-8",
                "title": "A title 0000000010"
            }
        }

    def _get_second_new_test_data(self):
        return {
            "open_access": False,
            "file_modified": "2017-05-23T14:41:59.507392Z",
            "created_by_api": "2017-05-23T10:07:22.559656Z",
            "checksum_value": "habeebit",
            "project_identifier": "my group",
            "identifier": "urn:nbn:fi:csc-ida201401200000000002",
            "download_url": "http://some.url.csc.fi/0000000002",
            "file_name": "second_new_file_name",
            "file_format": "html/text",
            "file_path": "/some/path/",
            "byte_size": 0,
            "modified_by_api": "2017-05-23T10:07:22.559656Z",
            "checksum_algorithm": "sha2",
            "replication_path": "empty",
            "checksum_checked": None,
            "file_storage": self._get_object_from_test_data('filestorage', requested_index=0),
            "file_characteristics": {
                "application_name": "Application Name",
                "description": "A nice description 0000000010",
                "metadata_modified": "2014-01-17T08:19:31Z",
                "file_created": "2014-01-17T08:19:31Z",
                "encoding": "utf-8",
                "title": "A title 0000000010"
            }
        }
