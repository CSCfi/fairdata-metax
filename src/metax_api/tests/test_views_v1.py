from json import load as json_load

from django.core.management import call_command
from rest_framework import status
from rest_framework.test import APITestCase

from metax_api.models import File, FileStorage

d = print

class FileApiReadTestV1(APITestCase):

    identifier = 'urn:nbn:fi:csc-ida201401200000000001'

    """
    Fields defined in FileReadSerializer
    """
    file_field_names = (
        'id',
        'access_group',
        'byte_size',
        'checksum_algorithm',
        'checksum_checked',
        'checksum_value',
        'download_url',
        'file_format',
        'file_modified',
        'file_name',
        'file_storage_id',
        'file_path',
        'identifier',
        'file_characteristics',
        'open_access',
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
        call_command('loaddata', 'metax_api/tests/test_data.json')
        super(FileApiReadTestV1, cls).setUpClass()

    def test_read_file_list(self):
        response = self.client.get('/rest/files/')
        self.assertEqual(response.status_code, status.HTTP_200_OK)

    def test_read_file_details(self):
        response = self.client.get('/rest/files/%s/' % self.identifier)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(hasattr(response, 'data'), True, 'Request response object is missing attribute \'data\'')
        self.assertEqual('file_name' in response.data.keys(), True)
        self.assertEqual(response.data['identifier'], self.identifier)

    def test_read_file_details_not_found(self):
        response = self.client.get('/rest/files/shouldnotexist/')
        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND)

    def test_model_fields_as_expected(self):
        response = self.client.get('/rest/files/%s/' % self.identifier)
        actual_received_fields = [field for field in response.data.keys()]
        self._test_model_fields_as_expected(self.file_field_names, actual_received_fields)

    def _test_model_fields_as_expected(self, expected_fields, actual_fields):
        # todo add to parent class, because useful also in model tests

        for field in expected_fields:
            if field not in actual_fields:
                raise Exception('Model is missing an expected field: %s' % field)
            actual_fields.remove(field)

        self.assertEqual(len(actual_fields), 0, 'Model contains unexpected fields: %s' % str(actual_fields))


class FileApiWriteTestV1(APITestCase):

    identifier = 'urn:nbn:fi:csc-ida201401200000000001'

    def setUp(self):
        """
        Reloaded for every test case
        """
        call_command('loaddata', 'metax_api/tests/test_data.json')

        """
        New data that is sent to the server for POST, PUT, PATCH requests. Modifier
        slightly as approriate for different purposes
        """
        self.test_new_data = self._get_new_test_data()
        self.second_test_new_data = self._get_second_new_test_data()

    def test_create_file(self):
        newly_created_file_name = 'newly_created_file_name'
        self.test_new_data['file_name'] = newly_created_file_name
        self.test_new_data['identifier'] = 'urn:nbn:fi:csc-thisisanewurn'

        response = self.client.post('/rest/files/', self.test_new_data, format="json")

        self.assertEqual(response.status_code, status.HTTP_201_CREATED)
        self.assertEqual('file_name' in response.data.keys(), True)
        self.assertEqual(response.data['file_name'], newly_created_file_name)

    def test_create_file_error_identifier_exists(self):
        response = self.client.post('/rest/files/', self.test_new_data, format="json")
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

        response = self.client.post('/rest/files/', self.test_new_data, format="json")

        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual('file_characteristics' in response.data.keys(), True, 'The error should concern the field file_characteristics')
        self.assertEqual('field: metadata_modified' in response.data['file_characteristics'][0], True, 'The error should contain the name of the erroneous field')

    def test_create_file_list(self):
        newly_created_file_name = 'newly_created_file_name'
        self.test_new_data['file_name'] = newly_created_file_name
        self.test_new_data['identifier'] = 'urn:nbn:fi:csc-thisisanewurn'
        self.second_test_new_data['identifier'] = 'urn:nbn:fi:csc-thisisanewurnalso'

        response = self.client.post('/rest/files/', [self.test_new_data, self.second_test_new_data], format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED)
        self.assertEqual('success' in response.data.keys(), True)
        self.assertEqual('failed' in response.data.keys(), True)
        self.assertEqual('object' in response.data['success'][0].keys(), True)
        self.assertEqual(len(response.data['success']), 2)
        self.assertEqual(len(response.data['failed']), 0)

    def test_create_file_dont_allow_file_storage_fields_update(self):
        self.test_new_data['identifier'] = 'urn:nbn:fi:csc-thisisanewurn'
        original_title = self.test_new_data['file_storage_id']['file_storage_json']['title']
        self.test_new_data['file_storage_id']['file_storage_json']['title'] = 'new title'

        response = self.client.post('/rest/files/', self.test_new_data, format="json")

        self.assertEqual(response.status_code, status.HTTP_201_CREATED)
        self.assertEqual(response.data['file_storage_id']['file_storage_json']['title'], original_title)
        file_storage = FileStorage.objects.get(pk=response.data['file_storage_id']['id'])
        self.assertEqual(file_storage.file_storage_json['title'], original_title)

    def test_create_file_list_error_one_fails(self):
        newly_created_file_name = 'newly_created_file_name'
        self.test_new_data['file_name'] = newly_created_file_name
        self.test_new_data['identifier'] = 'urn:nbn:fi:csc-thisisanewurn'
        # same as above - should fail
        self.second_test_new_data['identifier'] = 'urn:nbn:fi:csc-thisisanewurn'

        response = self.client.post('/rest/files/', [self.test_new_data, self.second_test_new_data], format="json")

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

        response = self.client.post('/rest/files/', [self.test_new_data, self.second_test_new_data], format="json")
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual('success' in response.data.keys(), True)
        self.assertEqual('failed' in response.data.keys(), True)
        self.assertEqual('object' in response.data['failed'][0].keys(), True)
        self.assertEqual(len(response.data['success']), 0)
        self.assertEqual(len(response.data['failed']), 2)

    def test_update_file(self):
        response = self.client.put('/rest/files/%s/' % self.identifier, self.test_new_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_204_NO_CONTENT)
        self.assertEqual(len(response.data.keys()), 0, 'Returned dict should be empty')

    def test_update_file_error_required_fields(self):
        """
        Field 'access_group' is missing, which should result in an error, since PUT
        replaces an object and requires all 'required' fields to be present.
        """
        self.test_new_data.pop('access_group')
        response = self.client.put('/rest/files/%s/' % self.identifier, self.test_new_data, format="json")

        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual('access_group' in response.data.keys(), True, 'Error for field \'access_group\' is missing from response.data')

    def test_update_file_partial(self):
        new_data = {
            "file_name": "new_file_name",
        }
        response = self.client.patch('/rest/files/%s/' % self.identifier, new_data, format="json")

        # todo 204 no content according to swagger...?
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual('file_name' in response.data.keys(), True)
        self.assertEqual(response.data['file_name'], 'new_file_name', 'Field file_name was not updated')

    def test_update_file_dont_allow_file_storage_fields_update(self):
        original_title = self.test_new_data['file_storage_id']['file_storage_json']['title']
        self.test_new_data['file_storage_id']['file_storage_json']['title'] = 'new title'

        response = self.client.put('/rest/files/%s/' % self.identifier, self.test_new_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_204_NO_CONTENT)
        file_storage = FileStorage.objects.get(pk=self.test_new_data['file_storage_id']['id'])
        self.assertEqual(file_storage.file_storage_json['title'], original_title)

    def test_update_file_not_found(self):
        response = self.client.put('/rest/files/doesnotexist/', self.test_new_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND)

    def test_delete_file(self):
        url = '/rest/files/%s/' % self.identifier
        response = self.client.delete(url)
        self.assertEqual(response.status_code, status.HTTP_204_NO_CONTENT)
        response = self.client.get(url)
        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND)

        try:
            deleted_file = File.objects.get(identifier=self.identifier)
        except File.DoesNotExist:
            raise Exception('Deleted file should not be deleted from the db, but marked as removed')

        self.assertEqual(deleted_file.removed, True)
        self.assertEqual(deleted_file.file_name, 'file_name_0000000001')

    def _get_new_test_data(self):
        return {
            "open_access": False,
            "file_modified": "2017-05-23T14:41:59.507392Z",
            "created_by_api": "2017-05-23T10:07:22.559656Z",
            "checksum_value": "habeebit",
            "access_group": "my group",
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
            "file_storage_id": self._get_file_storage_from_test_data(),
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
            "access_group": "my group",
            "identifier": None,
            "download_url": "http://some.url.csc.fi/0000000001",
            "file_name": "second_new_file_name",
            "file_format": "html/text",
            "file_path": "/some/path/",
            "byte_size": 0,
            "modified_by_api": "2017-05-23T10:07:22.559656Z",
            "checksum_algorithm": "sha2",
            "replication_path": "empty",
            "checksum_checked": None,
            "file_storage_id": self._get_file_storage_from_test_data(),
            "file_characteristics": {
                "application_name": "Application Name",
                "description": "A nice description 0000000010",
                "metadata_modified": "2014-01-17T08:19:31Z",
                "file_created": "2014-01-17T08:19:31Z",
                "encoding": "utf-8",
                "title": "A title 0000000010"
            }
        }

    def _get_file_storage_from_test_data(self):
        with open('metax_api/tests/test_data.json') as test_data_file:
            test_data_dict = json_load(test_data_file)
            return {
                'id': test_data_dict[0]['pk'],
                'file_storage_json': test_data_dict[0]['fields']['file_storage_json'],
            }