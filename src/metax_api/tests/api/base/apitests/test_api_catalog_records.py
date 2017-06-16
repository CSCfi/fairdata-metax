from django.core.management import call_command
from rest_framework import status
from rest_framework.test import APITestCase

from metax_api.models import CatalogRecord, DatasetCatalog
from metax_api.tests.utils import test_data_file_path, TestClassUtils

d = print

class CatalogRecordApiReadTestV1(APITestCase, TestClassUtils):

    """
    Fields defined in CatalogRecordSerializer
    """
    file_field_names = (
        'id',
        'identifier',
        'dataset_catalog',
        'research_dataset',
        'preservation_state',
        'preservation_state_modified',
        'preservation_state_description',
        'preservation_reason_description',
        'ready_status',
        'contract_identifier',
        'mets_object_identifier',
        'catalog_record_modified',
        'dataset_group_edit',
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
        super(CatalogRecordApiReadTestV1, cls).setUpClass()

    def setUp(self):
        catalog_record_from_test_data = self._get_object_from_test_data('catalogrecord')
        self.identifier = catalog_record_from_test_data['identifier']
        self.pk = catalog_record_from_test_data['id']

    def test_read_catalog_record_list(self):
        response = self.client.get('/rest/datasets')
        self.assertEqual(response.status_code, status.HTTP_200_OK)

    def test_read_catalog_record_details_by_pk(self):
        response = self.client.get('/rest/datasets/%s' % self.pk)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data['identifier'], self.identifier)

    def test_read_catalog_record_details_by_identifier(self):
        response = self.client.get('/rest/datasets/%s' % self.identifier)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data['identifier'], self.identifier)

    def test_read_catalog_record_details_not_found(self):
        response = self.client.get('/rest/datasets/shouldnotexist')
        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND)

    def test_model_fields_as_expected(self):
        response = self.client.get('/rest/datasets/%s' % self.identifier)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        actual_received_fields = [field for field in response.data.keys()]
        self._test_model_fields_as_expected(self.file_field_names, actual_received_fields)


class CatalogRecordApiWriteTestV1(APITestCase, TestClassUtils):

    def setUp(self):
        """
        Reloaded for every test case
        """
        call_command('loaddata', test_data_file_path, verbosity=0)
        catalog_record_from_test_data = self._get_object_from_test_data('catalogrecord')
        self.identifier = catalog_record_from_test_data['identifier']
        self.pk = catalog_record_from_test_data['id']

        """
        New data that is sent to the server for POST, PUT, PATCH requests. Modified
        slightly as approriate for different purposes
        """
        self.test_new_data = self._get_new_test_data()
        self.second_test_new_data = self._get_second_new_test_data()

    def test_create_catalog_record(self):
        self.test_new_data['identifier'] = 'urn:nbn:fi:csc-thisisanewurn'
        response = self.client.post('/rest/datasets', self.test_new_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)
        self.assertEqual('research_dataset' in response.data.keys(), True)
        self.assertEqual(response.data['identifier'], self.test_new_data['identifier'])

    def test_create_catalog_record_error_identifier_exists(self):
        # first ok
        response = self.client.post('/rest/datasets', self.test_new_data, format="json")
        # second should give error
        response = self.client.post('/rest/datasets', self.test_new_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual('identifier' in response.data.keys(), True, 'The error should be about an already existing identifier')

    # todo validation disabled until schema updated
    # def test_create_catalog_record_error_json_validation(self):
    #     self.test_new_data['research_dataset']["title"] = 1234456
    #     response = self.client.post('/rest/datasets', self.test_new_data, format="json")

    #     self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
    #     self.assertEqual('research_dataset' in response.data.keys(), True, 'The error should concern the field research_dataset')
    #     self.assertEqual('field: title' in response.data['research_dataset'][0], True, 'The error should contain the name of the erroneous field')

    def test_create_catalog_record_list(self):
        self.test_new_data['identifier'] = 'urn:nbn:fi:csc-thisisanewurn'
        self.second_test_new_data['identifier'] = 'urn:nbn:fi:csc-thisisanewurnalso'

        response = self.client.post('/rest/datasets', [self.test_new_data, self.second_test_new_data], format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED)
        self.assertEqual('success' in response.data.keys(), True)
        self.assertEqual('failed' in response.data.keys(), True)
        self.assertEqual('object' in response.data['success'][0].keys(), True)
        self.assertEqual(len(response.data['success']), 2)
        self.assertEqual(len(response.data['failed']), 0)

    def test_create_catalog_record_dont_allow_dataset_catalog_fields_update(self):
        self.test_new_data['identifier'] = 'urn:nbn:fi:csc-thisisanewurn'
        original_title = self.test_new_data['dataset_catalog']['catalog_json']['title'][0]['en']
        self.test_new_data['dataset_catalog']['catalog_json']['title'][0]['en'] = 'new title'

        response = self.client.post('/rest/datasets', self.test_new_data, format="json")

        self.assertEqual(response.status_code, status.HTTP_201_CREATED)
        self.assertEqual(response.data['dataset_catalog']['catalog_json']['title'][0]['en'], original_title)
        dataset_catalog = DatasetCatalog.objects.get(pk=response.data['dataset_catalog']['id'])
        self.assertEqual(dataset_catalog.catalog_json['title'][0]['en'], original_title)

    def test_create_catalog_record_list_error_one_fails(self):
        self.test_new_data['identifier'] = 'urn:nbn:fi:csc-thisisanewurn'
        # same as above - should fail
        self.second_test_new_data['identifier'] = 'urn:nbn:fi:csc-thisisanewurn'

        response = self.client.post('/rest/datasets', [self.test_new_data, self.second_test_new_data], format="json")

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
        self.assertEqual('identifier' in response.data['failed'][0]['errors'], True, 'The error should have been about an already existing identifier')

    def test_create_catalog_record_list_error_all_fail(self):
        # identifier is a required field, should fail
        self.test_new_data['identifier'] = None
        self.second_test_new_data['identifier'] = None

        response = self.client.post('/rest/datasets', [self.test_new_data, self.second_test_new_data], format="json")
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual('success' in response.data.keys(), True)
        self.assertEqual('failed' in response.data.keys(), True)
        self.assertEqual('object' in response.data['failed'][0].keys(), True)
        self.assertEqual(len(response.data['success']), 0)
        self.assertEqual(len(response.data['failed']), 2)

    def test_update_catalog_record(self):
        self.test_new_data['identifier'] = self.identifier
        response = self.client.put('/rest/datasets/%s' % self.identifier, self.test_new_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_204_NO_CONTENT, response.data)
        self.assertEqual(len(response.data.keys()), 0, 'Returned dict should be empty')

    def test_update_catalog_record_error_required_fields(self):
        """
        Field 'access_group' is missing, which should result in an error, since PUT
        replaces an object and requires all 'required' fields to be present.
        """
        self.test_new_data['identifier'] = self.identifier
        self.test_new_data.pop('research_dataset')
        response = self.client.put('/rest/datasets/%s' % self.identifier, self.test_new_data, format="json")

        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual('research_dataset' in response.data.keys(), True, 'Error for field \'research_dataset\' is missing from response.data')

    def test_update_catalog_record_partial(self):
        new_dataset_catalog = self._get_object_from_test_data('datasetcatalog', requested_index=1)['id']
        new_data = {
            "dataset_catalog": new_dataset_catalog,
        }
        response = self.client.patch('/rest/datasets/%s' % self.identifier, new_data, format="json")

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual('research_dataset' in response.data.keys(), True, 'PATCH operation should return full content')
        self.assertEqual(response.data['dataset_catalog']['id'], new_dataset_catalog, 'Field dataset_catalog was not updated')

    def test_update_catalog_record_dont_allow_dataset_catalog_fields_update(self):
        original_title = self.test_new_data['dataset_catalog']['catalog_json']['title'][0]['en']
        self.test_new_data['dataset_catalog']['catalog_json']['title'][0]['en'] = 'new title'
        self.test_new_data['identifier'] = self.identifier

        response = self.client.put('/rest/datasets/%s' % self.identifier, self.test_new_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_204_NO_CONTENT, response.data)
        dataset_catalog = DatasetCatalog.objects.get(pk=self.test_new_data['dataset_catalog']['id'])
        self.assertEqual(dataset_catalog.catalog_json['title'][0]['en'], original_title)

    def test_update_catalog_record_not_found(self):
        response = self.client.put('/rest/datasets/doesnotexist', self.test_new_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND)

    def test_delete_file(self):
        url = '/rest/datasets/%s' % self.identifier
        response = self.client.delete(url)
        self.assertEqual(response.status_code, status.HTTP_204_NO_CONTENT)
        response = self.client.get(url)
        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND)

        try:
            deleted_catalog_record = CatalogRecord.objects.get(identifier=self.identifier)
        except CatalogRecord.DoesNotExist:
            raise Exception('Deleted file should not be deleted from the db, but marked as removed')

        self.assertEqual(deleted_catalog_record.removed, True)
        self.assertEqual(deleted_catalog_record.identifier, self.identifier)

    def _get_new_test_data(self):
        catalog_record_from_test_data = self._get_object_from_test_data('catalogrecord', requested_index=0)
        return {
            "dataset_catalog": self._get_object_from_test_data('datasetcatalog', requested_index=0),
            "research_dataset": {
                "identifier": "http://urn.fi/urn:nbn:fi:iiidentifier",
                "modified": "2014-01-17T08:19:58Z",
                "versionNotes": [
                    "This version contains changes to x and y."
                ],
                "title": [{
                    "en": "Wonderful Title"
                }],
                "description": [{
                    "en": "A descriptive description describing the contents of this dataset. Must be descriptive."
                }],
                "creator": [{
                    "name": "Teppo Testaaja"
                }],
                "language": [{
                    "title": ["en"],
                    "identifier": "http://lang.ident.ifier/en"
                }],
                "totalbytesize": 1024,
                "files": catalog_record_from_test_data['research_dataset']['files']
            }
        }

    def _get_second_new_test_data(self):
        catalog_record_from_test_data = self._get_object_from_test_data('catalogrecord', requested_index=0)
        return {
            "dataset_catalog": self._get_object_from_test_data('datasetcatalog', requested_index=0),
            "research_dataset": {
                "identifier": "http://urn.fi/urn:nbn:fi:iiidentifier2",
                "modified": "2014-01-17T08:19:58Z",
                "versionNotes": [
                    "This version contains changes to x and y."
                ],
                "title": [{
                    "en": "Wonderful Title"
                }],
                "description": [{
                    "en": "A descriptive description describing the contents of this dataset. Must be descriptive."
                }],
                "creator": [{
                    "name": "Teppo Testaaja"
                }],
                "language": [{
                    "title": ["en"],
                    "identifier": "http://lang.ident.ifier/en"
                }],
                "totalbytesize": 1024,
                "files": catalog_record_from_test_data['research_dataset']['files']
            }
        }
