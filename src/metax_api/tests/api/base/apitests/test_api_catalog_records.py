from datetime import datetime, timedelta

from django.core.management import call_command
from rest_framework import status
from rest_framework.test import APITestCase

from metax_api.models import CatalogRecord, DatasetCatalog
from metax_api.tests.utils import test_data_file_path, TestClassUtils

class CatalogRecordApiReadTestV1(APITestCase, TestClassUtils):

    """
    Fields defined in CatalogRecordSerializer
    """
    file_field_names = (
        'id',
        'identifier',
        'contract',
        'dataset_catalog',
        'research_dataset',
        'preservation_state',
        'preservation_state_modified',
        'preservation_state_description',
        'preservation_reason_description',
        'contract_identifier',
        'mets_object_identifier',
        'dataset_group_edit',
        'modified_by_user_id',
        'modified_by_api',
        'created_by_user_id',
        'created_by_api',
        'next_version_id',
        'next_version_identifier',
        'previous_version_id',
        'previous_version_identifier',
        'version_created',
    )

    @classmethod
    def setUpClass(cls):
        """
        Loaded only once for test cases inside this class.
        """
        call_command('loaddata', test_data_file_path, verbosity=0)
        super(CatalogRecordApiReadTestV1, cls).setUpClass()

    def setUp(self):
        catalog_record_from_test_data = self._get_object_from_test_data('catalogrecord', requested_index=0)
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

    def test_read_catalog_record_list_pagination_1(self):
        response = self.client.get('/rest/datasets?limit=2&offset=0')
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data['results']), 2, 'There should have been exactly two results')
        self.assertEqual(response.data['results'][0]['id'], 1, 'Id of first result should have been 1')

    def test_read_catalog_record_list_pagination_2(self):
        response = self.client.get('/rest/datasets?limit=2&offset=2')
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data['results']), 2, 'There should have been exactly two results')
        self.assertEqual(response.data['results'][0]['id'], 3, 'Id of first result should have been 3')

    def test_read_catalog_record_search_by_preservation_state_0(self):
        response = self.client.get('/rest/datasets?state=0')
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data) > 2, True, 'There should have been multiple results for state=0 request')
        self.assertEqual(response.data[0]['id'], 1)

    def test_read_catalog_record_search_by_preservation_state_1(self):
        response = self.client.get('/rest/datasets?state=1')
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data), 1)
        self.assertEqual(response.data[0]['id'], 2)

    def test_read_catalog_record_search_by_preservation_state_2(self):
        response = self.client.get('/rest/datasets?state=2')
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data), 1)
        self.assertEqual(response.data[0]['id'], 3)

    def test_read_catalog_record_search_by_preservation_state_666(self):
        response = self.client.get('/rest/datasets?state=666')
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data), 0)

    def test_read_catalog_record_search_by_preservation_state_many(self):
        response = self.client.get('/rest/datasets?state=1,2')
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data), 2)
        self.assertEqual(response.data[0]['preservation_state'], 1)
        self.assertEqual(response.data[1]['preservation_state'], 2)

    def test_read_catalog_record_search_by_preservation_state_invalid_value(self):
        response = self.client.get('/rest/datasets?state=1,a')
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual('is not an integer' in response.data['state'][0], True, 'Error should say letter a is not an integer')

    def test_read_catalog_record_search_by_owner_1(self):
        response = self.client.get('/rest/datasets?owner=id:of:curator:rahikainen')
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data), 5)
        self.assertEqual(response.data[0]['research_dataset']['curator'][0]['name'], 'Rahikainen', 'Owner name is not matching')
        self.assertEqual(response.data[4]['research_dataset']['curator'][0]['name'], 'Rahikainen', 'Owner name is not matching')

    def test_read_catalog_record_search_by_owner_2(self):
        response = self.client.get('/rest/datasets?owner=id:of:curator:jarski')
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data), 4)
        self.assertEqual(response.data[0]['research_dataset']['curator'][0]['name'], 'Jarski', 'Owner name is not matching')
        self.assertEqual(response.data[3]['research_dataset']['curator'][0]['name'], 'Jarski', 'Owner name is not matching')

    def test_read_catalog_record_search_by_owner_not_found_1(self):
        response = self.client.get('/rest/datasets?owner=Not Found')
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data), 0)

    def test_read_catalog_record_search_by_owner_not_found_case_sensitivity(self):
        response = self.client.get('/rest/datasets?owner=id:of:curator:Rahikainen')
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data), 0)

    def test_read_catalog_record_search_by_owner_and_state_1(self):
        response = self.client.get('/rest/datasets?owner=id:of:curator:rahikainen&state=1')
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data), 1)
        self.assertEqual(response.data[0]['id'], 2)
        self.assertEqual(response.data[0]['preservation_state'], 1)
        self.assertEqual(response.data[0]['research_dataset']['curator'][0]['name'], 'Rahikainen', 'Owner name is not matching')

    def test_read_catalog_record_search_by_owner_and_state_2(self):
        response = self.client.get('/rest/datasets?owner=id:of:curator:rahikainen&state=2')
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data), 1)
        self.assertEqual(response.data[0]['id'], 3)
        self.assertEqual(response.data[0]['preservation_state'], 2)
        self.assertEqual(response.data[0]['research_dataset']['curator'][0]['name'], 'Rahikainen', 'Owner name is not matching')

    def test_read_catalog_record_search_by_owner_and_state_not_found(self):
        response = self.client.get('/rest/datasets?owner=id:of:curator:rahikainen&state=55')
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data), 0)

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
        cr = CatalogRecord.objects.get(pk=response.data['id'])
        self.assertEqual(cr.created_by_api >= datetime.now() - timedelta(seconds=5), True, 'Timestamp should have been updated during object creation')

    def test_create_catalog_record_error_identifier_exists(self):
        # first ok
        response = self.client.post('/rest/datasets', self.test_new_data, format="json")
        # second should give error
        response = self.client.post('/rest/datasets', self.test_new_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual('identifier' in response.data.keys(), True, 'The error should be about an already existing identifier')

    def test_create_catalog_record_error_json_validation(self):
        self.test_new_data['identifier'] = "neeeeeeeeew:id"
        self.test_new_data['research_dataset']["title"] = 1234456
        response = self.client.post('/rest/datasets', self.test_new_data, format="json")

        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual('research_dataset' in response.data.keys(), True, 'The error should concern the field research_dataset')
        self.assertEqual('field: title' in response.data['research_dataset'][0], True, 'The error should contain the name of the erroneous field')

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
        cr = CatalogRecord.objects.get(pk=self.pk)
        self.assertEqual(cr.modified_by_api >= datetime.now() - timedelta(seconds=5), True, 'Timestamp should have been updated during object update')

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

    def test_update_catalog_record_pas_state_allowed_value(self):
        self.test_new_data['identifier'] = self.identifier
        self.test_new_data['preservation_state'] = 3
        response = self.client.put('/rest/datasets/%s' % self.identifier, self.test_new_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_204_NO_CONTENT)

    def test_update_catalog_record_pas_state_unallowed_value(self):
        self.test_new_data['identifier'] = self.identifier
        self.test_new_data['preservation_state'] = 111
        response = self.client.put('/rest/datasets/%s' % self.identifier, self.test_new_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST, 'HTTP status should be 400 due to invalid value')
        self.assertEqual('preservation_state' in response.data.keys(), True, 'The error should mention the field preservation_state')

    def test_update_catalog_record_preservation_state_modified_is_updated(self):
        self.test_new_data['identifier'] = self.identifier
        self.test_new_data['preservation_state'] = 4
        response = self.client.put('/rest/datasets/%s' % self.identifier, self.test_new_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_204_NO_CONTENT, response.data)
        cr = CatalogRecord.objects.get(pk=self.pk)
        self.assertEqual(cr.preservation_state_modified >= datetime.now() - timedelta(seconds=5), True, 'Timestamp should have been updated during object update')

    def test_delete_catalog_record(self):
        url = '/rest/datasets/%s' % self.identifier
        response = self.client.delete(url)
        self.assertEqual(response.status_code, status.HTTP_204_NO_CONTENT)
        response = self.client.get(url)
        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND)

        deleted_catalog_record = None

        try:
            deleted_catalog_record = CatalogRecord.objects.get(identifier=self.identifier)
        except CatalogRecord.DoesNotExist:
            pass

        if deleted_catalog_record:
            raise Exception('Deleted CatalogRecord should not be retrievable from the default objects table')

        try:
            deleted_catalog_record = CatalogRecord.objects_unfiltered.get(identifier=self.identifier)
        except CatalogRecord.DoesNotExist:
            raise Exception('Deleted CatalogRecord should not be deleted from the db, but marked as removed')

        self.assertEqual(deleted_catalog_record.removed, True)
        self.assertEqual(deleted_catalog_record.identifier, self.identifier)

    def test_delete_catalog_record_contract_is_not_deleted(self):
        catalog_record_from_test_data = self._get_object_from_test_data('catalogrecord', requested_index=0)
        url = '/rest/datasets/%s' % catalog_record_from_test_data['identifier']
        self.client.delete(url)
        response2 = self.client.get('/rest/contracts/%d' % catalog_record_from_test_data['contract'])
        self.assertEqual(response2.status_code, status.HTTP_200_OK, 'The contract of the CatalogRecord should not be deleted when deleting a single CatalogRecord.')

    def test_delete_catalog_record_not_found_with_search_by_owner(self):
        # owner of pk=1 is Default Owner. Delete pk=2 == first dataset owner by Rahikainen.
        # After deleting, first dataset owned by Rahikainen should be pk=3
        response = self.client.delete('/rest/datasets/2')
        response = self.client.get('/rest/datasets?owner=id:of:curator:rahikainen')
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data), 4)
        self.assertEqual(response.data[0]['id'], 3)

    def test_catalog_record_propose_to_pas_success(self):
        catalog_record_before = CatalogRecord.objects.get(pk=self.pk)
        catalog_record_before.research_dataset['ready_status'] = CatalogRecord.READY_STATUS_FINISHED
        catalog_record_before.save()

        response = self.client.post('/rest/datasets/%s/proposetopas?state=%d&contract=%s' %
            (
                self.identifier,
                CatalogRecord.PRESERVATION_STATE_PROPOSED_MIDTERM,
                self._get_object_from_test_data('contract', requested_index=0)['contract_json']['identifier']
            ),
            format="json")

        self.assertEqual(response.status_code, status.HTTP_204_NO_CONTENT)
        catalog_record_after = CatalogRecord.objects.get(pk=self.pk)
        self.assertEqual(catalog_record_after.preservation_state, CatalogRecord.PRESERVATION_STATE_PROPOSED_MIDTERM)

    def test_catalog_record_propose_to_pas_missing_parameter_state(self):
        response = self.client.post('/rest/datasets/%s/proposetopas?contract=%s' %
            (
                self.identifier,
                self._get_object_from_test_data('contract', requested_index=0)['contract_json']['identifier']
            ),
            format="json")

        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual('state' in response.data, True, 'Response data should contain an error about the field')

    def test_catalog_record_propose_to_pas_wrong_parameter_state(self):
        response = self.client.post('/rest/datasets/%s/proposetopas?state=%d&contract=%s' %
            (
                self.identifier,
                15,
                self._get_object_from_test_data('contract', requested_index=0)['contract_json']['identifier']
            ),
            format="json")

        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual('state' in response.data, True, 'Response data should contain an error about the field')

    def test_catalog_record_propose_to_pas_missing_parameter_contract(self):
        response = self.client.post('/rest/datasets/%s/proposetopas?state=%s' %
            (
                self.identifier,
                CatalogRecord.PRESERVATION_STATE_PROPOSED_MIDTERM,
            ),
            format="json")

        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual('contract' in response.data, True, 'Response data should contain an error about the field')

    def test_catalog_record_propose_to_pas_contract_not_found(self):
        catalog_record_before = CatalogRecord.objects.get(pk=self.pk)
        catalog_record_before.research_dataset['ready_status'] = CatalogRecord.READY_STATUS_FINISHED
        catalog_record_before.save()

        response = self.client.post('/rest/datasets/%s/proposetopas?state=%d&contract=%s' %
            (
                self.identifier,
                CatalogRecord.PRESERVATION_STATE_PROPOSED_MIDTERM,
                'does-not-exist'
            ),
            format="json")

        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual('contract' in response.data, True, 'Response data should contain an error about the field')

    def test_catalog_record_propose_to_pas_wrong_ready_status(self):
        catalog_record_before = CatalogRecord.objects.get(pk=self.pk)
        catalog_record_before.research_dataset['ready_status'] = CatalogRecord.READY_STATUS_UNFINISHED
        catalog_record_before.save()

        response = self.client.post('/rest/datasets/%s/proposetopas?state=%d&contract=%s' %
            (
                self.identifier,
                CatalogRecord.PRESERVATION_STATE_PROPOSED_MIDTERM,
                self._get_object_from_test_data('contract', requested_index=0)['contract_json']['identifier']
            ),
            format="json")

        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN)
        self.assertEqual('research_dataset' in response.data, True, 'Response data should contain an error about the field')
        self.assertEqual('ready_status' in response.data['research_dataset'], True, 'Response data should contain an error about the field')

    def test_catalog_record_propose_to_pas_wrong_preservation_state(self):
        catalog_record_before = CatalogRecord.objects.get(pk=self.pk)
        catalog_record_before.research_dataset['ready_status'] = CatalogRecord.READY_STATUS_FINISHED
        catalog_record_before.preservation_state = CatalogRecord.PRESERVATION_STATE_IN_LONGTERM_PAS
        catalog_record_before.save()

        response = self.client.post('/rest/datasets/%s/proposetopas?state=%d&contract=%s' %
            (
                self.identifier,
                CatalogRecord.PRESERVATION_STATE_PROPOSED_MIDTERM,
                self._get_object_from_test_data('contract', requested_index=0)['contract_json']['identifier']
            ),
            format="json")

        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN)
        self.assertEqual('preservation_state' in response.data, True, 'Response data should contain an error about the field')

    def _get_new_test_data(self):
        catalog_record_from_test_data = self._get_object_from_test_data('catalogrecord', requested_index=0)
        return {
            "contract": self._get_object_from_test_data('contract', requested_index=0),
            "dataset_catalog": self._get_object_from_test_data('datasetcatalog', requested_index=0),
            "research_dataset": {
                "urn_identifier": "http://urn.fi/urn:nbn:fi:iiidentifier",
                "preferred_identifier": "http://urn.fi/urn:nbn:fi:preferred1",
                "modified": "2014-01-17T08:19:58Z",
                "version_notes": [
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
                "curator": [{
                    "name": "Default Owner"
                }],
                "language": [{
                    "title": "en",
                    "identifier": "http://lang.ident.ifier/en"
                }],
                "total_byte_size": 1024,
                "ready_status": "Unfinished",
                "files": catalog_record_from_test_data['research_dataset']['files']
            }
        }

    def _get_second_new_test_data(self):
        catalog_record_from_test_data = self._get_object_from_test_data('catalogrecord', requested_index=0)
        return {
            "contract": self._get_object_from_test_data('contract', requested_index=0),
            "dataset_catalog": self._get_object_from_test_data('datasetcatalog', requested_index=0),
            "research_dataset": {
                "urn_identifier": "http://urn.fi/urn:nbn:fi:iiidentifier2",
                "preferred_identifier": "http://urn.fi/urn:nbn:fi:preferred2",
                "modified": "2014-01-17T08:19:58Z",
                "version_notes": [
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
                "curator": [{
                    "name": "Default Owner"
                }],
                "language": [{
                    "title": "en",
                    "identifier": "http://lang.ident.ifier/en"
                }],
                "total_byte_size": 1024,
                "ready_status": "Unfinished",
                "files": catalog_record_from_test_data['research_dataset']['files']
            }
        }
