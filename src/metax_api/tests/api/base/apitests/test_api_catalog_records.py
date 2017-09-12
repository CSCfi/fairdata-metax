from datetime import datetime, timedelta

from django.core.management import call_command
from rest_framework import status
from rest_framework.test import APITestCase

from metax_api.models import CatalogRecord, DataCatalog
from metax_api.tests.utils import test_data_file_path, TestClassUtils


class CatalogRecordApiReadTestV1(APITestCase, TestClassUtils):

    @classmethod
    def setUpClass(cls):
        """
        Loaded only once for test cases inside this class.
        """
        call_command('loaddata', test_data_file_path, verbosity=0)
        super(CatalogRecordApiReadTestV1, cls).setUpClass()

    def setUp(self):
        catalog_record_from_test_data = self._get_object_from_test_data('catalogrecord', requested_index=0)
        self.pk = catalog_record_from_test_data['id']
        self.urn_identifier = catalog_record_from_test_data['research_dataset']['urn_identifier']
        self.preferred_identifier = catalog_record_from_test_data['research_dataset']['preferred_identifier']

    def test_read_catalog_record_list(self):
        response = self.client.get('/rest/datasets')
        self.assertEqual(response.status_code, status.HTTP_200_OK)

    def test_read_catalog_record_details_by_pk(self):
        response = self.client.get('/rest/datasets/%s' % self.pk)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data['research_dataset']['preferred_identifier'], self.preferred_identifier)

    def test_read_catalog_record_details_by_identifier(self):
        response = self.client.get('/rest/datasets/%s' % self.preferred_identifier)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data['research_dataset']['preferred_identifier'], self.preferred_identifier)

    def test_read_catalog_record_details_not_found(self):
        response = self.client.get('/rest/datasets/shouldnotexist')
        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND)

    #
    # pagination
    #

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

    #
    # preservation_state filtering
    #

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

    #
    # query_params
    #

    def test_read_catalog_record_search_by_curator_1(self):
        response = self.client.get('/rest/datasets?curator=id:of:curator:rahikainen')
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data), 5)
        self.assertEqual(response.data[0]['research_dataset']['curator'][0]['name'], 'Rahikainen', 'Curator name is not matching')
        self.assertEqual(response.data[4]['research_dataset']['curator'][0]['name'], 'Rahikainen', 'Curator name is not matching')

    def test_read_catalog_record_search_by_curator_2(self):
        response = self.client.get('/rest/datasets?curator=id:of:curator:jarski')
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data), 4)
        self.assertEqual(response.data[0]['research_dataset']['curator'][0]['name'], 'Jarski', 'Curator name is not matching')
        self.assertEqual(response.data[3]['research_dataset']['curator'][0]['name'], 'Jarski', 'Curator name is not matching')

    def test_read_catalog_record_search_by_curator_not_found_1(self):
        response = self.client.get('/rest/datasets?curator=Not Found')
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data), 0)

    def test_read_catalog_record_search_by_curator_not_found_case_sensitivity(self):
        response = self.client.get('/rest/datasets?curator=id:of:curator:Rahikainen')
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data), 0)

    def test_read_catalog_record_search_by_curator_and_state_1(self):
        response = self.client.get('/rest/datasets?curator=id:of:curator:rahikainen&state=1')
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data), 1)
        self.assertEqual(response.data[0]['id'], 2)
        self.assertEqual(response.data[0]['preservation_state'], 1)
        self.assertEqual(response.data[0]['research_dataset']['curator'][0]['name'], 'Rahikainen', 'Curator name is not matching')

    def test_read_catalog_record_search_by_curator_and_state_2(self):
        response = self.client.get('/rest/datasets?curator=id:of:curator:rahikainen&state=2')
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data), 1)
        self.assertEqual(response.data[0]['id'], 3)
        self.assertEqual(response.data[0]['preservation_state'], 2)
        self.assertEqual(response.data[0]['research_dataset']['curator'][0]['name'], 'Rahikainen', 'Curator name is not matching')

    def test_read_catalog_record_search_by_curator_and_state_not_found(self):
        response = self.client.get('/rest/datasets?curator=id:of:curator:rahikainen&state=55')
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data), 0)

    def test_read_catalog_record_search_by_owner_id(self):
        cr = CatalogRecord.objects.get(pk=1)
        cr.owner_id = '123'
        cr.save()
        response = self.client.get('/rest/datasets?owner_id=123')
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data), 1)
        self.assertEqual(response.data[0]['owner_id'], '123')

    def test_read_catalog_record_search_by_creator_id(self):
        cr = CatalogRecord.objects.get(pk=1)
        cr.created_by_user_id = '123'
        cr.save()
        response = self.client.get('/rest/datasets?created_by_user_id=123')
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data), 1)
        self.assertEqual(response.data[0]['created_by_user_id'], '123')

    #
    # misc
    #

    def test_read_catalog_record_exists(self):
        response = self.client.get('/rest/datasets/%s/exists' % self.pk)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertTrue(response.data)
        response = self.client.get('/rest/datasets/%s/exists' % self.urn_identifier)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertTrue(response.data)
        response = self.client.get('/rest/datasets/%s/exists' % self.preferred_identifier)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertTrue(response.data)

    def test_read_catalog_record_does_not_exist(self):
        response = self.client.get('/rest/datasets/%s/exists' % 'urn:nbn:fi:non_existing_dataset_identifier')
        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND)
        self.assertFalse(response.data)


class CatalogRecordApiWriteTestV1(APITestCase, TestClassUtils):

    def setUp(self):
        """
        Reloaded for every test case
        """
        call_command('loaddata', test_data_file_path, verbosity=0)
        catalog_record_from_test_data = self._get_object_from_test_data('catalogrecord')
        self.urn_identifier = catalog_record_from_test_data['research_dataset']['urn_identifier']
        self.preferred_identifier = catalog_record_from_test_data['research_dataset']['preferred_identifier']
        self.pk = catalog_record_from_test_data['id']

        """
        New data that is sent to the server for POST, PUT, PATCH requests. Modified
        slightly as approriate for different purposes
        """
        self.test_new_data = self._get_new_test_data()
        self.second_test_new_data = self._get_second_new_test_data()

        self._use_http_authorization()

    #
    #
    #
    # create apis
    #
    #
    #

    def test_create_catalog_record(self):
        self.test_new_data['research_dataset']['preferred_identifier'] = 'urn:nbn:fi:csc-thisisanewurn'
        response = self.client.post('/rest/datasets', self.test_new_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)
        self.assertEqual('research_dataset' in response.data.keys(), True)
        self.assertEqual(response.data['research_dataset']['urn_identifier'] is not None, True, 'urn_identifier should have been generated')
        self.assertEqual(response.data['research_dataset']['preferred_identifier'], self.test_new_data['research_dataset']['preferred_identifier'])
        cr = CatalogRecord.objects.get(pk=response.data['id'])
        self.assertEqual(cr.created_by_api >= datetime.now() - timedelta(seconds=5), True, 'Timestamp should have been updated during object creation')

    def test_create_catalog_record_without_preferred_identifier(self):
        self.test_new_data['research_dataset']['preferred_identifier'] = None
        response = self.client.post('/rest/datasets', self.test_new_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)
        self.assertEqual('research_dataset' in response.data.keys(), True)
        self.assertEqual(response.data['research_dataset']['preferred_identifier'], response.data['research_dataset']['urn_identifier'],
            'urn_identifier and preferred_identifier should equal')
        cr = CatalogRecord.objects.get(pk=response.data['id'])
        self.assertEqual(cr.created_by_api >= datetime.now() - timedelta(seconds=5), True, 'Timestamp should have been updated during object creation')

    def test_create_catalog_record_error_identifier_exists(self):
        # first ok
        response = self.client.post('/rest/datasets', self.test_new_data, format="json")

        # second should give error
        existing_identifier = response.data['research_dataset']['urn_identifier']
        self.test_new_data['research_dataset']['preferred_identifier'] = existing_identifier
        response = self.client.post('/rest/datasets', self.test_new_data, format="json")

        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual('research_dataset' in response.data.keys(), True, 'The error should be about an error in research_dataset')
        self.assertEqual('preferred_identifier' in response.data['research_dataset'][0], True, 'The error should be about urn_identifier already existing')

    def test_create_catalog_contract_string_identifier(self):
        self.test_new_data['contract'] = 'optional:contract:identifier1'
        response = self.client.post('/rest/datasets', self.test_new_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)

    def test_create_catalog_error_contract_string_identifier_not_found(self):
        self.test_new_data['contract'] = 'doesnotexist'
        response = self.client.post('/rest/datasets', self.test_new_data, format="json")
        # self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND, 'Should have raised 404 not found')
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual('contract' in response.data, True, 'Error should have been about contract not found')

    def test_create_catalog_record_error_json_validation(self):
        self.test_new_data['research_dataset']['preferred_identifier'] = "neeeeeeeeew:id"
        self.test_new_data['research_dataset']["title"] = 1234456
        response = self.client.post('/rest/datasets', self.test_new_data, format="json")

        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual('research_dataset' in response.data.keys(), True, 'The error should concern the field research_dataset')
        self.assertEqual('field: title' in response.data['research_dataset'][0], True, 'The error should contain the name of the erroneous field')

    def test_create_catalog_record_dont_allow_data_catalog_fields_update(self):
        self.test_new_data['research_dataset']['preferred_identifier'] = 'urn:nbn:fi:csc-thisisanewurn'
        original_title = self.test_new_data['data_catalog']['catalog_json']['title'][0]['en']
        self.test_new_data['data_catalog']['catalog_json']['title'][0]['en'] = 'new title'

        response = self.client.post('/rest/datasets', self.test_new_data, format="json")

        self.assertEqual(response.status_code, status.HTTP_201_CREATED)
        self.assertEqual(response.data['data_catalog']['catalog_json']['title'][0]['en'], original_title)
        data_catalog = DataCatalog.objects.get(pk=response.data['data_catalog']['id'])
        self.assertEqual(data_catalog.catalog_json['title'][0]['en'], original_title)

    #
    # create list operations
    #

    def test_create_catalog_record_list(self):
        self.test_new_data['research_dataset']['preferred_identifier'] = 'urn:nbn:fi:csc-thisisanewurn'
        self.second_test_new_data['research_dataset']['preferred_identifier'] = 'urn:nbn:fi:csc-thisisanewurnalso'

        response = self.client.post('/rest/datasets', [self.test_new_data, self.second_test_new_data], format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED)
        self.assertEqual('success' in response.data.keys(), True)
        self.assertEqual('failed' in response.data.keys(), True)
        self.assertEqual('object' in response.data['success'][0].keys(), True)
        self.assertEqual(len(response.data['success']), 2)
        self.assertEqual(len(response.data['failed']), 0)

    def test_create_catalog_record_list_error_one_fails(self):
        self.test_new_data['research_dataset']['preferred_identifier'] = 'urn:nbn:fi:csc-thisisanewurn'
        # same as above - should fail
        self.second_test_new_data['research_dataset']['preferred_identifier'] = 'urn:nbn:fi:csc-thisisanewurn'

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
        self.assertEqual('research_dataset' in response.data['failed'][0]['errors'], True, 'The error should have been about an already existing identifier')

    def test_create_catalog_record_list_error_all_fail(self):
        # data catalog is a required field, should fail
        self.test_new_data['data_catalog'] = None
        self.second_test_new_data['data_catalog'] = None

        response = self.client.post('/rest/datasets', [self.test_new_data, self.second_test_new_data], format="json")
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual('success' in response.data.keys(), True)
        self.assertEqual('failed' in response.data.keys(), True)
        self.assertEqual('object' in response.data['failed'][0].keys(), True)
        self.assertEqual(len(response.data['success']), 0)
        self.assertEqual(len(response.data['failed']), 2)

    #
    #
    # update apis
    #
    #

    def test_update_catalog_record(self):
        self.test_new_data['research_dataset']['preferred_identifier'] = self.preferred_identifier
        response = self.client.put('/rest/datasets/%s' % self.urn_identifier, self.test_new_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_204_NO_CONTENT, response.data)
        self.assertEqual(len(response.data.keys()), 0, 'Returned dict should be empty')
        cr = CatalogRecord.objects.get(pk=self.pk)
        self.assertEqual(cr.modified_by_api >= datetime.now() - timedelta(seconds=5), True, 'Timestamp should have been updated during object update')

    def test_update_catalog_record_error_using_preferred_identifier(self):
        self.test_new_data['research_dataset']['preferred_identifier'] = self.preferred_identifier
        response = self.client.put('/rest/datasets/%s' % self.preferred_identifier, self.test_new_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND, 'Update operation should return 404 when using preferred_identifier')

    def test_update_catalog_record_error_required_fields(self):
        """
        Field 'research_dataset' is missing, which should result in an error, since PUT
        replaces an object and requires all 'required' fields to be present.
        """
        self.test_new_data.pop('research_dataset')
        response = self.client.put('/rest/datasets/%s' % self.urn_identifier, self.test_new_data, format="json")

        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual('research_dataset' in response.data.keys(), True, 'Error for field \'research_dataset\' is missing from response.data')

    def test_update_catalog_record_partial(self):
        new_data_catalog = self._get_object_from_test_data('datacatalog', requested_index=1)['id']
        new_data = {
            "data_catalog": new_data_catalog,
        }
        response = self.client.patch('/rest/datasets/%s' % self.urn_identifier, new_data, format="json")

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual('research_dataset' in response.data.keys(), True, 'PATCH operation should return full content')
        self.assertEqual(response.data['data_catalog']['id'], new_data_catalog, 'Field data_catalog was not updated')

    def test_update_catalog_record_dont_allow_data_catalog_fields_update(self):
        original_title = self.test_new_data['data_catalog']['catalog_json']['title'][0]['en']
        self.test_new_data['data_catalog']['catalog_json']['title'][0]['en'] = 'new title'
        self.test_new_data['research_dataset']['preferred_identifier'] = self.preferred_identifier

        response = self.client.put('/rest/datasets/%s' % self.urn_identifier, self.test_new_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_204_NO_CONTENT, response.data)
        data_catalog = DataCatalog.objects.get(pk=self.test_new_data['data_catalog']['id'])
        self.assertEqual(data_catalog.catalog_json['title'][0]['en'], original_title)

    def test_update_catalog_record_not_found(self):
        response = self.client.put('/rest/datasets/doesnotexist', self.test_new_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND)

    def test_update_catalog_record_contract_string_identifier(self):
        cr_id = 2
        cr = CatalogRecord.objects.get(pk=cr_id)
        old_contract_identifier = cr.contract.contract_json['identifier']
        self.test_new_data['contract'] = 'optional:contract:identifier2'
        response = self.client.put('/rest/datasets/%d' % cr_id, self.test_new_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_204_NO_CONTENT, response.data)
        cr2 = CatalogRecord.objects.get(pk=cr_id)
        new_contract_identifier = cr2.contract.contract_json['identifier']
        self.assertNotEqual(old_contract_identifier, new_contract_identifier, 'Contract identifier should have changed')

    #
    # update preservation_state operations
    #

    def test_update_catalog_record_pas_state_allowed_value(self):
        self.test_new_data['preservation_state'] = 3
        response = self.client.put('/rest/datasets/%s' % self.urn_identifier, self.test_new_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_204_NO_CONTENT)

    def test_update_catalog_record_pas_state_unallowed_value(self):
        self.test_new_data['preservation_state'] = 111
        response = self.client.put('/rest/datasets/%s' % self.urn_identifier, self.test_new_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST, 'HTTP status should be 400 due to invalid value')
        self.assertEqual('preservation_state' in response.data.keys(), True, 'The error should mention the field preservation_state')

    def test_update_catalog_record_preservation_state_modified_is_updated(self):
        self.test_new_data['preservation_state'] = 4
        response = self.client.put('/rest/datasets/%s' % self.urn_identifier, self.test_new_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_204_NO_CONTENT, response.data)
        cr = CatalogRecord.objects.get(pk=self.pk)
        self.assertEqual(cr.preservation_state_modified >= datetime.now() - timedelta(seconds=5), True, 'Timestamp should have been updated during object update')

    #
    # update list operations PUT
    #

    def test_catalog_record_update_list(self):
        self.test_new_data['id'] = 1
        self.test_new_data['research_dataset']['description'] = [{ 'en': 'updated description' }]

        self.second_test_new_data['id'] = 2
        self.second_test_new_data['research_dataset']['description'] = [{ 'en': 'second updated description' }]

        response = self.client.put('/rest/datasets', [self.test_new_data, self.second_test_new_data], format="json")
        self.assertEqual(response.status_code, status.HTTP_204_NO_CONTENT, response.data)
        self.assertEqual(response.data, {}, 'response.data should be empty object')

        updated_cr = CatalogRecord.objects.get(pk=1)
        desc = updated_cr.research_dataset['description']
        self.assertEqual(desc[0]['en'], 'updated description', 'description did not update')

    def test_catalog_record_update_list_error_one_fails(self):
        self.test_new_data['id'] = 1
        self.test_new_data['research_dataset']['description'] = [{ 'en': 'updated description' }]

        # this value should already exist, therefore should fail
        self.second_test_new_data['research_dataset']['preferred_identifier'] = self.urn_identifier
        self.second_test_new_data['id'] = 2

        response = self.client.put('/rest/datasets', [self.test_new_data, self.second_test_new_data], format="json")
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        self.assertEqual('success' in response.data.keys(), True)
        self.assertEqual('failed' in response.data.keys(), True)
        self.assertEqual(isinstance(response.data['success'], list), True, 'return data should contain key success, which is a list')
        self.assertEqual(len(response.data['success']), 0, 'success list should be empty')
        self.assertEqual(len(response.data['failed']), 1, 'there should have been one failed element')

        updated_cr = CatalogRecord.objects.get(pk=1)
        desc = updated_cr.research_dataset['description']
        self.assertEqual(desc[0]['en'], 'updated description', 'description did not update for first item')

    def test_catalog_record_update_list_error_key_not_found(self):
        # does not have identifier key
        self.test_new_data['research_dataset'].pop('urn_identifier')
        self.test_new_data['research_dataset']['description'] = [{ 'en': 'updated description' }]

        self.second_test_new_data['id'] = 2
        self.second_test_new_data['research_dataset']['description'] = [{ 'en': 'second updated description' }]

        response = self.client.put('/rest/datasets', [self.test_new_data, self.second_test_new_data], format="json")
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        self.assertEqual('success' in response.data.keys(), True)
        self.assertEqual('failed' in response.data.keys(), True)
        self.assertEqual(len(response.data['success']), 0, 'success list should be empty')
        self.assertEqual(len(response.data['failed']), 1, 'there should have been one failed element')

    #
    # update list operations PATCH
    #

    def test_catalog_record_partial_update_list(self):
        test_data = {}
        test_data['id'] = 1
        test_data['preservation_state'] = 1

        second_test_data = {}
        second_test_data['id'] = 2
        second_test_data['preservation_state'] = 2

        response = self.client.patch('/rest/datasets', [test_data, second_test_data], format="json")
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        self.assertEqual('success' in response.data, True, 'response.data should contain list of changed objects')
        self.assertEqual(len(response.data), 2, 'response.data should contain 2 changed objects')
        self.assertEqual('research_dataset' in response.data['success'][0]['object'], True, 'response.data should contain full objects')

        updated_cr = CatalogRecord.objects.get(pk=1)
        self.assertEqual(updated_cr.preservation_state, 1, 'preservation state should have changed to 1')

    def test_catalog_record_partial_update_list_error_one_fails(self):
        test_data = {}
        test_data['id'] = 1
        test_data['preservation_state'] = 1

        second_test_data = {}
        second_test_data['preservation_state'] = 555 # value not allowed
        second_test_data['id'] = 2

        response = self.client.patch('/rest/datasets', [test_data, second_test_data], format="json")
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        self.assertEqual('success' in response.data.keys(), True)
        self.assertEqual('failed' in response.data.keys(), True)
        self.assertEqual(len(response.data['success']), 1, 'success list should contain one item')
        self.assertEqual(len(response.data['failed']), 1, 'there should have been one failed element')
        self.assertEqual('preservation_state' in response.data['failed'][0]['errors'], True, response.data['failed'][0]['errors'])

    def test_catalog_record_partial_update_list_error_key_not_found(self):
        # does not have identifier key
        test_data = {}
        test_data['preservation_state'] = 1

        second_test_data = {}
        second_test_data['id'] = 2
        second_test_data['preservation_state'] = 2

        response = self.client.patch('/rest/datasets', [test_data, second_test_data], format="json")
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        self.assertEqual('success' in response.data.keys(), True)
        self.assertEqual('failed' in response.data.keys(), True)
        self.assertEqual(len(response.data['success']), 1, 'success list should contain one item')
        self.assertEqual(len(response.data['failed']), 1, 'there should have been one failed element')
        self.assertEqual('detail' in response.data['failed'][0]['errors'], True, response.data['failed'][0]['errors'])
        self.assertEqual('identifying key' in response.data['failed'][0]['errors']['detail'][0], True, response.data['failed'][0]['errors'])

    #
    # header if-modified-since tests, single
    #

    def test_update_with_if_unmodified_since_header_ok(self):
        self.test_new_data['preservation_description'] = 'damn this is good coffee'
        cr = CatalogRecord.objects.get(pk=1)
        headers = { 'If-Unmodified-Since': cr.modified_by_api.strftime('%a, %d %b %Y %H:%M:%S %Z') }
        response = self.client.put('/rest/datasets/%s' % self.urn_identifier, self.test_new_data, headers=headers, format="json")
        self.assertEqual(response.status_code, status.HTTP_204_NO_CONTENT, response.data)

    def test_update_with_if_unmodified_since_header_error(self):
        self.test_new_data['preservation_description'] = 'the owls are not what they seem'
        headers = { 'If-Unmodified-Since': 'Wed, 23 Sep 2009 22:15:29 GMT' }
        response = self.client.put('/rest/datasets/%s' % self.urn_identifier, self.test_new_data, headers=headers, format="json")
        self.assertEqual(response.status_code, 412, 'http status should be 412 = precondition failed')

    #
    # header if-modified-since tests, list
    #

    def test_update_list_with_if_unmodified_since_header_ok(self):
        response = self.client.get('/rest/datasets/1', format="json")
        data_1 = response.data
        response = self.client.get('/rest/datasets/2', format="json")
        data_2 = response.data

        data_1['preservation_description'] = 'damn this is good coffee'
        data_2['preservation_description'] = 'damn this is good coffee also'

        headers = { 'If-Unmodified-Since': 'value is not checked' }
        response = self.client.put('/rest/datasets', [ data_1, data_2 ], headers=headers, format="json")

        self.assertEqual(response.status_code, status.HTTP_204_NO_CONTENT, response.data)

    def test_update_list_with_if_unmodified_since_header_error_1(self):
        """
        One resource being updated was updated in the meantime, resulting in an error
        """
        response = self.client.get('/rest/datasets/1', format="json")
        data_1 = response.data
        response = self.client.get('/rest/datasets/2', format="json")
        data_2 = response.data

        data_1['preservation_description'] = 'damn this is good coffee'

        # should result in error for this record
        data_2['modified_by_api'] = '2002-01-01T10:10:10.000000'

        headers = { 'If-Unmodified-Since': 'value is not checked' }
        response = self.client.put('/rest/datasets', [ data_1, data_2 ], headers=headers, format="json")
        self.assertEqual('modified' in response.data['failed'][0]['errors']['detail'][0], True, 'error should indicate resource has been modified')

    def test_update_list_with_if_unmodified_since_header_error_2(self):
        """
        Field modified_by_api is missing, while if-modified-since header is set, resulting in an error.
        """
        response = self.client.get('/rest/datasets/1', format="json")
        data_1 = response.data
        response = self.client.get('/rest/datasets/2', format="json")
        data_2 = response.data

        data_1['preservation_description'] = 'damn this is good coffee'

        # should result in error for this record
        data_2.pop('modified_by_api')

        headers = { 'If-Unmodified-Since': 'value is not checked' }
        response = self.client.patch('/rest/datasets', [ data_1, data_2 ], headers=headers, format="json")
        self.assertEqual('required' in response.data['failed'][0]['errors']['detail'][0], True, 'error should be about field modified_by_api is required')

    #
    #
    #
    # delete apis
    #
    #
    #

    def test_delete_catalog_record(self):
        url = '/rest/datasets/%s' % self.urn_identifier
        response = self.client.delete(url)
        self.assertEqual(response.status_code, status.HTTP_204_NO_CONTENT)
        response = self.client.get(url)
        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND)

        deleted_catalog_record = None

        try:
            deleted_catalog_record = CatalogRecord.objects.get(research_dataset__contains={ 'urn_identifier': self.urn_identifier })
        except CatalogRecord.DoesNotExist:
            pass

        if deleted_catalog_record:
            raise Exception('Deleted CatalogRecord should not be retrievable from the default objects table')

        try:
            deleted_catalog_record = CatalogRecord.objects_unfiltered.get(research_dataset__contains={ 'urn_identifier': self.urn_identifier })
        except CatalogRecord.DoesNotExist:
            raise Exception('Deleted CatalogRecord should not be deleted from the db, but marked as removed')

        self.assertEqual(deleted_catalog_record.removed, True)
        self.assertEqual(deleted_catalog_record.urn_identifier, self.urn_identifier)

    def test_delete_catalog_record_error_using_preferred_identifier(self):
        url = '/rest/datasets/%s' % self.preferred_identifier
        response = self.client.delete(url)
        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND)

    def test_delete_catalog_record_contract_is_not_deleted(self):
        catalog_record_from_test_data = self._get_object_from_test_data('catalogrecord', requested_index=1)
        url = '/rest/datasets/%s' % catalog_record_from_test_data['research_dataset']['urn_identifier']
        self.client.delete(url)
        response2 = self.client.get('/rest/contracts/%d' % catalog_record_from_test_data['contract'])
        self.assertEqual(response2.status_code, status.HTTP_200_OK, 'The contract of the CatalogRecord should not be deleted when deleting a single CatalogRecord.')

    def test_delete_catalog_record_not_found_with_search_by_owner(self):
        # owner of pk=1 is Default Owner. Delete pk=2 == first dataset owner by Rahikainen.
        # After deleting, first dataset owned by Rahikainen should be pk=3
        response = self.client.delete('/rest/datasets/2')
        response = self.client.get('/rest/datasets?curator=id:of:curator:rahikainen')
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data), 4)
        self.assertEqual(response.data[0]['id'], 3)

    #
    #
    #
    # proposetopas apis
    #
    #
    #

    def test_catalog_record_propose_to_pas_success(self):
        catalog_record_before = CatalogRecord.objects.get(pk=self.pk)
        catalog_record_before.research_dataset['ready_status'] = CatalogRecord.READY_STATUS_FINISHED
        catalog_record_before.save()

        response = self.client.post('/rest/datasets/%s/proposetopas?state=%d&contract=%s' %
            (
                self.urn_identifier,
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
                self.urn_identifier,
                self._get_object_from_test_data('contract', requested_index=0)['contract_json']['identifier']
            ),
            format="json")

        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual('state' in response.data, True, 'Response data should contain an error about the field')

    def test_catalog_record_propose_to_pas_wrong_parameter_state(self):
        response = self.client.post('/rest/datasets/%s/proposetopas?state=%d&contract=%s' %
            (
                self.urn_identifier,
                15,
                self._get_object_from_test_data('contract', requested_index=0)['contract_json']['identifier']
            ),
            format="json")

        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual('state' in response.data, True, 'Response data should contain an error about the field')

    def test_catalog_record_propose_to_pas_missing_parameter_contract(self):
        response = self.client.post('/rest/datasets/%s/proposetopas?state=%s' %
            (
                self.urn_identifier,
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
                self.urn_identifier,
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
                self.urn_identifier,
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
                self.urn_identifier,
                CatalogRecord.PRESERVATION_STATE_PROPOSED_MIDTERM,
                self._get_object_from_test_data('contract', requested_index=0)['contract_json']['identifier']
            ),
            format="json")

        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN)
        self.assertEqual('preservation_state' in response.data, True, 'Response data should contain an error about the field')

    #
    #
    #
    # internal helper methods
    #
    #
    #

    def _get_new_test_data(self):
        catalog_record_from_test_data = self._get_object_from_test_data('catalogrecord', requested_index=0)
        return {
            "contract": self._get_object_from_test_data('contract', requested_index=0),
            "data_catalog": self._get_object_from_test_data('datacatalog', requested_index=0),
            "research_dataset": {
                "urn_identifier": "pid:urn:new1",
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
                    "identifier": "http://lexvo.org/id/iso639-3/aar"
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
            "data_catalog": self._get_object_from_test_data('datacatalog', requested_index=0),
            "research_dataset": {
                "urn_identifier": "pid:urn:new2",
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
                    "identifier": "http://lexvo.org/id/iso639-3/aar"
                }],
                "total_byte_size": 1024,
                "ready_status": "Unfinished",
                "files": catalog_record_from_test_data['research_dataset']['files']
            }
        }
