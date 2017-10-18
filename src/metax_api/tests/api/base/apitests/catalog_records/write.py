from datetime import datetime, timedelta

from django.core.management import call_command
from rest_framework import status
from rest_framework.test import APITestCase

from metax_api.models import CatalogRecord, DataCatalog
from metax_api.tests.utils import test_data_file_path, TestClassUtils
from metax_api.utils import RedisSentinelCache


class CatalogRecordApiWriteCommon(APITestCase, TestClassUtils):

    """
    Common class for write tests, inherited by other write test classes
    """

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
        self.third_test_new_data = self._get_third_new_test_data()

        self._use_http_authorization()

    #
    #
    #
    # internal helper methods
    #
    #
    #

    def _get_new_test_data(self):
        catalog_record_from_test_data = self._get_object_from_test_data('catalogrecord', requested_index=0)
        catalog_record_from_test_data.update({
            "contract": self._get_object_from_test_data('contract', requested_index=0),
            "data_catalog": self._get_object_from_test_data('datacatalog', requested_index=0),
        })
        catalog_record_from_test_data['research_dataset'].update({
            "urn_identifier": "pid:urn:new1",
            "preferred_identifier": None,
            "creator": [{
                "@type": "Person",
                "name": "Teppo Testaaja"
            }],
            "curator": [{
                "@type": "Person",
                "name": "Default Owner"
            }],
            "total_byte_size": 1024,
            "files": catalog_record_from_test_data['research_dataset']['files']
        })
        return catalog_record_from_test_data

    def _get_second_new_test_data(self):
        catalog_record_from_test_data = self._get_new_test_data()
        catalog_record_from_test_data['research_dataset'].update({
            "urn_identifier": "pid:urn:new2",
        })
        return catalog_record_from_test_data

    def _get_third_new_test_data(self):
        """
        Returns one of the fuller generated test datasets
        """
        catalog_record_from_test_data = self._get_object_from_test_data('catalogrecord', requested_index=11)
        catalog_record_from_test_data.update({
            "contract": self._get_object_from_test_data('contract', requested_index=0),
            "data_catalog": self._get_object_from_test_data('datacatalog', requested_index=0)
        })
        catalog_record_from_test_data['research_dataset'].pop('urn_identifier')
        catalog_record_from_test_data['research_dataset'].pop('preferred_identifier')
        return catalog_record_from_test_data


class CatalogRecordApiWriteCreateTests(CatalogRecordApiWriteCommon):

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

    def test_create_catalog_record_json_validation_error_1(self):
        """
        Ensure the json path of the error is returned along with other details
        """
        self.test_new_data['research_dataset']["title"] = 1234456
        response = self.client.post('/rest/datasets', self.test_new_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual(len(response.data), 1, 'there should be only one error')
        self.assertEqual('research_dataset' in response.data.keys(), True, 'The error should concern the field research_dataset')
        self.assertEqual('1234456 is not of type' in response.data['research_dataset'][0], True, response.data)
        self.assertEqual('Json path: [\'title\']' in response.data['research_dataset'][0], True, response.data)

    def test_create_catalog_record_json_validation_error_2(self):
        """
        Ensure the json path of the error is returned along with other details also in
        objects that are deeply nested
        """
        self.test_new_data['research_dataset']['provenance'] = [{
            'title': { 'en': 'provenance title' },
            'was_associated_with': [
                {'@type': 'Person', 'xname': 'seppo'}
            ]
        }]
        response = self.client.post('/rest/datasets', self.test_new_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual(len(response.data), 1, 'there should be only one error')
        self.assertEqual('research_dataset' in response.data.keys(), True, 'The error should concern the field research_dataset')
        self.assertEqual('is not valid under any of the given schemas' in response.data['research_dataset'][0], True, response.data)
        self.assertEqual('was_associated_with' in response.data['research_dataset'][0], True, response.data)

    def test_create_catalog_record_dont_allow_data_catalog_fields_update(self):
        self.test_new_data['research_dataset']['preferred_identifier'] = 'urn:nbn:fi:csc-thisisanewurn'
        original_title = self.test_new_data['data_catalog']['catalog_json']['title']['en']
        self.test_new_data['data_catalog']['catalog_json']['title']['en'] = 'new title'

        response = self.client.post('/rest/datasets', self.test_new_data, format="json")

        self.assertEqual(response.status_code, status.HTTP_201_CREATED)
        self.assertEqual(response.data['data_catalog']['catalog_json']['title']['en'], original_title)
        data_catalog = DataCatalog.objects.get(pk=response.data['data_catalog']['id'])
        self.assertEqual(data_catalog.catalog_json['title']['en'], original_title)

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


class CatalogRecordApiWriteDatasetSchemaSelection(CatalogRecordApiWriteCommon):

    #
    #
    #
    # dataset schema selection related
    #
    #
    #

    def setUp(self):
        super(CatalogRecordApiWriteDatasetSchemaSelection, self).setUp()
        self._set_data_catalog_schema_to_harvester()

    def test_catalog_record_with_not_found_json_schema_defaults_to_att_schema(self):
        # catalog has dataset schema, but it is not found on the server
        dc = DataCatalog.objects.get(pk=1)
        dc.catalog_json['research_dataset_schema'] = 'nonexisting'
        dc.save()

        response = self.client.post('/rest/datasets', self.test_new_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)

        # catalog has no dataset schema at all
        dc = DataCatalog.objects.get(pk=1)
        dc.catalog_json.pop('research_dataset_schema')
        dc.save()

        response = self.client.post('/rest/datasets', self.test_new_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)

    def test_catalog_record_create_with_other_schema(self):
        """
        Ensure that dataset json schema validation works with other
        json schemas than the default ATT
        """
        self.test_new_data['research_dataset']['remote_resources'] = [
            { 'title': 'title' },
            { 'title': 'title' }
        ]

        response = self.client.post('/rest/datasets', self.test_new_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)

        self.test_new_data['research_dataset']['remote_resources'] = [
            { 'title': 'title' },
            { 'title': 'title' },
            { 'woah': 'this should give a failure, since title is a required field, and it is missing' }
        ]

        response = self.client.post('/rest/datasets', self.test_new_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST, response.data)

    def test_catalog_record_ref_data_validation_with_other_schema(self):
        """
        Ensure that dataset reference data validation and population works with other
        json schemas than the default ATT. Ref data validation should be schema agnostic
        """
        self.test_new_data['research_dataset']['remote_resources'] = [
            { 'title': 'title' },
            {
                'title': 'title',
                'checksum': {
                    'algorithm': 'SHA-512',
                    'checksum_value': 'xxxyyyzz'
                }
            },
        ]

        response = self.client.post('/rest/datasets', self.test_new_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)
        self.assertEqual(
            'purl' in response.data['research_dataset']['remote_resources'][1]['checksum']['algorithm'],
            True,
            'algorithm should have been populated with data from ref data'
        )

    def _set_data_catalog_schema_to_harvester(self):
        dc = DataCatalog.objects.get(pk=1)
        dc.catalog_json['research_dataset_schema'] = 'syke_harvester'
        dc.save()


class CatalogRecordApiWriteUpdateTests(CatalogRecordApiWriteCommon):

    #
    #
    # update apis PUT
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

    def test_update_catalog_record_dont_allow_data_catalog_fields_update(self):
        original_title = self.test_new_data['data_catalog']['catalog_json']['title']['en']
        self.test_new_data['data_catalog']['catalog_json']['title']['en'] = 'new title'
        self.test_new_data['research_dataset']['preferred_identifier'] = self.preferred_identifier

        response = self.client.put('/rest/datasets/%s' % self.urn_identifier, self.test_new_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_204_NO_CONTENT, response.data)
        data_catalog = DataCatalog.objects.get(pk=self.test_new_data['data_catalog']['id'])
        self.assertEqual(data_catalog.catalog_json['title']['en'], original_title)

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


class CatalogRecordApiWritePartialUpdateTests(CatalogRecordApiWriteCommon):

    #
    #
    # update apis PATCH
    #
    #

    def test_update_catalog_record_partial(self):
        new_data_catalog = self._get_object_from_test_data('datacatalog', requested_index=1)['id']
        new_data = {
            "data_catalog": new_data_catalog,
        }
        response = self.client.patch('/rest/datasets/%s' % self.urn_identifier, new_data, format="json")

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual('research_dataset' in response.data.keys(), True, 'PATCH operation should return full content')
        self.assertEqual(response.data['data_catalog']['id'], new_data_catalog, 'Field data_catalog was not updated')

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


class CatalogRecordApiWriteHTTPHeaderTests(CatalogRecordApiWriteCommon):

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

    def test_update_list_with_if_unmodified_since_header_error_3(self):
        """
        One resource being updated has never been modified before. Make sure that modified_by_api = None
        is an accepted value. The end result should be that the resource has been modified, since the
        server version has a timestamp set in modified_by_api.
        """
        response = self.client.get('/rest/datasets/1', format="json")
        data_1 = response.data
        response = self.client.get('/rest/datasets/2', format="json")
        data_2 = response.data

        data_1['preservation_description'] = 'damn this is good coffee'
        data_2['preservation_description'] = 'damn this is good coffee also'
        data_2['modified_by_api'] = None

        headers = { 'If-Unmodified-Since': 'value is not checked' }
        response = self.client.put('/rest/datasets', [ data_1, data_2 ], headers=headers, format="json")
        self.assertEqual('modified' in response.data['failed'][0]['errors']['detail'][0], True, 'error should indicate resource has been modified')


class CatalogRecordApiWriteDeleteTests(CatalogRecordApiWriteCommon):

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
        self.assertEqual(response.data['results'][0]['id'], 3)


class CatalogRecordApiWriteProposeToPasTests(CatalogRecordApiWriteCommon):

    #
    #
    #
    # proposetopas apis
    #
    #
    #

    def test_catalog_record_propose_to_pas_success(self):
        catalog_record_before = CatalogRecord.objects.get(pk=self.pk)
        catalog_record_before.ready_status = CatalogRecord.READY_STATUS_FINISHED
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
        catalog_record_before.ready_status = CatalogRecord.READY_STATUS_FINISHED
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
        catalog_record_before.ready_status = CatalogRecord.READY_STATUS_UNFINISHED
        catalog_record_before.save()

        response = self.client.post('/rest/datasets/%s/proposetopas?state=%d&contract=%s' %
            (
                self.urn_identifier,
                CatalogRecord.PRESERVATION_STATE_PROPOSED_MIDTERM,
                self._get_object_from_test_data('contract', requested_index=0)['contract_json']['identifier']
            ),
            format="json")

        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN)
        self.assertEqual('ready_status' in response.data, True, 'Response data should contain an error about the field')

    def test_catalog_record_propose_to_pas_wrong_preservation_state(self):
        catalog_record_before = CatalogRecord.objects.get(pk=self.pk)
        catalog_record_before.ready_status = CatalogRecord.READY_STATUS_FINISHED
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


class CatalogRecordApiWriteReferenceDataTests(CatalogRecordApiWriteCommon):

    """
    Tests related to reference_data validation and dataset fields population
    from reference_data, according to given uri or code as the value.
    """

    def test_catalog_record_reference_data_missing_ok(self):
        """
        The API should attempt to reload the reference data if it is missing from
        cache for whatever reason, and successfully finish the request
        """
        cache = RedisSentinelCache()
        cache.delete('reference_data')
        self.assertEqual(cache.get('reference_data', master=True), None, 'cache ref data should be missing after cache.delete()')

        response = self.client.post('/rest/datasets', self.test_new_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)

    def test_create_catalog_record_with_invalid_reference_data(self):
        rd = self.third_test_new_data['research_dataset']
        rd['theme'][0]['identifier'] = 'nonexisting'
        rd['field_of_science'][0]['identifier'] = 'nonexisting'
        rd['remote_resources'][0]['checksum']['algorithm'] = 'nonexisting'
        rd['remote_resources'][0]['license'][0]['identifier'] = 'nonexisting'
        rd['remote_resources'][0]['type']['identifier'] = 'nonexisting'
        rd['language'][0]['identifier'] = 'nonexisting'
        rd['access_rights']['type'][0]['identifier'] = 'nonexisting'
        rd['access_rights']['license'][0]['identifier'] = 'nonexisting'
        rd['is_output_of'][0]['source_organization'][0]['identifier'] = 'nonexisting'
        rd['other_identifier'][0]['type']['identifier'] = 'nonexisting'
        rd['spatial'][0]['place_uri'][0]['identifier'] = 'nonexisting'
        rd['files'][0]['type']['identifier'] = 'nonexisting'
        response = self.client.post('/rest/datasets', self.third_test_new_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual('research_dataset' in response.data.keys(), True)
        self.assertEqual(len(response.data['research_dataset']), 12)

    def test_create_catalog_record_populate_fields_from_reference_data(self):
        """
        1) Insert codes from cached reference data to dataset identifier fields
           that will be validated, and then populated
        2) Check that that the values in dataset identifier fields are changed from
           codes to uris after a successful create
        3) Check that labels have also been copied to datasets to their approriate fields
        """
        from metax_api.utils import RedisSentinelCache

        # noticed that these data types in reference data are currently not being used to
        # validate anything:
        # access_restriction_grounds_type
        # contributor_role
        # funder_type = 'tekes'
        # mime_type
        # research_infra

        cache = RedisSentinelCache()
        refdata = cache.get('reference_data')['reference_data']
        orgdata = cache.get('reference_data')['organization_data']
        refs = {}

        data_types = [
            'access_type',
            'checksum_algorithm',
            'field_of_science',
            'identifier_type',
            'keyword',
            'language',
            'license',
            'location',
            # 'organization', # handled separately since in difference place
            'resource_type',
        ]

        # the values in these selected entries will be used throghout the rest of the test case
        for dtype in data_types:
            entry = refdata[dtype][0]
            refs[dtype] = {
                'code': entry['code'],
                'uri': entry['uri'],
                'label': entry.get('label', None),
            }

        refs['organization'] = {
            'uri': orgdata['organization'][0]['uri'],
            'code': orgdata['organization'][0]['code'],
            'label': orgdata['organization'][0]['label'],
        }

        # replace the relations with objects that have only the identifier set with code as value,
        # to easily check that label was populated (= that it appeared in the dataset after create)
        # without knowing its original value from the generated test data
        rd = self.third_test_new_data['research_dataset']
        rd['theme'][0]                    = { 'identifier': refs['keyword']['code'] }
        rd['field_of_science'][0]         = { 'identifier': refs['field_of_science']['code'] }
        rd['language'][0]                 = { 'identifier': refs['language']['code'] }
        rd['access_rights']['type'][0]    = { 'identifier': refs['access_type']['code'] }
        rd['access_rights']['license'][0] = { 'identifier': refs['license']['code'] }
        rd['other_identifier'][0]['type'] = { 'identifier': refs['identifier_type']['code'] }
        rd['spatial'][0]['place_uri'][0]  = { 'identifier': refs['location']['code'] }
        rd['files'][0]['type']            = { 'identifier': refs['resource_type']['code'] }
        rd['remote_resources'][0]['type'] = { 'identifier': refs['resource_type']['code'] }
        rd['remote_resources'][0]['license'][0] = { 'identifier': refs['license']['code'] }

        # these have other required fields, so only update the identifier with code
        rd['remote_resources'][0]['checksum']['algorithm']            = refs['checksum_algorithm']['code']
        rd['is_output_of'][0]['source_organization'][0]['identifier'] = refs['organization']['code']

        response = self.client.post('/rest/datasets', self.third_test_new_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)
        self.assertEqual('research_dataset' in response.data.keys(), True)

        new_rd = response.data['research_dataset']
        self._assert_uri_copied_to_identifier(refs, new_rd)
        self._assert_label_copied_to_pref_label(refs, new_rd)
        self._assert_label_copied_to_title(refs, new_rd)
        self._assert_label_copied_to_others(refs, new_rd)

    def _assert_uri_copied_to_identifier(self, refs, new_rd):
        self.assertEqual(refs['keyword']['uri'],          new_rd['theme'][0]['identifier'])
        self.assertEqual(refs['field_of_science']['uri'], new_rd['field_of_science'][0]['identifier'])
        self.assertEqual(refs['language']['uri'],         new_rd['language'][0]['identifier'])
        self.assertEqual(refs['access_type']['uri'],      new_rd['access_rights']['type'][0]['identifier'])
        self.assertEqual(refs['license']['uri'],          new_rd['access_rights']['license'][0]['identifier'])
        self.assertEqual(refs['identifier_type']['uri'],  new_rd['other_identifier'][0]['type']['identifier'])
        self.assertEqual(refs['location']['uri'],         new_rd['spatial'][0]['place_uri'][0]['identifier'])
        self.assertEqual(refs['resource_type']['uri'],    new_rd['files'][0]['type']['identifier'])
        self.assertEqual(refs['resource_type']['uri'],    new_rd['remote_resources'][0]['type']['identifier'])
        self.assertEqual(refs['license']['uri'],          new_rd['remote_resources'][0]['license'][0]['identifier'])
        self.assertEqual(refs['organization']['uri'],     new_rd['is_output_of'][0]['source_organization'][0]['identifier'])
        self.assertEqual(refs['checksum_algorithm']['uri'], new_rd['remote_resources'][0]['checksum']['algorithm'])

    def _assert_label_copied_to_pref_label(self, refs, new_rd):
        self.assertEqual(refs['keyword']['label'],          new_rd['theme'][0].get('pref_label', None))
        self.assertEqual(refs['field_of_science']['label'], new_rd['field_of_science'][0].get('pref_label', None))
        self.assertEqual(refs['access_type']['label'],      new_rd['access_rights']['type'][0].get('pref_label', None))
        self.assertEqual(refs['identifier_type']['label'],  new_rd['other_identifier'][0]['type'].get('pref_label', None))
        self.assertEqual(refs['location']['label'],         new_rd['spatial'][0]['place_uri'][0].get('pref_label', None))
        self.assertEqual(refs['resource_type']['label'],    new_rd['files'][0]['type'].get('pref_label', None))
        self.assertEqual(refs['resource_type']['label'],    new_rd['remote_resources'][0]['type'].get('pref_label', None))

    def _assert_label_copied_to_title(self, refs, new_rd):
        self.assertEqual(refs['language']['label'], new_rd['language'][0].get('title', None))
        self.assertEqual(refs['license']['label'],  new_rd['access_rights']['license'][0].get('title', None))
        self.assertEqual(refs['license']['label'],  new_rd['remote_resources'][0]['license'][0].get('title', None))

    def _assert_label_copied_to_others(self, refs, new_rd):
        # the black sheep
        self.assertEqual(refs['organization']['label']['default'],     new_rd['is_output_of'][0]['source_organization'][0].get('name', None))
        self.assertEqual(refs['checksum_algorithm']['label'], new_rd['remote_resources'][0]['checksum'].get('checksum_algorithm', None))
