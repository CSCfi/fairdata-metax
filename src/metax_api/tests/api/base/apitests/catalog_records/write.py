from datetime import timedelta

from django.core.management import call_command
from rest_framework import status
from rest_framework.test import APITestCase

from metax_api.models import AlternateRecordSet, CatalogRecord, DataCatalog, File
from metax_api.tests.utils import test_data_file_path, TestClassUtils
from metax_api.utils import RedisSentinelCache, get_tz_aware_now_without_micros


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
            "urn_identifier": "urn:nbn:fi:att:ec55c1dd-668d-43ae-b51b-f6c56a5bd4d6",
            "preferred_identifier": None,
            "creator": [{
                "@type": "Person",
                "name": "Teppo Testaaja",
                "member_of": {
                    "@type": "Organization",
                    "name": {"fi": "Mysterious Organization"}
                }
            }],
            "curator": [{
                "@type": "Person",
                "name": "Default Owner",
                "member_of": {
                    "@type": "Organization",
                    "name": {"fi": "Mysterious Organization"}
                }
            }],
            "total_byte_size": 1024,
            "files": catalog_record_from_test_data['research_dataset']['files']
        })
        return catalog_record_from_test_data

    def _get_second_new_test_data(self):
        catalog_record_from_test_data = self._get_new_test_data()
        catalog_record_from_test_data['research_dataset'].update({
            "urn_identifier": "urn:nbn:fi:att:5cd4d4f9-9583-422e-9946-990c8ea96781"
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
        self.assertEqual(response.data['research_dataset']['urn_identifier'] is not None, True,
                         'urn_identifier should have been generated')
        self.assertEqual(response.data['research_dataset']['preferred_identifier'],
                         self.test_new_data['research_dataset']['preferred_identifier'])
        cr = CatalogRecord.objects.get(pk=response.data['id'])
        self.assertEqual(cr.date_created >= get_tz_aware_now_without_micros() - timedelta(seconds=5), True,
                         'Timestamp should have been updated during object creation')

    def test_create_catalog_record_without_preferred_identifier(self):
        self.test_new_data['research_dataset']['preferred_identifier'] = None
        response = self.client.post('/rest/datasets', self.test_new_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)
        self.assertEqual('research_dataset' in response.data.keys(), True)
        self.assertEqual(response.data['research_dataset']['preferred_identifier'],
                         response.data['research_dataset']['urn_identifier'],
                         'urn_identifier and preferred_identifier should equal')
        cr = CatalogRecord.objects.get(pk=response.data['id'])
        self.assertEqual(cr.date_created >= get_tz_aware_now_without_micros() - timedelta(seconds=5), True,
                         'Timestamp should have been updated during object creation')

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
        self.assertEqual('research_dataset' in response.data.keys(), True,
                         'The error should concern the field research_dataset')
        self.assertEqual('1234456 is not of type' in response.data['research_dataset'][0], True, response.data)
        self.assertEqual('Json path: [\'title\']' in response.data['research_dataset'][0], True, response.data)

    def test_create_catalog_record_json_validation_error_2(self):
        """
        Ensure the json path of the error is returned along with other details also in
        objects that are deeply nested
        """
        self.test_new_data['research_dataset']['provenance'] = [{
            'title': {'en': 'provenance title'},
            'was_associated_with': [
                {'@type': 'Person', 'xname': 'seppo'}
            ]
        }]
        response = self.client.post('/rest/datasets', self.test_new_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual(len(response.data), 1, 'there should be only one error')
        self.assertEqual('research_dataset' in response.data.keys(), True,
                         'The error should concern the field research_dataset')
        self.assertEqual('is not valid' in response.data['research_dataset'][0], True, response.data)
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
        self.assertEqual('research_dataset' in response.data['failed'][0]['errors'], True,
                         'The error should have been about an already existing identifier')

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


class CatalogRecordApiWriteIdentifierUniqueness(CatalogRecordApiWriteCommon):
    """
    Tests related to checking preferred_identifier uniqueness. Topics of interest:
    - when saving to ATT catalog, preferred_identifier already existing in the ATT
      catalog is fine (other versions of the same record).
    - when saving to ATT catalog, preferred_identifier already existing in OTHER
      catalogs is an error (ATT catalog should only have "new" records).
    - when saving to OTHER catalogs than ATT, preferred_identifier already existing
      in any other catalog is fine (the same record can be harvested from multiple
      sources).
    """

    #
    # create operations
    #

    def test_create_catalog_record_error_preferred_identifier_cant_be_urn_identifier(self):
        """
        preferred_identifier can never be the same as a urn_identifier in another cr, in any catalog
        """
        existing_urn_identifier = CatalogRecord.objects.get(pk=1).research_dataset['urn_identifier']
        self.test_new_data['research_dataset']['preferred_identifier'] = existing_urn_identifier

        response = self.client.post('/rest/datasets', self.test_new_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual('research_dataset' in response.data.keys(), True,
                         'The error should be about an error in research_dataset')

        # the error message should clearly state that the value of preferred_identifier appears in the
        # field urn_identifier in another record, therefore two asserts
        self.assertEqual('preferred_identifier' in response.data['research_dataset'][0], True,
                         'The error should be about urn_identifier existing with this identifier')
        self.assertEqual('urn_identifier' in response.data['research_dataset'][0], True,
                         'The error should be about urn_identifier existing with this identifier')

    def test_create_catalog_record_error_preferred_identifier_exists_in_same_catalog(self):
        """
        preferred_identifier already existing in the same data catalog is an error
        """
        unique_identifier = self._set_preferred_identifier_to_record(pk=1, catalog_id=1)
        self.test_new_data['research_dataset']['preferred_identifier'] = unique_identifier

        response = self.client.post('/rest/datasets', self.test_new_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual('research_dataset' in response.data.keys(), True,
                         'The error should be about an error in research_dataset')
        self.assertEqual('preferred_identifier' in response.data['research_dataset'][0], True,
                         'The error should be about preferred_identifier already existing')

    def test_create_catalog_record_preferred_identifier_exists_in_another_catalog(self):
        """
        preferred_identifier existing in another data catalog is not an error.
        """
        unique_identifier = self._set_preferred_identifier_to_record(pk=1, catalog_id=1)
        self.test_new_data['research_dataset']['preferred_identifier'] = unique_identifier

        # different catalog, should be OK (not ATT catalog, so preferred_identifier being saved
        # can exist in other catalogs)
        self.test_new_data['data_catalog'] = 3

        response = self.client.post('/rest/datasets', self.test_new_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)

    def test_create_catalog_record_to_att_preferred_identifier_exists_in_another_catalog(self):
        """
        preferred_identifier existing in another data catalog IS an error, when saving to ATT
        catalog.
        """
        pref_id = 'abcdefghijklmop'

        # save a record to catalog #2 using pref_id
        self.test_new_data['research_dataset']['preferred_identifier'] = pref_id
        self.test_new_data['data_catalog'] = 2
        response = self.client.post('/rest/datasets', self.test_new_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED)

        # save a record to catalog #1 (ATT catalog) using the same pref_id. this should be an error
        self.test_new_data['research_dataset']['preferred_identifier'] = pref_id
        self.test_new_data['data_catalog'] = 1
        response = self.client.post('/rest/datasets', self.test_new_data, format="json")

        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual('research_dataset' in response.data.keys(), True,
                         'The error should be about an error in research_dataset')
        self.assertEqual('preferred_identifier' in response.data['research_dataset'][0], True,
                         'The error should be about preferred_identifier already existing')
        self.assertEqual('saving to ATT' in response.data['research_dataset'][0], True,
                         'The error should mention saving to ATT catalog as the reason')

    #
    # update operations
    #

    def test_update_catalog_record_preferred_identifier_exists_in_another_catalog_1(self):
        """
        preferred_identifier existing in another data catalog is not an error.

        Test PATCH, when data_catalog of the record being updated is already
        different than another record's which has the same identifier.
        """
        unique_identifier = self._set_preferred_identifier_to_record(pk=1, catalog_id=1)

        cr = CatalogRecord.objects.get(pk=2)
        cr.data_catalog_id = 2
        cr.save()

        data = {'research_dataset': self.test_new_data['research_dataset']}
        data['research_dataset']['preferred_identifier'] = unique_identifier

        response = self.client.patch('/rest/datasets/2', data, format="json")
        self.assertEqual(response.status_code, status.HTTP_200_OK)

    def test_update_catalog_record_preferred_identifier_exists_in_another_catalog_2(self):
        """
        preferred_identifier existing in another data catalog is not an error.

        Test PATCH, when data_catalog is being updated to a different catalog
        in the same request. In this case, the uniqueness check has to be executed
        on the new data_catalog being passed.

        In this test, catalog is updated to 2, which should not contain a conflicting
        identifier.
        """
        unique_identifier = self._set_preferred_identifier_to_record(pk=1, catalog_id=1)

        data = {'research_dataset': self.test_new_data['research_dataset']}
        data['research_dataset']['preferred_identifier'] = unique_identifier
        data['data_catalog'] = 2

        response = self.client.patch('/rest/datasets/2', data, format="json")
        self.assertEqual(response.status_code, status.HTTP_200_OK)

    def test_update_catalog_record_preferred_identifier_exists_in_another_catalog_3(self):
        """
        preferred_identifier already existing in the same data catalog is an error,
        in other catalogs than ATT: Harvester or other catalogs cant contain same
        preferred_identifier twice.

        Test PATCH, when data_catalog is being updated to a different catalog
        in the same request. In this case, the uniqueness check has to be executed
        on the new data_catalog being passed.

        In this test, catalog is updated to 3, which should contain a conflicting
        identifier, resulting in an error.
        """

        # setup the record in db which will cause conflict
        unique_identifier = self._set_preferred_identifier_to_record(pk=3, catalog_id=3)

        data = {'research_dataset': self.test_new_data['research_dataset']}
        data['research_dataset']['preferred_identifier'] = unique_identifier
        data['data_catalog'] = 3

        response = self.client.patch('/rest/datasets/2', data, format="json")
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual('preferred_identifier' in response.data['research_dataset'][0], True,
                         'The error should be about preferred_identifier already existing')

    def test_update_catalog_record_in_att_preferred_identifier_exists_in_another_catalog(self):
        """
        when saving to ATT catalog, preferred_identifier existing in another data
        catalog IS an error.

        Update an existing record in catalog #1 to have pref_id x, when a record in catalog #2
        already has the same pref_id x. This should be an error, since records in the ATT catalog
        should be "new".
        """
        # setup the record that will cause conflict
        unique_identifier = self._set_preferred_identifier_to_record(pk=2, catalog_id=2)

        data = {'research_dataset': self.test_new_data['research_dataset']}
        data['research_dataset']['preferred_identifier'] = unique_identifier

        response = self.client.patch('/rest/datasets/1', data, format="json")
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual('research_dataset' in response.data.keys(), True,
                         'The error should be about an error in research_dataset')
        self.assertEqual('preferred_identifier' in response.data['research_dataset'][0], True,
                         'The error should be about preferred_identifier already existing')
        self.assertEqual('saving to ATT' in response.data['research_dataset'][0], True,
                         'The error should mention saving to ATT catalog as the reason')

    def test_update_catalog_record_in_att_multiple_preferred_identifiers_are_allowed(self):
        """
        When saving to ATT catalog, multiple same preferred_identifier values already existing in
        the same data catalog is OK, as records which are versions of each other can have
        the same preferred_identifier.

        Test PATCH, when updating a record in ATT catalog, and another record already has the
        same preferred_identifier.
        """
        target_catalog = 1
        unique_identifier = self._set_preferred_identifier_to_record(pk=1, catalog_id=target_catalog)

        # set data to update another record to have the same preferred_identifier and catalog
        # as another already existing record. in ATT catalog, that should be fine.
        data = {'research_dataset': self.test_new_data['research_dataset']}
        data['research_dataset']['preferred_identifier'] = unique_identifier
        data['data_catalog'] = target_catalog

        response = self.client.patch('/rest/datasets/2', data, format="json")
        self.assertEqual(response.status_code, status.HTTP_200_OK)

        new_version = CatalogRecord.objects.get(pk=response.data['next_version']['id'])
        self.assertEqual(new_version.research_dataset['preferred_identifier'], unique_identifier)
        self.assertEqual(new_version.data_catalog_id, target_catalog)

    #
    # helpers
    #

    def _set_preferred_identifier_to_record(self, pk=None, catalog_id=None):
        """
        Set preferred_identifier to an existing record to a value, and return that value,
        which will then be used by the test to create or update another record.
        """
        unique_identifier = 'im unique yo'
        cr = CatalogRecord.objects.get(pk=pk)
        cr.research_dataset['preferred_identifier'] = unique_identifier
        cr.data_catalog_id = catalog_id
        cr.force_save()
        cr._handle_preferred_identifier_changed()
        return unique_identifier


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
            {'title': 'title'},
            {'title': 'title'}
        ]

        response = self.client.post('/rest/datasets', self.test_new_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)

        self.test_new_data['research_dataset']['remote_resources'] = [
            {'title': 'title'},
            {'title': 'title'},
            {'woah': 'this should give a failure, since title is a required field, and it is missing'}
        ]

        response = self.client.post('/rest/datasets', self.test_new_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST, response.data)

    def test_catalog_record_ref_data_validation_with_other_schema(self):
        """
        Ensure that dataset reference data validation and population works with other
        json schemas than the default ATT. Ref data validation should be schema agnostic
        """
        self.test_new_data['research_dataset']['other_identifier'] = [
            {
                'notation': 'urn:1',
                'type': {
                    'identifier': 'doi',
                }
            }
        ]

        response = self.client.post('/rest/datasets', self.test_new_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)
        self.assertEqual(
            'purl' in response.data['research_dataset']['other_identifier'][0]['type']['identifier'],
            True,
            'Identifier type should have been populated with data from ref data'
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
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        cr = CatalogRecord.objects.get(pk=self.pk)
        self.assertEqual(cr.date_modified >= get_tz_aware_now_without_micros() - timedelta(seconds=5), True,
                         'Timestamp should have been updated during object update')

    def test_update_catalog_record_error_using_preferred_identifier(self):
        self.test_new_data['research_dataset']['preferred_identifier'] = self.preferred_identifier
        response = self.client.put('/rest/datasets/%s' % self.preferred_identifier, self.test_new_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND,
                         'Update operation should return 404 when using preferred_identifier')

    def test_update_catalog_record_error_required_fields(self):
        """
        Field 'research_dataset' is missing, which should result in an error, since PUT
        replaces an object and requires all 'required' fields to be present.
        """
        self.test_new_data.pop('research_dataset')
        response = self.client.put('/rest/datasets/%s' % self.urn_identifier, self.test_new_data, format="json")

        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual('research_dataset' in response.data.keys(), True,
                         'Error for field \'research_dataset\' is missing from response.data')

    def test_update_catalog_record_dont_allow_data_catalog_fields_update(self):
        original_title = self.test_new_data['data_catalog']['catalog_json']['title']['en']
        self.test_new_data['data_catalog']['catalog_json']['title']['en'] = 'new title'
        self.test_new_data['research_dataset']['preferred_identifier'] = self.preferred_identifier

        response = self.client.put('/rest/datasets/%s' % self.urn_identifier, self.test_new_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
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
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        cr2 = CatalogRecord.objects.get(pk=cr_id)
        new_contract_identifier = cr2.contract.contract_json['identifier']
        self.assertNotEqual(old_contract_identifier, new_contract_identifier, 'Contract identifier should have changed')

    #
    # update preservation_state operations
    #

    def test_update_catalog_record_pas_state_allowed_value(self):
        self.test_new_data['preservation_state'] = 3
        response = self.client.put('/rest/datasets/%s' % self.urn_identifier, self.test_new_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_200_OK)

    def test_update_catalog_record_pas_state_unallowed_value(self):
        self.test_new_data['preservation_state'] = 111
        response = self.client.put('/rest/datasets/%s' % self.urn_identifier, self.test_new_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST,
                         'HTTP status should be 400 due to invalid value')
        self.assertEqual('preservation_state' in response.data.keys(), True,
                         'The error should mention the field preservation_state')

    def test_update_catalog_record_preservation_state_modified_is_updated(self):
        self.test_new_data['preservation_state'] = 4
        response = self.client.put('/rest/datasets/%s' % self.urn_identifier, self.test_new_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        cr = CatalogRecord.objects.get(pk=self.pk)
        self.assertEqual(cr.preservation_state_modified >= get_tz_aware_now_without_micros() - timedelta(seconds=5),
                         True, 'Timestamp should have been updated during object update')

    #
    # update list operations PUT
    #

    def test_catalog_record_update_list(self):
        self.test_new_data['id'] = 1
        self.test_new_data['preservation_description'] = 'updated description'

        self.second_test_new_data['id'] = 2
        self.second_test_new_data['preservation_description'] = 'second updated description'

        response = self.client.put('/rest/datasets', [self.test_new_data, self.second_test_new_data], format="json")
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        self.assertEqual(len(response.data['success']), 2)

        updated_cr = CatalogRecord.objects.get(pk=1)
        self.assertEqual(updated_cr.preservation_description, 'updated description')
        updated_cr = CatalogRecord.objects.get(pk=2)
        self.assertEqual(updated_cr.preservation_description, 'second updated description')

    def test_catalog_record_update_list_error_one_fails(self):
        self.test_new_data['id'] = 1
        self.test_new_data['preservation_description'] = 'updated description'

        # data catalog is a required field, should therefore fail
        self.second_test_new_data.pop('data_catalog', None)
        self.second_test_new_data['id'] = 2

        response = self.client.put('/rest/datasets', [self.test_new_data, self.second_test_new_data], format="json")
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        self.assertEqual('success' in response.data.keys(), True)
        self.assertEqual('failed' in response.data.keys(), True)
        self.assertEqual(isinstance(response.data['success'], list), True,
                         'return data should contain key success, which is a list')
        self.assertEqual(len(response.data['success']), 1)
        self.assertEqual(len(response.data['failed']), 1)

        updated_cr = CatalogRecord.objects.get(pk=1)
        self.assertEqual(updated_cr.preservation_description, 'updated description')

    def test_catalog_record_update_list_error_key_not_found(self):
        # does not have identifier key
        self.test_new_data['research_dataset'].pop('urn_identifier')
        self.test_new_data['preservation_description'] = 'updated description'

        self.second_test_new_data['id'] = 2
        self.second_test_new_data['preservation_description'] = 'second updated description'

        response = self.client.put('/rest/datasets', [self.test_new_data, self.second_test_new_data], format="json")
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        self.assertEqual('success' in response.data.keys(), True)
        self.assertEqual('failed' in response.data.keys(), True)
        self.assertEqual(len(response.data['success']), 1)
        self.assertEqual(len(response.data['failed']), 1)


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
        # import ipdb; ipdb.sset_trace()
        response = self.client.patch('/rest/datasets/%s' % self.urn_identifier, new_data, format="json")

        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
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
        self.assertEqual('research_dataset' in response.data['success'][0]['object'], True,
                         'response.data should contain full objects')

        updated_cr = CatalogRecord.objects.get(pk=1)
        self.assertEqual(updated_cr.preservation_state, 1, 'preservation state should have changed to 1')

    def test_catalog_record_partial_update_list_error_one_fails(self):
        test_data = {}
        test_data['id'] = 1
        test_data['preservation_state'] = 1

        second_test_data = {}
        second_test_data['preservation_state'] = 555  # value not allowed
        second_test_data['id'] = 2

        response = self.client.patch('/rest/datasets', [test_data, second_test_data], format="json")
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        self.assertEqual('success' in response.data.keys(), True)
        self.assertEqual('failed' in response.data.keys(), True)
        self.assertEqual(len(response.data['success']), 1, 'success list should contain one item')
        self.assertEqual(len(response.data['failed']), 1, 'there should have been one failed element')
        self.assertEqual('preservation_state' in response.data['failed'][0]['errors'], True,
                         response.data['failed'][0]['errors'])

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
        self.assertEqual('identifying key' in response.data['failed'][0]['errors']['detail'][0], True,
                         response.data['failed'][0]['errors'])


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
            deleted_catalog_record = CatalogRecord.objects.get(
                research_dataset__contains={'urn_identifier': self.urn_identifier})
        except CatalogRecord.DoesNotExist:
            pass

        if deleted_catalog_record:
            raise Exception('Deleted CatalogRecord should not be retrievable from the default objects table')

        try:
            deleted_catalog_record = CatalogRecord.objects_unfiltered.get(
                research_dataset__contains={'urn_identifier': self.urn_identifier})
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
        self.assertEqual(response2.status_code, status.HTTP_200_OK,
                         'The contract of CatalogRecord should not be deleted when deleting a single CatalogRecord.')

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
        catalog_record_before.save()

        response = self.client.post('/rest/datasets/%s/proposetopas?state=%d&contract=%s' %
                                    (
                                        self.urn_identifier,
                                        CatalogRecord.PRESERVATION_STATE_PROPOSED_MIDTERM,
                                        self._get_object_from_test_data('contract', requested_index=0)['contract_json'][
                                            'identifier']
                                    ),
                                    format="json")

        self.assertEqual(response.status_code, status.HTTP_204_NO_CONTENT)
        catalog_record_after = CatalogRecord.objects.get(pk=self.pk)
        self.assertEqual(catalog_record_after.preservation_state, CatalogRecord.PRESERVATION_STATE_PROPOSED_MIDTERM)

    def test_catalog_record_propose_to_pas_missing_parameter_state(self):
        response = self.client.post('/rest/datasets/%s/proposetopas?contract=%s' %
                                    (
                                        self.urn_identifier,
                                        self._get_object_from_test_data('contract', requested_index=0)['contract_json'][
                                            'identifier']
                                    ),
                                    format="json")

        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual('state' in response.data, True, 'Response data should contain an error about the field')

    def test_catalog_record_propose_to_pas_wrong_parameter_state(self):
        response = self.client.post('/rest/datasets/%s/proposetopas?state=%d&contract=%s' %
                                    (
                                        self.urn_identifier,
                                        15,
                                        self._get_object_from_test_data('contract', requested_index=0)['contract_json'][
                                            'identifier']
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

    def test_catalog_record_propose_to_pas_wrong_preservation_state(self):
        catalog_record_before = CatalogRecord.objects.get(pk=self.pk)
        catalog_record_before.preservation_state = CatalogRecord.PRESERVATION_STATE_IN_LONGTERM_PAS
        catalog_record_before.save()

        response = self.client.post('/rest/datasets/%s/proposetopas?state=%d&contract=%s' %
                                    (self.urn_identifier, CatalogRecord.PRESERVATION_STATE_PROPOSED_MIDTERM,
                                     self._get_object_from_test_data('contract', requested_index=0)['contract_json'][
                                         'identifier']), format="json")

        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN)
        self.assertEqual('preservation_state' in response.data, True,
                         'Response data should contain an error about the field')


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
        self.assertEqual(cache.get('reference_data', master=True), None,
                         'cache ref data should be missing after cache.delete()')

        response = self.client.post('/rest/datasets', self.test_new_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)

    def test_create_catalog_record_with_invalid_reference_data(self):
        rd = self.third_test_new_data['research_dataset']
        rd['theme'][0]['identifier'] = 'nonexisting'
        rd['field_of_science'][0]['identifier'] = 'nonexisting'
        rd['remote_resources'][0]['checksum']['algorithm'] = 'nonexisting'
        rd['remote_resources'][0]['license'][0]['identifier'] = 'nonexisting'
        rd['remote_resources'][0]['resource_type']['identifier'] = 'nonexisting'
        rd['remote_resources'][0]['use_category']['identifier'] = 'nonexisting'
        rd['language'][0]['identifier'] = 'nonexisting'
        rd['access_rights']['access_type']['identifier'] = 'nonexisting'
        rd['access_rights']['license'][0]['identifier'] = 'nonexisting'
        rd['other_identifier'][0]['type']['identifier'] = 'nonexisting'
        rd['spatial'][0]['place_uri']['identifier'] = 'nonexisting'
        rd['files'][0]['file_type']['identifier'] = 'nonexisting'
        rd['files'][0]['use_category']['identifier'] = 'nonexisting'
        rd['infrastructure'][0]['identifier'] = 'nonexisting'
        rd['creator'][0]['contributor_role']['identifier'] = 'nonexisting'
        rd['is_output_of'][0]['funder_type']['identifier'] = 'nonexisting'
        rd['directories'][0]['use_category']['identifier'] = 'nonexisting'
        rd['relation'][0]['relation_type']['identifier'] = 'nonexisting'
        rd['provenance'][0]['type']['identifier'] = 'nonexisting'
        rd['provenance'][1]['type']['identifier'] = 'nonexisting'
        response = self.client.post('/rest/datasets', self.third_test_new_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual('research_dataset' in response.data.keys(), True)
        self.assertEqual(len(response.data['research_dataset']), 19)
        self.assertEqual(len(response.data['research_dataset']['research_dataset.activity.type.identifier']), 2)

    def test_create_catalog_record_populate_fields_from_reference_data(self):
        """
        1) Insert codes from cached reference data to dataset identifier fields
           that will be validated, and then populated
        2) Check that that the values in dataset identifier fields are changed from
           codes to uris after a successful create
        3) Check that labels have also been copied to datasets to their approriate fields
        """
        from metax_api.utils import RedisSentinelCache

        cache = RedisSentinelCache()
        refdata = cache.get('reference_data')['reference_data']
        orgdata = cache.get('reference_data')['organization_data']
        refs = {}

        data_types = [
            'access_type',
            'restriction_grounds',
            'checksum_algorithm',
            'field_of_science',
            'identifier_type',
            'keyword',
            'language',
            'license',
            'location',
            'resource_type',
            'file_type',
            'use_category',
            'research_infra',
            'contributor_role',
            'funder_type',
            'relation_type',
            'lifecycle_event',
            'preservation_event'
        ]

        # the values in these selected entries will be used throghout the rest of the test case
        for dtype in data_types:
            if dtype == 'location':
                entry = next((obj for obj in refdata[dtype] if obj.get('wkt', False)), None)
                self.assertTrue(entry is not None)
            else:
                entry = refdata[dtype][0]
            refs[dtype] = {
                'code': entry['code'],
                'uri': entry['uri'],
                'label': entry.get('label', None),
                'wkt': entry.get('wkt', None)
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
        rd['theme'][0] = {'identifier': refs['keyword']['code']}
        rd['field_of_science'][0] = {'identifier': refs['field_of_science']['code']}
        rd['language'][0] = {'identifier': refs['language']['code']}
        rd['access_rights']['access_type'] = {'identifier': refs['access_type']['code']}
        rd['access_rights']['restriction_grounds'] = {'identifier': refs['restriction_grounds']['code']}
        rd['access_rights']['license'][0] = {'identifier': refs['license']['code']}
        rd['other_identifier'][0]['type'] = {'identifier': refs['identifier_type']['code']}
        rd['spatial'][0]['place_uri'] = {'identifier': refs['location']['code']}
        rd['files'][0]['file_type'] = {'identifier': refs['file_type']['code']}
        rd['files'][0]['use_category'] = {'identifier': refs['use_category']['code']}
        rd['directories'][0]['use_category'] = {'identifier': refs['use_category']['code']}
        rd['remote_resources'][0]['resource_type'] = {'identifier': refs['resource_type']['code']}
        rd['remote_resources'][0]['use_category'] = {'identifier': refs['use_category']['code']}
        rd['remote_resources'][0]['license'][0] = {'identifier': refs['license']['code']}
        rd['infrastructure'][0] = {'identifier': refs['research_infra']['code']}
        rd['creator'][0]['contributor_role'] = {'identifier': refs['contributor_role']['code']}
        rd['is_output_of'][0]['funder_type'] = {'identifier': refs['funder_type']['code']}
        rd['relation'][0]['relation_type'] = {'identifier': refs['relation_type']['code']}
        rd['provenance'][0]['type'] = {'identifier': refs['lifecycle_event']['code']}
        rd['provenance'][1]['type'] = {'identifier': refs['preservation_event']['code']}

        # these have other required fields, so only update the identifier with code
        rd['is_output_of'][0]['source_organization'][0]['identifier'] = refs['organization']['code']
        rd['is_output_of'][0]['has_funding_agency'][0]['identifier'] = refs['organization']['code']
        rd['other_identifier'][0]['provider']['identifier'] = refs['organization']['code']
        rd['contributor'][0]['member_of']['identifier'] = refs['organization']['code']
        rd['creator'][0]['member_of']['identifier'] = refs['organization']['code']
        rd['curator'][0]['is_part_of']['identifier'] = refs['organization']['code']
        rd['publisher']['is_part_of']['identifier'] = refs['organization']['code']
        rd['rights_holder']['is_part_of']['identifier'] = refs['organization']['code']
        rd['access_rights']['has_rights_related_agent'][0]['identifier'] = refs['organization']['code']

        # These are fields for which reference data values can be used, but their value should not be touched
        # when they arrive to metax api. The existence of this section may not be justified since this concerns
        # mostly e.g. qvain
        rd['remote_resources'][0]['checksum']['algorithm'] = refs['checksum_algorithm']['code']

        # Other type of reference data populations
        orig_wkt_value = rd['spatial'][0]['as_wkt'][0]
        rd['spatial'][0]['place_uri']['identifier'] = refs['location']['code']
        rd['spatial'][1]['as_wkt'] = []
        rd['spatial'][1]['place_uri']['identifier'] = refs['location']['code']

        response = self.client.post('/rest/datasets', self.third_test_new_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)
        self.assertEqual('research_dataset' in response.data.keys(), True)

        new_rd = response.data['research_dataset']
        self._assert_uri_copied_to_identifier(refs, new_rd)
        self._assert_label_copied_to_pref_label(refs, new_rd)
        self._assert_label_copied_to_title(refs, new_rd)
        self._assert_label_copied_to_name(refs, new_rd)
        self._assert_has_remained_the_same(refs, new_rd)

        # Assert if spatial as_wkt field has been populated with a value from ref data which has wkt value having
        # condition that the user has not given own coordinates in the as_wkt field
        self.assertEqual(orig_wkt_value, new_rd['spatial'][0]['as_wkt'][0])
        self.assertEqual(refs['location']['wkt'], new_rd['spatial'][1]['as_wkt'][0])

    def _assert_uri_copied_to_identifier(self, refs, new_rd):
        self.assertEqual(refs['keyword']['uri'], new_rd['theme'][0]['identifier'])
        self.assertEqual(refs['field_of_science']['uri'], new_rd['field_of_science'][0]['identifier'])
        self.assertEqual(refs['language']['uri'], new_rd['language'][0]['identifier'])
        self.assertEqual(refs['access_type']['uri'], new_rd['access_rights']['access_type']['identifier'])
        self.assertEqual(refs['restriction_grounds']['uri'],
                         new_rd['access_rights']['restriction_grounds']['identifier'])
        self.assertEqual(refs['license']['uri'], new_rd['access_rights']['license'][0]['identifier'])
        self.assertEqual(refs['identifier_type']['uri'], new_rd['other_identifier'][0]['type']['identifier'])
        self.assertEqual(refs['location']['uri'], new_rd['spatial'][0]['place_uri']['identifier'])
        self.assertEqual(refs['file_type']['uri'], new_rd['files'][0]['file_type']['identifier'])
        self.assertEqual(refs['use_category']['uri'], new_rd['files'][0]['use_category']['identifier'])
        self.assertEqual(refs['resource_type']['uri'], new_rd['remote_resources'][0]['resource_type']['identifier'])
        self.assertEqual(refs['use_category']['uri'], new_rd['remote_resources'][0]['use_category']['identifier'])
        self.assertEqual(refs['use_category']['uri'], new_rd['directories'][0]['use_category']['identifier'])
        self.assertEqual(refs['license']['uri'], new_rd['remote_resources'][0]['license'][0]['identifier'])
        self.assertEqual(refs['organization']['uri'], new_rd['is_output_of'][0]['source_organization'][0]['identifier'])
        self.assertEqual(refs['organization']['uri'], new_rd['is_output_of'][0]['has_funding_agency'][0]['identifier'])
        self.assertEqual(refs['organization']['uri'], new_rd['other_identifier'][0]['provider']['identifier'])
        self.assertEqual(refs['organization']['uri'], new_rd['contributor'][0]['member_of']['identifier'])
        self.assertEqual(refs['organization']['uri'], new_rd['creator'][0]['member_of']['identifier'])
        self.assertEqual(refs['organization']['uri'], new_rd['curator'][0]['is_part_of']['identifier'])
        self.assertEqual(refs['organization']['uri'], new_rd['publisher']['is_part_of']['identifier'])
        self.assertEqual(refs['organization']['uri'], new_rd['rights_holder']['is_part_of']['identifier'])
        self.assertEqual(refs['organization']['uri'],
                         new_rd['access_rights']['has_rights_related_agent'][0]['identifier'])
        self.assertEqual(refs['research_infra']['uri'], new_rd['infrastructure'][0]['identifier'])
        self.assertEqual(refs['contributor_role']['uri'], new_rd['creator'][0]['contributor_role']['identifier'])
        self.assertEqual(refs['funder_type']['uri'], new_rd['is_output_of'][0]['funder_type']['identifier'])
        self.assertEqual(refs['relation_type']['uri'], new_rd['relation'][0]['relation_type']['identifier'])
        self.assertEqual(refs['lifecycle_event']['uri'], new_rd['provenance'][0]['type']['identifier'])
        self.assertEqual(refs['preservation_event']['uri'], new_rd['provenance'][1]['type']['identifier'])

    def _assert_label_copied_to_pref_label(self, refs, new_rd):
        self.assertEqual(refs['keyword']['label'], new_rd['theme'][0].get('pref_label', None))
        self.assertEqual(refs['field_of_science']['label'], new_rd['field_of_science'][0].get('pref_label', None))
        self.assertEqual(refs['access_type']['label'], new_rd['access_rights']['access_type'].get('pref_label', None))
        self.assertEqual(refs['restriction_grounds']['label'],
                         new_rd['access_rights']['restriction_grounds'].get('pref_label', None))
        self.assertEqual(refs['identifier_type']['label'],
                         new_rd['other_identifier'][0]['type'].get('pref_label', None))
        self.assertEqual(refs['location']['label'], new_rd['spatial'][0]['place_uri'].get('pref_label', None))
        self.assertEqual(refs['file_type']['label'], new_rd['files'][0]['file_type'].get('pref_label', None))
        self.assertEqual(refs['use_category']['label'], new_rd['files'][0]['use_category'].get('pref_label', None))
        self.assertEqual(refs['use_category']['label'],
                         new_rd['directories'][0]['use_category'].get('pref_label', None))
        self.assertEqual(refs['resource_type']['label'],
                         new_rd['remote_resources'][0]['resource_type'].get('pref_label', None))
        self.assertEqual(refs['use_category']['label'],
                         new_rd['remote_resources'][0]['use_category'].get('pref_label', None))
        self.assertEqual(refs['research_infra']['label'], new_rd['infrastructure'][0].get('pref_label', None))
        self.assertEqual(refs['contributor_role']['label'],
                         new_rd['creator'][0]['contributor_role'].get('pref_label', None))
        self.assertEqual(refs['funder_type']['label'], new_rd['is_output_of'][0]['funder_type'].get('pref_label', None))
        self.assertEqual(refs['relation_type']['label'], new_rd['relation'][0]['relation_type'].get('pref_label', None))
        self.assertEqual(refs['lifecycle_event']['label'], new_rd['provenance'][0]['type'].get('pref_label', None))
        self.assertEqual(refs['preservation_event']['label'], new_rd['provenance'][1]['type'].get('pref_label', None))

    def _assert_label_copied_to_title(self, refs, new_rd):
        required_langs = dict((lang, val) for lang, val in refs['language']['label'].items()
            if lang in ['fi', 'sv', 'en', 'und'])
        self.assertEqual(required_langs, new_rd['language'][0].get('title', None))
        self.assertEqual(refs['license']['label'], new_rd['access_rights']['license'][0].get('title', None))
        self.assertEqual(refs['license']['label'], new_rd['remote_resources'][0]['license'][0].get('title', None))

    def _assert_label_copied_to_name(self, refs, new_rd):
        self.assertEqual(refs['organization']['label'],
                         new_rd['is_output_of'][0]['source_organization'][0].get('name', None))
        self.assertEqual(refs['organization']['label'],
                         new_rd['is_output_of'][0]['has_funding_agency'][0].get('name', None))
        self.assertEqual(refs['organization']['label'], new_rd['other_identifier'][0]['provider'].get('name', None))
        self.assertEqual(refs['organization']['label'], new_rd['contributor'][0]['member_of'].get('name', None))
        self.assertEqual(refs['organization']['label'], new_rd['creator'][0]['member_of'].get('name', None))
        self.assertEqual(refs['organization']['label'], new_rd['curator'][0]['is_part_of'].get('name', None))
        self.assertEqual(refs['organization']['label'], new_rd['publisher']['is_part_of'].get('name', None))
        self.assertEqual(refs['organization']['label'], new_rd['rights_holder']['is_part_of'].get('name', None))
        self.assertEqual(refs['organization']['label'],
                         new_rd['access_rights']['has_rights_related_agent'][0].get('name', None))

    def _assert_has_remained_the_same(self, refs, new_rd):
        self.assertEquals(refs['checksum_algorithm']['code'], new_rd['remote_resources'][0]['checksum']['algorithm'])


class CatalogRecordApiWriteAlternateRecords(CatalogRecordApiWriteCommon):
    """
    Tests related to handling alternate records: Records which have the same
    preferred_identifier, but are in different data catalogs.

    The tricky part here is that, in catalogs which support versioning, changing preferred_identifier
    will leave the old record in its existing alternate_record_set, and the new version will have
    the changed preferred_identifier, which may or may not be placed into a different
    alternate_record_set.
    """

    def setUp(self):
        super(CatalogRecordApiWriteAlternateRecords, self).setUp()
        self.preferred_identifier = self._set_preferred_identifier_to_record(pk=1, data_catalog=1)
        self.test_new_data['research_dataset']['preferred_identifier'] = self.preferred_identifier
        self.test_new_data['data_catalog'] = None

    def test_alternate_record_set_is_created_if_it_doesnt_exist(self):
        """
        Add a record, where a record already existed with the same pref_id, but did not have an
        alternate_record_set yet. Ensure a new set is created, and both records are added to it.
        """

        # new record is saved to catalog 3, which does not support versioning
        self.test_new_data['data_catalog'] = 3

        existing_records_count = CatalogRecord.objects.filter(
            research_dataset__contains={'preferred_identifier': self.preferred_identifier}).count()
        self.assertEqual(existing_records_count, 1,
                         'in the beginning, there should be only one record with pref id %s'
                         % self.preferred_identifier)

        response = self.client.post('/rest/datasets', self.test_new_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)

        records = CatalogRecord.objects.filter(
            research_dataset__contains={'preferred_identifier': self.preferred_identifier})
        self.assertEqual(len(records), 2,
                         'after, there should be two records with pref id %s' % self.preferred_identifier)

        # both records are moved to same set
        ars_id = records[0].alternate_record_set.id
        self.assertEqual(records[0].alternate_record_set.id, ars_id)
        self.assertEqual(records[1].alternate_record_set.id, ars_id)

        # records in the set are the ones expected
        self.assertEqual(records[0].id, 1)
        self.assertEqual(records[1].id, response.data['id'])

        # records in the set are indeed in different catalogs
        self.assertEqual(records[0].data_catalog.id, 1)
        self.assertEqual(records[1].data_catalog.id, 3)

    def test_append_to_existing_alternate_record_set_if_it_exists(self):
        """
        An alternate_record_set already exists with two records in it. Create a third
        record with the same preferred_identifier. The created record should be added
        to the existing alternate_record_set.
        """
        self._set_preferred_identifier_to_record(pk=2, data_catalog=2)
        self.test_new_data['data_catalog'] = 3

        existing_records_count = CatalogRecord.objects.filter(
            research_dataset__contains={'preferred_identifier': self.preferred_identifier}).count()
        self.assertEqual(existing_records_count, 2,
            'in the beginning, there should be two records with pref id %s' % self.preferred_identifier)

        response = self.client.post('/rest/datasets', self.test_new_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED)

        records = CatalogRecord.objects.filter(
            research_dataset__contains={'preferred_identifier': self.preferred_identifier})
        self.assertEqual(len(records), 3,
            'after, there should be three records with pref id %s' % self.preferred_identifier)

        # all records belong to same set
        ars_id = records[0].alternate_record_set.id
        self.assertEqual(records[0].alternate_record_set.id, ars_id)
        self.assertEqual(records[1].alternate_record_set.id, ars_id)
        self.assertEqual(records[2].alternate_record_set.id, ars_id)

        # records in the set are the ones expected
        self.assertEqual(records[0].id, 1)
        self.assertEqual(records[1].id, 2)
        self.assertEqual(records[2].id, response.data['id'])

        # records in the set are indeed in different catalogs
        self.assertEqual(records[0].data_catalog.id, 1)
        self.assertEqual(records[1].data_catalog.id, 2)
        self.assertEqual(records[2].data_catalog.id, 3)

    def test_record_is_removed_from_alternate_record_set_when_deleted(self):
        """
        When a record belong to an alternate_record_set with multiple other records,
        only the records itself should be deleted. The alternate_record_set should keep
        existing for the other records.
        """

        # initial conditions will have 3 records in the same set.
        self._set_and_ensure_initial_conditions()

        response = self.client.delete('/rest/datasets/2', format="json")
        self.assertEqual(response.status_code, status.HTTP_204_NO_CONTENT)

        # check resulting conditions
        records = CatalogRecord.objects.filter(
            research_dataset__contains={'preferred_identifier': self.preferred_identifier})
        self.assertEqual(records[0].alternate_record_set.records.count(), 2)

    def test_alternate_record_set_is_unchanged_if_updating_record(self):
        """
        Since updating preferred_identifier causes a new dataset version to be created in so
        configured catalogs, the previous version, which belongs to some alternate_record_set,
        should stay as it is.

        The new version, which has a new preferred_identifier, may or may not then be attached
        to another alternate_record_set.

        In other words, a record is never deleted from an alternate_record_set by only updating
        the record, IF the catalog supports versioning. Explicitly deleting the record is
        required to remove it from its alternate_record_set.

        In this test, a record belonging to a catalog which supports versioning, has its
        preferred_identifier changed.
        """
        original_preferred_identifier = self.preferred_identifier

        # after this, pk=1 and pk=2 have the same preferred_identifier, in catalogs 1 and 2.
        # note! catalog=2, so later an update to the record creates a new version.
        self._set_preferred_identifier_to_record(pk=2, data_catalog=2)

        # save for later checking
        old_ars_id = CatalogRecord.objects.get(pk=2).alternate_record_set.id

        # retrieve record id=2, and change its preferred identifier
        response = self.client.get('/rest/datasets/2', format="json")
        data = {'research_dataset': response.data['research_dataset']}
        data['research_dataset']['preferred_identifier'] = 'a:new:identifier:here'

        # updating preferred_identifier - a new version is created, preserving the old version
        # and its preferred_identifier, and its alternate_record_set.
        response = self.client.patch('/rest/datasets/2', data=data, format="json")
        self.assertEqual(response.status_code, status.HTTP_200_OK)

        records = CatalogRecord.objects.filter(
            research_dataset__contains={'preferred_identifier': original_preferred_identifier })
        self.assertEqual(records.count(), 2, 'there still should exist the original two records')

        AlternateRecordSet.objects.get(pk=old_ars_id) # should not throw DoesNotExist

    def test_alternate_record_set_is_deleted_if_updating_record_with_no_versioning_and_one_record_left(self):
        """
        Same as above, but updating a record in a catalog, which does NOT support versioning.
        In this case, the the records itself gets updated, and removed from the old alternate_record_set.
        Since the old alternate_record_set is left with only one other record, the alternate set
        should be deleted.
        """
        original_preferred_identifier = self.preferred_identifier

        # after this, pk=1 and pk=2 have the same preferred_identifier, in catalogs 1 and 3.
        # note! catalog=3, so an update will not create a new version!
        self._set_preferred_identifier_to_record(pk=2, data_catalog=3)

        # save for later checking
        old_ars_id = CatalogRecord.objects.get(pk=2).alternate_record_set.id

        # retrieve record id=2, and change its preferred identifier
        response = self.client.get('/rest/datasets/2', format="json")
        data = {'research_dataset': response.data['research_dataset']}
        data['research_dataset']['preferred_identifier'] = 'a:new:identifier:here'

        # updating preferred_identifier - a new version is NOT created
        response = self.client.patch('/rest/datasets/2', data=data, format="json")
        self.assertEqual(response.status_code, status.HTTP_200_OK)

        records = CatalogRecord.objects.filter(
            research_dataset__contains={'preferred_identifier': original_preferred_identifier })
        self.assertEqual(records.count(), 1)

        with self.assertRaises(AlternateRecordSet.DoesNotExist, msg='alternate record set should have been deleted'):
            AlternateRecordSet.objects.get(pk=old_ars_id)

    def test_alternate_record_set_is_deleted_if_deleting_record_and_only_one_record_left(self):
        """
        Same princible as above, but through deleting a record, instead of updating a record.

        End result for the alternate_record_set should be the same (it gets deleted).
        """
        self._set_preferred_identifier_to_record(pk=2, data_catalog=2)
        old_ars_id = CatalogRecord.objects.get(pk=2).alternate_record_set.id

        response = self.client.delete('/rest/datasets/2', format="json")
        self.assertEqual(response.status_code, status.HTTP_204_NO_CONTENT)

        records = CatalogRecord.objects.filter(
            research_dataset__contains={'preferred_identifier': self.preferred_identifier})
        self.assertEqual(records.count(), 1, 'should be only record with this identifier left now')

        with self.assertRaises(AlternateRecordSet.DoesNotExist, msg='alternate record set should have been deleted'):
            AlternateRecordSet.objects.get(pk=old_ars_id)

    def test_alternate_record_set_is_included_in_responses(self):
        """
        Details of a dataset should contain field alternate_record_set in it.
        For a particular record, the set should not contain its own urn_identifier in the set.
        """
        self.test_new_data['data_catalog'] = 3
        msg_self_should_not_be_listed = 'urn_identifier of the record itself should not be listed'

        response_1 = self.client.post('/rest/datasets', self.test_new_data, format="json")
        response_2 = self.client.get('/rest/datasets/1', format="json")
        self.assertEqual(response_1.status_code, status.HTTP_201_CREATED)
        self.assertEqual('alternate_record_set' in response_1.data, True)
        self.assertEqual(
            response_1.data['research_dataset']['urn_identifier'] not in response_1.data['alternate_record_set'], True,
            msg_self_should_not_be_listed)
        self.assertEqual(
            response_2.data['research_dataset']['urn_identifier'] in response_1.data['alternate_record_set'], True)

        self.test_new_data.update({'data_catalog': 4})
        response_3 = self.client.post('/rest/datasets', self.test_new_data, format="json")
        self.assertEqual(response_3.status_code, status.HTTP_201_CREATED)
        self.assertEqual('alternate_record_set' in response_3.data, True)
        self.assertEqual(
            response_1.data['research_dataset']['urn_identifier'] in response_3.data['alternate_record_set'], True)
        self.assertEqual(
            response_2.data['research_dataset']['urn_identifier'] in response_3.data['alternate_record_set'], True)
        self.assertEqual(
            response_3.data['research_dataset']['urn_identifier'] not in response_3.data['alternate_record_set'], True,
            msg_self_should_not_be_listed)

        response_2 = self.client.get('/rest/datasets/1', format="json")
        self.assertEqual('alternate_record_set' in response_2.data, True)
        self.assertEqual(
            response_1.data['research_dataset']['urn_identifier'] in response_2.data['alternate_record_set'], True)
        self.assertEqual(
            response_3.data['research_dataset']['urn_identifier'] in response_2.data['alternate_record_set'], True)
        self.assertEqual(
            response_2.data['research_dataset']['urn_identifier'] not in response_2.data['alternate_record_set'], True,
            msg_self_should_not_be_listed)

    def _set_preferred_identifier_to_record(self, pk=1, data_catalog=1):
        """
        Set preferred_identifier to an existing record to a value, and return that value,
        which will then be used by the test to create or update a record.

        Note that if calling this method several times, this will also create an
        alternate_record_set (by calling _handle_preferred_identifier_changed()).
        """
        unique_identifier = 'im unique yo'
        cr = CatalogRecord.objects.get(pk=pk)
        cr.research_dataset['preferred_identifier'] = unique_identifier
        cr.data_catalog_id = data_catalog
        cr.force_save()
        cr._handle_preferred_identifier_changed()
        return unique_identifier

    def _set_and_ensure_initial_conditions(self):
        """
        Update two existing records to have same pref_id and be in different catalogs,
        to create an alternate_record_set.
        """

        # pk=1 also shares the same preferred_identifier (has been set in setUp())
        self._set_preferred_identifier_to_record(pk=2, data_catalog=3)
        self._set_preferred_identifier_to_record(pk=3, data_catalog=4)

        # ensuring initial conditions...
        records = CatalogRecord.objects.filter(
            research_dataset__contains={'preferred_identifier': self.preferred_identifier})
        self.assertEqual(len(records), 3,
            'in the beginning, there should be three records with pref id %s' % self.preferred_identifier)
        ars_id = records[0].alternate_record_set.id
        self.assertEqual(records[0].alternate_record_set.id, ars_id)
        self.assertEqual(records[1].alternate_record_set.id, ars_id)
        self.assertEqual(records[2].alternate_record_set.id, ars_id)

class CatalogRecordApiWriteDatasetVersioning(CatalogRecordApiWriteCommon):

    """
    Test dataset versioning when updating datasets which belong to a data catalog that
    has dataset_versioning=True.

    Catalogs 1-2 should have dataset_versioning=True, while the rest should not.
    """

    def test_update_to_non_versioning_catalog_does_not_create_version(self):
        self._set_cr_to_catalog(pk=self.pk, dc=3)
        response = self._get_and_update_title(self.pk)
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        self.assertEqual('next_version' not in response.data, True, response.data)
        cr = CatalogRecord.objects.get(pk=self.pk)
        self.assertEqual(cr.next_version, None)

    def test_update_to_versioning_catalog_with_preserve_version_parameter_does_not_create_version(self):
        self._set_cr_to_catalog(pk=self.pk, dc=1)
        response = self._get_and_update_title(self.pk, params='?preserve_version')
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        self.assertEqual('next_version' not in response.data, True, response.data)
        cr = CatalogRecord.objects.get(pk=self.pk)
        self.assertEqual(cr.next_version, None)

    def test_update_rd_title_creates_new_version(self):
        """
        Updating the title should create a new version, but should not change the
        preferred_identifier of the new version.
        """
        self._set_cr_to_catalog(pk=self.pk, dc=1)
        cr = CatalogRecord.objects.get(pk=self.pk)
        preferred_identifier_before = cr.preferred_identifier

        response = self._get_and_update_title(self.pk)
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        self.assertEqual('next_version' in response.data, True, response.data)

        old_version, next_version = self._get_old_and_new_version(self.pk)

        # new version id in the response is correctly set
        self.assertEqual(next_version.id, response.data['next_version']['id'])

        # pref_id did not change for the previous version
        self.assertEqual(preferred_identifier_before, old_version.preferred_identifier)

        # pref_id did not change for the new version
        self.assertEqual(preferred_identifier_before, next_version.preferred_identifier)

    def test_update_rd_preferred_identifier_creates_new_version(self):
        """
        Updating preferred_identifier should create a new version, and should change
        the preferred_identifier in the new version only, NOT the previous version.
        """
        self._set_cr_to_catalog(pk=self.pk, dc=1)
        cr = CatalogRecord.objects.get(pk=self.pk)
        preferred_identifier_before = cr.preferred_identifier

        response, new_preferred_identifier = self._get_and_update_preferred_identifier(self.pk)
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        self.assertEqual('next_version' in response.data, True, response.data)

        old_version, next_version = self._get_old_and_new_version(self.pk)

        # new version id in the response is correctly set
        self.assertEqual(next_version.id, response.data['next_version']['id'])

        # pref_id did not change for the previous version
        self.assertEqual(preferred_identifier_before, old_version.preferred_identifier)

        # pref_id changed for the new version
        self.assertEqual(new_preferred_identifier, next_version.preferred_identifier)

    def test_update_rd_files_creates_new_version(self):
        """
        Updating files should create a new version, and should force a new preferred_identifier,
        even if the user did not provide one. In that case, the new urn_identifier should be copied
        as the new value.

        Here, preferred_identifier is not provided, so urn_identifier should be used automatically
        for the new version.
        """
        self._set_cr_to_catalog(pk=self.pk, dc=1)
        cr = CatalogRecord.objects.get(pk=self.pk)
        preferred_identifier_before = cr.preferred_identifier

        response = self._get_and_update_files(self.pk)
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        self.assertEqual('next_version' in response.data, True, response.data)

        old_version, next_version = self._get_old_and_new_version(self.pk)
        new_preferred_identifier = next_version.preferred_identifier

        # new version id in the response is correctly set
        self.assertEqual(next_version.id, response.data['next_version']['id'])

        # pref_id did not change for the previous version
        self.assertEqual(preferred_identifier_before, old_version.preferred_identifier)

        # urn_identifier was used as the preferred_identifier in the new version
        self.assertEqual(next_version.urn_identifier, next_version.preferred_identifier)

        # pref_id changed for the new version
        self.assertEqual(new_preferred_identifier, next_version.preferred_identifier)

    def test_update_rd_files_and_preferred_identifier_creates_new_version(self):
        """
        Same as above, but if providing a new preferred_identifier as well, then that value
        should be used instead of the urn_identifier.
        """
        self._set_cr_to_catalog(pk=self.pk, dc=1)
        cr = CatalogRecord.objects.get(pk=self.pk)
        preferred_identifier_before = cr.preferred_identifier

        response, new_preferred_identifier = self._get_and_update_preferred_identifier(self.pk)
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        self.assertEqual('next_version' in response.data, True, response.data)

        old_version, next_version = self._get_old_and_new_version(self.pk)
        # new version id in the response is correctly set
        self.assertEqual(next_version.id, response.data['next_version']['id'])

        # pref_id did not change for the previous version
        self.assertEqual(preferred_identifier_before, old_version.preferred_identifier)

        # pref_id changed for the new version
        self.assertEqual(new_preferred_identifier, next_version.preferred_identifier)

    def test_prevent_update_of_dataset_metadata_in_old_versions(self):
        """
        Updating any metadata in a CR which has newer versions available should not be allowed.
        """
        self._set_cr_to_catalog(pk=self.pk, dc=1)

        # updates the record, creates a new version
        response = self._get_and_update_title(self.pk)
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)

        data = self.client.get('/rest/datasets/%d' % self.pk, format="json").data
        data['research_dataset']['title']['en'] = 'modified title again'

        # attempt updating the record again, which should result in an error, since it
        # has newer versions available
        response = self.client.put('/rest/datasets/%d' % self.pk, data, format="json")
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST, response.data)
        self.assertEqual('permitted' in response.data['detail'][0], True, response.data)

    def test_update_cr_fields_in_old_versions_is_ok(self):
        """
        Updating any OTHER field than metadata in a CR which has newer versions available,
        is ok.
        """
        self._set_cr_to_catalog(pk=self.pk, dc=1)

        # updates the record, creates a new version
        response = self._get_and_update_title(self.pk)
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)

        data = { 'preservation_state_description': 'this edit should be ok' }
        response = self.client.patch('/rest/datasets/%d' % self.pk, data, format="json")
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)

    def _set_cr_to_catalog(self, pk=None, dc=None):
        cr = CatalogRecord.objects.get(pk=pk)
        cr.data_catalog_id = dc
        cr.force_save()

    def _get_and_update_title(self, pk, params=None):
        """
        Get, modify, and update data for given pk. The modification should cause a new
        version to be created if the catalog permits.

        Should not force preferred_identifier to change.
        """
        data = self.client.get('/rest/datasets/%d' % pk, format="json").data
        data['research_dataset']['title']['en'] = 'modified title'
        return self.client.put('/rest/datasets/%d%s' % (pk, params or ''), data, format="json")

    def _get_and_update_preferred_identifier(self, pk, params=None):
        """
        Get, modify, and update data for given pk. The modification should cause a new
        version to be created if the catalog permits.

        Should force preferred_identifier to change.
        """
        new_pref_id = 'modified-preferred-identifier'
        data = self.client.get('/rest/datasets/%d' % pk, format="json").data
        data['research_dataset']['preferred_identifier'] = new_pref_id
        return (
            self.client.put('/rest/datasets/%d%s' % (pk, params or ''), data, format="json"),
            new_pref_id
        )

    def _get_and_update_files(self, pk, update_preferred_identifier=False, params=None):
        """
        Get, modify, and update data for given pk. The modification should cause a new
        version to be created if the catalog permits.

        Should force preferred_identifier to change.
        """
        file_identifiers = [
            {
                'identifier': f.identifier,
                'title': 'title',
                'use_category': { 'identifier': 'outcome' }
            }
            for f in File.objects.all()
        ]
        data = self.client.get('/rest/datasets/%d' % pk, format="json").data
        data['research_dataset']['files'] = file_identifiers[-5:]

        if update_preferred_identifier:
            new_pref_id = 'modified-preferred-identifier'
            data['research_dataset']['preferred_identifier'] = new_pref_id
            return (
                self.client.put('/rest/datasets/%d%s' % (pk, params or ''), data, format="json"),
                new_pref_id
            )
        return self.client.put('/rest/datasets/%d%s' % (pk, params or ''), data, format="json")

    def _get_old_and_new_version(self, pk):
        old_version = CatalogRecord.objects.get(pk=pk)
        return old_version, old_version.next_version
