# This file is part of the Metax API service
#
# Copyright 2017-2018 Ministry of Education and Culture, Finland
#
# :author: CSC - IT Center for Science Ltd., Espoo Finland <servicedesk@csc.fi>
# :license: MIT

from copy import deepcopy
from datetime import datetime, timedelta

import responses
from django.conf import settings as django_settings
from django.core.management import call_command
from rest_framework import status
from rest_framework.test import APITestCase

from metax_api.models import AlternateRecordSet, CatalogRecord, Contract, DataCatalog, Directory, File
from metax_api.services import ReferenceDataMixin as RDM, RedisCacheService as cache
from metax_api.tests.utils import test_data_file_path, TestClassUtils
from metax_api.utils import get_tz_aware_now_without_micros, get_identifier_type, IdentifierType
from metax_api.tests.utils import get_test_oidc_token


VALIDATE_TOKEN_URL = django_settings.VALIDATE_TOKEN_URL
END_USER_ALLOWED_DATA_CATALOGS = django_settings.END_USER_ALLOWED_DATA_CATALOGS
LEGACY_CATALOGS = django_settings.LEGACY_CATALOGS
IDA_CATALOG = django_settings.IDA_DATA_CATALOG_IDENTIFIER


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
        self.preferred_identifier = catalog_record_from_test_data['research_dataset']['preferred_identifier']
        self.identifier = catalog_record_from_test_data['identifier']
        self.pk = catalog_record_from_test_data['id']

        """
        New data that is sent to the server for POST, PUT, PATCH requests. Modified
        slightly as approriate for different purposes
        """
        self.cr_test_data = self._get_new_test_cr_data()
        self.cr_test_data['research_dataset']['publisher'] = {'@type': 'Organization', 'name': {'und': 'Testaaja'}}
        self.cr_test_data['research_dataset']['issued'] = '2010-01-01'

        self.cr_att_test_data = self._get_new_test_cr_data(cr_index=14, dc_index=5)
        self.cr_test_data_new_identifier = self._get_new_test_cr_data_with_updated_identifier()
        self.cr_full_ida_test_data = self._get_new_full_test_ida_cr_data()
        self.cr_full_att_test_data = self._get_new_full_test_att_cr_data()

        self._use_http_authorization()

    def update_record(self, record):
        return self.client.put('/rest/datasets/%d' % record['id'], record, format="json")

    def get_next_version(self, record):
        self.assertEqual('next_dataset_version' in record, True)
        response = self.client.get('/rest/datasets/%d' % record['next_dataset_version']['id'], format="json")
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        return response.data

    #
    #
    #
    # internal helper methods
    #
    #
    #

    def _get_new_test_cr_data(self, cr_index=0, dc_index=0, c_index=0):
        dc = self._get_object_from_test_data('datacatalog', requested_index=dc_index)
        catalog_record_from_test_data = self._get_object_from_test_data('catalogrecord', requested_index=cr_index)

        if dc['catalog_json']['research_dataset_schema'] == 'ida' and \
                'remote_resources' in catalog_record_from_test_data['research_dataset']:
            self.fail("Cannot generate the requested test catalog record since requested data catalog is indicates ida "
                      "schema and the requested catalog record is having remote resources, which is not allowed")

        if dc['catalog_json']['research_dataset_schema'] == 'att' and \
                ('files' in catalog_record_from_test_data['research_dataset'] or
                'directories' in catalog_record_from_test_data['research_dataset']):
            self.fail("Cannot generate the requested test catalog record since requested data catalog is indicates att "
                      "schema and the requested catalog record is having files or directories, which is not allowed")

        catalog_record_from_test_data.update({
            "contract": self._get_object_from_test_data('contract', requested_index=c_index),
            "data_catalog": dc
        })
        catalog_record_from_test_data['research_dataset'].update({
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
            }]
        })
        catalog_record_from_test_data['research_dataset'].pop('preferred_identifier', None)
        catalog_record_from_test_data['research_dataset'].pop('metadata_version_identifier', None)
        catalog_record_from_test_data.pop('identifier', None)
        return catalog_record_from_test_data

    def _get_new_test_cr_data_with_updated_identifier(self):
        catalog_record_from_test_data = self._get_new_test_cr_data()
        # catalog_record_from_test_data['research_dataset'].update({
        #     "metadata_version_identifier": "urn:nbn:fi:att:5cd4d4f9-9583-422e-9946-990c8ea96781"
        # })
        return catalog_record_from_test_data

    def _get_new_full_test_ida_cr_data(self):
        """
        Returns one of the fuller generated test datasets
        """
        catalog_record_from_test_data = self._get_object_from_test_data('catalogrecord', requested_index=11)
        data_catalog_from_test_data = self._get_object_from_test_data('datacatalog', requested_index=0)
        return self._get_new_full_test_cr_data(catalog_record_from_test_data, data_catalog_from_test_data)

    def _get_new_full_test_att_cr_data(self):
        """
        Returns one of the fuller generated test datasets
        """
        catalog_record_from_test_data = self._get_object_from_test_data('catalogrecord', requested_index=23)
        data_catalog_from_test_data = self._get_object_from_test_data('datacatalog', requested_index=5)
        return self._get_new_full_test_cr_data(catalog_record_from_test_data, data_catalog_from_test_data)

    def _get_new_full_test_cr_data(self, cr_from_test_data, dc_from_test_data):
        cr_from_test_data.update({
            "contract": self._get_object_from_test_data('contract', requested_index=0),
            "data_catalog": dc_from_test_data
        })
        cr_from_test_data['research_dataset'].pop('metadata_version_identifier')
        cr_from_test_data['research_dataset'].pop('preferred_identifier')
        cr_from_test_data.pop('identifier')
        return cr_from_test_data


class CatalogRecordApiWriteCreateTests(CatalogRecordApiWriteCommon):
    #
    #
    #
    # create apis
    #
    #
    #

    def test_create_catalog_record(self):
        self.cr_test_data['research_dataset']['preferred_identifier'] = 'this_should_be_overwritten'
        response = self.client.post('/rest/datasets', self.cr_test_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)
        self.assertEqual('research_dataset' in response.data.keys(), True)
        self.assertEqual('metadata_version_identifier' in response.data['research_dataset'], True,
                         'metadata_version_identifier should have been generated')
        self.assertEqual('preferred_identifier' in response.data['research_dataset'], True,
                         'preferred_identifier should have been generated')
        self.assertNotEqual(
            self.cr_test_data['research_dataset']['preferred_identifier'],
            response.data['research_dataset']['preferred_identifier'],
            'in fairdata catalogs, user is not allowed to set preferred_identifier'
        )
        self.assertNotEqual(
            response.data['research_dataset']['preferred_identifier'],
            response.data['research_dataset']['metadata_version_identifier'],
            'preferred_identifier and metadata_version_identifier should be generated separately'
        )
        cr = CatalogRecord.objects.get(pk=response.data['id'])
        self.assertEqual(cr.date_created >= get_tz_aware_now_without_micros() - timedelta(seconds=5), True,
                         'Timestamp should have been updated during object creation')

    def test_create_catalog_record_as_harvester(self):
        self.cr_test_data['research_dataset']['preferred_identifier'] = 'this_should_be_saved'
        self.cr_test_data['data_catalog'] = 3
        response = self.client.post('/rest/datasets', self.cr_test_data, format="json")
        self.assertEqual(
            self.cr_test_data['research_dataset']['preferred_identifier'],
            response.data['research_dataset']['preferred_identifier'],
            'in harvested catalogs, user (the harvester) is allowed to set preferred_identifier'
        )

    def test_preferred_identifier_is_checked_also_from_deleted_records(self):
        """
        If a catalog record having a specific preferred identifier is deleted, and a new catalog
        record is created having the same preferred identifier, metax should deny this request
        since a catalog record with the same pref id already exists, albeit deleted.
        """

        # dc 3 happens to be harvested catalog, which allows setting pref id
        cr = CatalogRecord.objects.filter(data_catalog_id=3).first()
        response = self.client.delete('/rest/datasets/%d' % cr.id)
        self.assertEqual(response.status_code, status.HTTP_204_NO_CONTENT)

        self.cr_test_data['research_dataset']['preferred_identifier'] = cr.preferred_identifier
        self.cr_test_data['data_catalog'] = 3
        response = self.client.post('/rest/datasets', self.cr_test_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual('already exists' in response.data['research_dataset'][0], True, response.data)

    def test_create_catalog_contract_string_identifier(self):
        contract_identifier = Contract.objects.first().contract_json['identifier']
        self.cr_test_data['contract'] = contract_identifier
        response = self.client.post('/rest/datasets', self.cr_test_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)
        self.assertEqual(response.data['contract']['identifier'], contract_identifier, response.data)

    def test_create_catalog_error_contract_string_identifier_not_found(self):
        self.cr_test_data['contract'] = 'doesnotexist'
        response = self.client.post('/rest/datasets', self.cr_test_data, format="json")
        # self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND, 'Should have raised 404 not found')
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual('contract' in response.data, True, 'Error should have been about contract not found')

    def test_create_catalog_record_json_validation_error_1(self):
        """
        Ensure the json path of the error is returned along with other details
        """
        self.cr_test_data['research_dataset']["title"] = 1234456
        response = self.client.post('/rest/datasets', self.cr_test_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual(len(response.data), 2, 'there should be two errors (error_identifier is one of them)')
        self.assertEqual('research_dataset' in response.data.keys(), True,
                         'The error should concern the field research_dataset')
        self.assertEqual('1234456 is not of type' in response.data['research_dataset'][0], True, response.data)
        self.assertEqual('Json path: [\'title\']' in response.data['research_dataset'][0], True, response.data)

    def test_create_catalog_record_json_validation_error_2(self):
        """
        Ensure the json path of the error is returned along with other details also in
        objects that are deeply nested
        """
        self.cr_test_data['research_dataset']['provenance'] = [{
            'title': {'en': 'provenance title'},
            'was_associated_with': [
                {'@type': 'Person', 'xname': 'seppo'}
            ]
        }]
        response = self.client.post('/rest/datasets', self.cr_test_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual(len(response.data), 2, 'there should be two errors (error_identifier is one of them)')
        self.assertEqual('research_dataset' in response.data.keys(), True,
                         'The error should concern the field research_dataset')
        self.assertEqual('is not valid' in response.data['research_dataset'][0], True, response.data)
        self.assertEqual('was_associated_with' in response.data['research_dataset'][0], True, response.data)

    def test_create_catalog_record_allowed_projects_ok(self):
        response = self.client.post('/rest/datasets?allowed_projects=project_x', self.cr_test_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)

    def test_create_catalog_record_allowed_projects_fail(self):
        # dataset file not in allowed projects
        response = self.client.post('/rest/datasets?allowed_projects=no,permission', self.cr_test_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN, response.data)

        # ensure list is properly handled (separated by comma, end result should be list)
        response = self.client.post('/rest/datasets?allowed_projects=no_good_project_x,another',
            self.cr_test_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN, response.data)

        # handle empty value
        response = self.client.post('/rest/datasets?allowed_projects=', self.cr_test_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN, response.data)

        # Other trickery
        response = self.client.post('/rest/datasets?allowed_projects=,', self.cr_test_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN, response.data)

    #
    # create list operations
    #

    def test_create_catalog_record_list(self):
        response = self.client.post('/rest/datasets',
                                    [self.cr_test_data, self.cr_test_data_new_identifier], format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED)
        self.assertEqual('success' in response.data.keys(), True)
        self.assertEqual('failed' in response.data.keys(), True)
        self.assertEqual('object' in response.data['success'][0].keys(), True)
        self.assertEqual(len(response.data['success']), 2)
        self.assertEqual(len(response.data['failed']), 0)

    def test_create_catalog_record_list_error_one_fails(self):
        self.cr_test_data['research_dataset']["title"] = 1234456
        response = self.client.post('/rest/datasets',
            [self.cr_test_data, self.cr_test_data_new_identifier], format="json")

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
        self.assertEqual('research_dataset' in response.data['failed'][0]['errors'], True, response.data)
        self.assertEqual(
            '1234456 is not of type' in response.data['failed'][0]['errors']['research_dataset'][0],
            True,
            response.data
        )
        self.assertEqual(
            'Json path: [\'title\']' in response.data['failed'][0]['errors']['research_dataset'][0],
            True,
            response.data
        )

    def test_create_catalog_record_list_error_all_fail(self):
        # data catalog is a required field, should fail
        self.cr_test_data['data_catalog'] = None
        self.cr_test_data_new_identifier['data_catalog'] = None

        response = self.client.post('/rest/datasets',
                                    [self.cr_test_data, self.cr_test_data_new_identifier], format="json")
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual('success' in response.data.keys(), True)
        self.assertEqual('failed' in response.data.keys(), True)
        self.assertEqual('object' in response.data['failed'][0].keys(), True)
        self.assertEqual(len(response.data['success']), 0)
        self.assertEqual(len(response.data['failed']), 2)

    def test_create_catalog_record_editor_field_is_optional(self):
        response = self.client.post('/rest/datasets', self.cr_test_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)
        new = response.data
        new['research_dataset']['title']['en'] = 'updated title'
        new.pop('editor')
        response = self.client.put('/rest/datasets/%d' % new['id'], new, format="json")
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)

    def test_parameter_migration_override_preferred_identifier_when_creating(self):
        """
        Normally, when saving to att/ida catalogs, providing a custom preferred_identifier is not
        permitted. Using the optional query parameter ?migration_override=bool a custom preferred_identifier
        can be passed.
        """
        custom_pid = 'custom-pid-value'
        self.cr_test_data['research_dataset']['preferred_identifier'] = custom_pid
        response = self.client.post('/rest/datasets?migration_override', self.cr_test_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)
        self.assertEqual(response.data['research_dataset']['preferred_identifier'], custom_pid)

    def test_parameter_migration_override_no_preferred_identifier_when_creating(self):
        """
        Normally, when saving to att/ida catalogs, providing a custom preferred_identifier is not
        permitted. Using the optional query parameter ?migration_override=bool a custom preferred_identifier
        can be passed.
        """
        self.cr_test_data['research_dataset']['preferred_identifier'] = ''
        response = self.client.post('/rest/datasets?migration_override', self.cr_test_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)
        self.assertTrue(len(response.data['research_dataset']['preferred_identifier']) > 0)

        self.cr_test_data['research_dataset'].pop('preferred_identifier', None)
        response = self.client.post('/rest/datasets?migration_override', self.cr_test_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)
        self.assertTrue(len(response.data['research_dataset']['preferred_identifier']) > 0)

    def test_create_catalog_record_using_pid_type(self):
        # Test with pid_type = urn
        self.cr_test_data['research_dataset']['preferred_identifier'] = ''
        response = self.client.post('/rest/datasets?pid_type=urn', self.cr_test_data, format="json")
        self.assertTrue(response.data['research_dataset']['preferred_identifier'].startswith('urn:'))

        # Test with pid_type = doi AND not ida catalog
        self.cr_test_data['research_dataset']['preferred_identifier'] = ''
        response = self.client.post('/rest/datasets?pid_type=doi', self.cr_test_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST, response.data)

        # Create ida data catalog
        dc = self._get_object_from_test_data('datacatalog', requested_index=0)
        dc_id = IDA_CATALOG
        dc['catalog_json']['identifier'] = dc_id
        self.client.post('/rest/datacatalogs', dc, format="json")
        # Test with pid_type = doi AND ida catalog
        self.cr_test_data['research_dataset']['preferred_identifier'] = ''
        self.cr_test_data['data_catalog'] = IDA_CATALOG
        response = self.client.post('/rest/datasets?pid_type=doi', self.cr_test_data, format="json")
        self.assertTrue(response.data['research_dataset']['preferred_identifier'].startswith('doi:10.'))

        # Test with pid_type = not_known
        self.cr_test_data['research_dataset']['preferred_identifier'] = ''
        response = self.client.post('/rest/datasets?pid_type=not_known', self.cr_test_data, format="json")
        self.assertTrue(response.data['research_dataset']['preferred_identifier'].startswith('urn:'))

        # Test without pid_type
        self.cr_test_data['research_dataset']['preferred_identifier'] = ''
        response = self.client.post('/rest/datasets', self.cr_test_data, format="json")
        self.assertTrue(response.data['research_dataset']['preferred_identifier'].startswith('urn:'))


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

    def test_create_catalog_record_error_preferred_identifier_cant_be_metadata_version_identifier(self):
        """
        preferred_identifier can never be the same as a metadata_version_identifier in another cr, in any catalog.
        """
        existing_metadata_version_identifier = CatalogRecord.objects.get(pk=1).metadata_version_identifier
        self.cr_test_data['research_dataset']['preferred_identifier'] = existing_metadata_version_identifier

        # setting preferred_identifier is only allowed in harvested catalogs.
        self.cr_test_data['data_catalog'] = 3

        response = self.client.post('/rest/datasets', self.cr_test_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual('research_dataset' in response.data.keys(), True,
                         'The error should be about an error in research_dataset')

        # the error message should clearly state that the value of preferred_identifier appears in the
        # field metadata_version_identifier in another record, therefore two asserts
        self.assertEqual('preferred_identifier' in response.data['research_dataset'][0], True,
                         'The error should be about metadata_version_identifier existing with this identifier')
        self.assertEqual('metadata_version_identifier' in response.data['research_dataset'][0], True,
                         'The error should be about metadata_version_identifier existing with this identifier')

    def test_create_catalog_record_error_preferred_identifier_exists_in_same_catalog(self):
        """
        preferred_identifier already existing in the same data catalog is an error
        """
        self.cr_test_data['research_dataset']['preferred_identifier'] = 'pid_by_harvester'
        self.cr_test_data['data_catalog'] = 3
        cr_1 = self.client.post('/rest/datasets', self.cr_test_data, format="json").data

        self.cr_test_data['research_dataset']['preferred_identifier'] = \
            cr_1['research_dataset']['preferred_identifier']

        response = self.client.post('/rest/datasets', self.cr_test_data, format="json")
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
        self.cr_test_data['research_dataset']['preferred_identifier'] = unique_identifier

        # different catalog, should be OK (not ATT catalog, so preferred_identifier being saved
        # can exist in other catalogs)
        self.cr_test_data['data_catalog'] = 3

        response = self.client.post('/rest/datasets', self.cr_test_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)

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

        cr = CatalogRecord.objects.get(pk=3)
        cr.data_catalog_id = 3
        cr.save()

        data = self.client.get('/rest/datasets/3').data
        data['research_dataset']['preferred_identifier'] = unique_identifier

        response = self.client.patch('/rest/datasets/3', data, format="json")
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)

    def test_update_catalog_record_preferred_identifier_exists_in_another_catalog_2(self):
        """
        preferred_identifier existing in another data catalog is not an error.

        Test PATCH, when data_catalog is being updated to a different catalog
        in the same request. In this case, the uniqueness check has to be executed
        on the new data_catalog being passed.

        In this test, catalog is updated to 3, which should not contain a conflicting
        identifier.
        """
        unique_identifier = self._set_preferred_identifier_to_record(pk=1, catalog_id=1)

        data = self.client.get('/rest/datasets/3').data
        data['research_dataset']['preferred_identifier'] = unique_identifier
        data['data_catalog'] = 3

        response = self.client.patch('/rest/datasets/3', data, format="json")
        self.assertEqual(response.status_code, status.HTTP_200_OK, data)

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

        data = {'research_dataset': self.cr_test_data['research_dataset']}
        data['research_dataset']['preferred_identifier'] = unique_identifier
        data['data_catalog'] = 3

        response = self.client.patch('/rest/datasets/2', data, format="json")
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual('preferred_identifier' in response.data['research_dataset'][0], True,
                         'The error should be about preferred_identifier already existing')

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

    def test_catalog_record_with_not_found_json_schema_gets_default_schema(self):
        # catalog has dataset schema, but it is not found on the server
        dc = DataCatalog.objects.get(pk=1)
        dc.catalog_json['research_dataset_schema'] = 'nonexisting'
        dc.save()

        response = self.client.post('/rest/datasets', self.cr_test_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)

        # catalog has no dataset schema at all
        dc = DataCatalog.objects.get(pk=1)
        dc.catalog_json.pop('research_dataset_schema')
        dc.save()

        response = self.client.post('/rest/datasets', self.cr_test_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)

    def test_catalog_record_create_with_other_schema(self):
        """
        Ensure that dataset json schema validation works with other
        json schemas than the default IDA
        """
        self.cr_test_data['research_dataset']['remote_resources'] = [
            {'title': 'title'},
            {'title': 'title'}
        ]

        response = self.client.post('/rest/datasets', self.cr_test_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)

        self.cr_test_data['research_dataset']['remote_resources'] = [
            {'title': 'title'},
            {'title': 'title'},
            {'woah': 'this should give a failure, since title is a required field, and it is missing'}
        ]

        response = self.client.post('/rest/datasets', self.cr_test_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST, response.data)

    def test_catalog_record_ref_data_validation_with_other_schema(self):
        """
        Ensure that dataset reference data validation and population works with other
        json schemas than the default IDA. Ref data validation should be schema agnostic
        """
        self.cr_test_data['research_dataset']['other_identifier'] = [
            {
                'notation': 'urn:1',
                'type': {
                    'identifier': 'doi',
                }
            }
        ]

        response = self.client.post('/rest/datasets', self.cr_test_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)
        self.assertEqual(
            'uri.suomi.fi' in response.data['research_dataset']['other_identifier'][0]['type']['identifier'],
            True,
            'Identifier type should have been populated with data from ref data'
        )

    def _set_data_catalog_schema_to_harvester(self):
        dc = DataCatalog.objects.get(pk=1)
        dc.catalog_json['research_dataset_schema'] = 'harvester'
        dc.save()


class CatalogRecordApiWriteUpdateTests(CatalogRecordApiWriteCommon):
    #
    #
    # update apis PUT
    #
    #

    def test_update_catalog_record(self):
        cr = self.client.get('/rest/datasets/1').data
        cr['preservation_description'] = 'what'

        response = self.client.put('/rest/datasets/1', cr, format="json")
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        self.assertEqual(response.data['preservation_description'], 'what')
        cr = CatalogRecord.objects.get(pk=1)
        self.assertEqual(cr.date_modified >= get_tz_aware_now_without_micros() - timedelta(seconds=5), True,
                         'Timestamp should have been updated during object update')

    def test_update_catalog_record_error_using_preferred_identifier(self):
        cr = self.client.get('/rest/datasets/1').data
        response = self.client.put('/rest/datasets/%s' % cr['research_dataset']['preferred_identifier'],
                                   { 'whatever': 123 }, format="json")
        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND,
                         'Update operation should return 404 when using preferred_identifier')

    def test_update_catalog_record_error_required_fields(self):
        """
        Field 'research_dataset' is missing, which should result in an error, since PUT
        replaces an object and requires all 'required' fields to be present.
        """
        cr = self.client.get('/rest/datasets/1').data
        cr.pop('research_dataset')
        response = self.client.put('/rest/datasets/1', cr, format="json")

        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual('research_dataset' in response.data.keys(), True,
                         'Error for field \'research_dataset\' is missing from response.data')

    def test_update_catalog_record_not_found(self):
        response = self.client.put('/rest/datasets/doesnotexist', self.cr_test_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND)

    def test_update_catalog_record_contract(self):
        # take any cr that has a contract set
        cr = CatalogRecord.objects.filter(contract_id__isnull=False).first()
        old_contract_id = cr.contract.id

        # update contract to any different contract
        cr_1 = self.client.get('/rest/datasets/%d' % cr.id).data
        cr_1['contract'] = Contract.objects.all().exclude(pk=old_contract_id).first().id

        response = self.client.put('/rest/datasets/%d' % cr.id, cr_1, format="json")
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        new_contract_id = CatalogRecord.objects.get(pk=cr.id).contract.id
        self.assertNotEqual(old_contract_id, new_contract_id, 'Contract should have changed')

    def test_catalog_record_update_allowed_projects_ok(self):
        cr_11 = self.client.get('/rest/datasets/11').data
        cr_11['preservation_state'] = 0
        cr_11_dir_len = len(cr_11['research_dataset']['directories'])
        cr_11['research_dataset']['directories'].pop(1)

        response = self.client.put('/rest/datasets/11?allowed_projects=project_x', cr_11, format="json")
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        self.assertEqual(len(response.data['research_dataset']['directories']), cr_11_dir_len - 1)

    def test_catalog_record_update_allowed_projects_fail(self):
        cr_1 = self.client.get('/rest/datasets/1').data
        cr_1['research_dataset']['files'].pop(0)

        response = self.client.put('/rest/datasets/1?allowed_projects=no,projects', cr_1, format="json")
        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN, response.data)

    #
    # update list operations PUT
    #

    def test_catalog_record_update_list(self):
        cr_1 = self.client.get('/rest/datasets/1').data
        cr_1['preservation_description'] = 'updated description'

        cr_2 = self.client.get('/rest/datasets/2').data
        cr_2['preservation_description'] = 'second updated description'

        response = self.client.put('/rest/datasets', [ cr_1, cr_2 ], format="json")
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        self.assertEqual(len(response.data['success']), 2)

        updated_cr = CatalogRecord.objects.get(pk=1)
        self.assertEqual(updated_cr.preservation_description, 'updated description')
        updated_cr = CatalogRecord.objects.get(pk=2)
        self.assertEqual(updated_cr.preservation_description, 'second updated description')

    def test_catalog_record_update_list_error_one_fails(self):
        cr_1 = self.client.get('/rest/datasets/1').data
        cr_1['preservation_description'] = 'updated description'

        # data catalog is a required field, should therefore fail
        cr_2 = self.client.get('/rest/datasets/2').data
        cr_2.pop('data_catalog', None)

        response = self.client.put('/rest/datasets', [ cr_1, cr_2 ], format="json")
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
        cr_1 = self.client.get('/rest/datasets/1').data
        cr_1.pop('id')
        cr_1.pop('identifier')
        cr_1['research_dataset'].pop('metadata_version_identifier')

        cr_2 = self.client.get('/rest/datasets/2').data
        cr_2['preservation_description'] = 'second updated description'

        response = self.client.put('/rest/datasets', [ cr_1, cr_2 ], format="json")
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        self.assertEqual('success' in response.data.keys(), True)
        self.assertEqual('failed' in response.data.keys(), True)
        self.assertEqual(len(response.data['success']), 1)
        self.assertEqual(len(response.data['failed']), 1)

    def test_catalog_record_deprecated_and_date_deprecated_cannot_be_set(self):
        # Test catalog record's deprecated field cannot be set with POST, PUT or PATCH

        initial_deprecated = True
        self.cr_test_data['deprecated'] = initial_deprecated
        self.cr_test_data['date_deprecated'] = '2018-01-01T00:00:00'
        response = self.client.post('/rest/datasets', self.cr_test_data, format="json")
        self.assertEqual(response.data['deprecated'], False)
        self.assertTrue('date_deprecated' not in response.data)

        response_json = self.client.get('/rest/datasets/1').data
        initial_deprecated = response_json['deprecated']
        response_json['deprecated'] = not initial_deprecated
        response_json['date_deprecated'] = '2018-01-01T00:00:00'
        response = self.client.put('/rest/datasets/1', response_json, format="json")
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        self.assertEqual(response.data['deprecated'], initial_deprecated)
        self.assertTrue('date_deprecated' not in response.data)

        initial_deprecated = self.client.get('/rest/datasets/1').data['deprecated']
        response = self.client.patch('/rest/datasets/1', { 'deprecated': not initial_deprecated }, format="json")
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        self.assertEqual(response.data['deprecated'], initial_deprecated)
        self.assertTrue('date_deprecated' not in response.data)

    def test_catalog_record_date_deprecated_and_date_deprecated_lifecycle(self):
        ds = CatalogRecord.objects.filter(files__id=1)
        ds_id = ds[0].identifier

        response = self.client.delete('/rest/files/1')
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)

        response = self.client.get('/rest/datasets/%s' % ds_id)
        cr = response.data
        self.assertTrue(cr['deprecated'])
        self.assertTrue(cr['date_deprecated'].startswith('2'))

        cr['deprecated'] = False
        cr.pop('date_deprecated', None)
        cr['research_dataset']['files'].pop(0)
        response = self.client.put('/rest/datasets/%s' % ds_id, cr, format="json")
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST, response.data)

    def test_catalog_record_deprecation_updates_date_modified(self):
        cr = CatalogRecord.objects.filter(files__id=1)
        cr_id = cr[0].identifier

        response = self.client.delete('/rest/files/1')
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)

        cr_depr = CatalogRecord.objects.get(identifier=cr_id)
        self.assertTrue(cr_depr.deprecated)
        self.assertEqual(cr_depr.date_modified, cr_depr.date_deprecated, 'date_modified should be updated')


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
        response = self.client.patch('/rest/datasets/%s' % self.identifier, new_data, format="json")

        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        self.assertEqual('research_dataset' in response.data.keys(), True, 'PATCH operation should return full content')
        self.assertEqual(response.data['data_catalog']['id'], new_data_catalog, 'Field data_catalog was not updated')

    #
    # update list operations PATCH
    #

    def test_catalog_record_partial_update_list(self):
        test_data = {}
        test_data['id'] = 1
        test_data['preservation_state'] = 10

        second_test_data = {}
        second_test_data['id'] = 2
        second_test_data['preservation_state'] = 20

        response = self.client.patch('/rest/datasets', [test_data, second_test_data], format="json")
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        self.assertEqual('success' in response.data, True, 'response.data should contain list of changed objects')
        self.assertEqual(len(response.data), 2, 'response.data should contain 2 changed objects')
        self.assertEqual('research_dataset' in response.data['success'][0]['object'], True,
                         'response.data should contain full objects')

        updated_cr = CatalogRecord.objects.get(pk=1)
        self.assertEqual(updated_cr.preservation_state, 10, 'preservation state should have changed to 1')

    def test_catalog_record_partial_update_list_error_one_fails(self):
        test_data = {}
        test_data['id'] = 1
        test_data['preservation_state'] = 10

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
        test_data['preservation_state'] = 10

        second_test_data = {}
        second_test_data['id'] = 2
        second_test_data['preservation_state'] = 20

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
        url = '/rest/datasets/%s' % self.identifier
        response = self.client.delete(url)
        self.assertEqual(response.status_code, status.HTTP_204_NO_CONTENT)
        response = self.client.get(url)
        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND)

        try:
            deleted_catalog_record = CatalogRecord.objects.get(identifier=self.identifier)
            raise Exception('Deleted CatalogRecord should not be retrievable from the default objects table')
        except CatalogRecord.DoesNotExist:
            # successful test should go here, instead of raising the expection in try: block
            pass

        try:
            deleted_catalog_record = CatalogRecord.objects_unfiltered.get(identifier=self.identifier)
        except CatalogRecord.DoesNotExist:
            raise Exception('Deleted CatalogRecord should not be deleted from the db, but marked as removed')

        self.assertEqual(deleted_catalog_record.removed, True)
        self.assertEqual(deleted_catalog_record.identifier, self.identifier)
        self.assertEqual(deleted_catalog_record.date_modified, deleted_catalog_record.date_removed,
                        'date_modified should be updated')

    def test_delete_catalog_record_error_using_preferred_identifier(self):
        url = '/rest/datasets/%s' % self.preferred_identifier
        response = self.client.delete(url)
        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND)


class CatalogRecordApiWritePreservationStateTests(CatalogRecordApiWriteCommon):

    """
    Field preservation_state related tests.
    """

    def test_update_catalog_record_pas_state_allowed_value(self):
        cr = self.client.get('/rest/datasets/1').data
        cr['preservation_state'] = 30
        response = self.client.put('/rest/datasets/1', cr, format="json")
        self.assertEqual(response.status_code, status.HTTP_200_OK)

    def test_update_catalog_record_pas_state_unallowed_value(self):
        cr = self.client.get('/rest/datasets/1').data
        cr['preservation_state'] = 111
        response = self.client.put('/rest/datasets/1', cr, format="json")
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST,
                         'HTTP status should be 400 due to invalid value')
        self.assertEqual('preservation_state' in response.data.keys(), True,
                         'The error should mention the field preservation_state')

    def test_update_catalog_record_preservation_state_modified_is_updated(self):
        cr = self.client.get('/rest/datasets/1').data
        cr['preservation_state'] = 40
        response = self.client.put('/rest/datasets/1', cr, format="json")
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        cr = CatalogRecord.objects.get(pk=1)
        self.assertEqual(cr.preservation_state_modified >= get_tz_aware_now_without_micros() - timedelta(seconds=5),
                         True, 'Timestamp should have been updated during object update')

    def test_update_pas_state_to_needs_revalidation(self):
        """
        When dataset metadata is updated, and preservation_state in (40, 50, 70), metax should
        automatically update preservation_state value to 60 ("validated metadata updated").
        """
        cr = CatalogRecord.objects.get(pk=1)

        for i, preservation_state_value in enumerate((40, 50, 70)):
            # set testing initial condition...
            cr.preservation_state = preservation_state_value
            cr.save()

            # retrieve record and ensure testing state was set correctly...
            cr_data = self.client.get('/rest/datasets/1', format="json").data
            self.assertEqual(cr_data['preservation_state'], preservation_state_value)

            # strike and verify
            cr_data['research_dataset']['title']['en'] = 'Metadata has been updated on loop %d' % i
            response = self.client.put('/rest/datasets/1', cr_data, format="json")
            self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
            self.assertEqual(response.data['preservation_state'], 60)

    def test_prevent_file_changes_when_record_in_pas_process(self):
        """
        When preservation_state > 0, changing associated files of a dataset should not be allowed.
        """
        cr = CatalogRecord.objects.get(pk=1)
        cr.preservation_state = 10
        cr.save()
        cr_data = self.client.get('/rest/datasets/1', format="json").data
        cr_data['research_dataset']['files'].pop(0)
        response = self.client.put('/rest/datasets/1', cr_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST, response.data)
        self.assertEqual('PAS process' in response.data['detail'][0], True, response.data)


class CatalogRecordApiWriteReferenceDataTests(CatalogRecordApiWriteCommon):
    """
    Tests related to reference_data validation and dataset fields population
    from reference_data, according to given uri or code as the value.
    """

    def test_organization_name_is_required(self):
        """
        Organization 'name' field is not madatory in the schema, but that is only because it does
        not make sense to end users when using an identifier from reference data, which will overwrite
        the name anyway.

        If an organization identifier is used, which is not found in the reference data, and therefore
        does not populate the name automatically, then the user is required to provide the name.
        """

        # simple case
        cr = deepcopy(self.cr_full_ida_test_data)
        cr['research_dataset']['curator'] = [{
            '@type': 'Organization',
            'identifier': 'not found!',
            # no name!
        }]
        response = self.client.post('/rest/datasets', cr, format="json")
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)

        # a more complex case. ensure organizations are found from deep structures
        cr = deepcopy(self.cr_full_ida_test_data)
        org = cr['research_dataset']['provenance'][0]['was_associated_with'][0]
        del org['name'] # should cause the error
        org['@type'] = 'Organization'
        org['identifier'] = 'not found!'
        response = self.client.post('/rest/datasets', cr, format="json")
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)

        # try again. should be ok
        org['identifier'] = 'http://uri.suomi.fi/codelist/fairdata/organization/code/10076'
        response = self.client.post('/rest/datasets', cr, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)

    def test_catalog_record_reference_data_missing_ok(self):
        """
        The API should attempt to reload the reference data if it is missing from
        cache for whatever reason, and successfully finish the request
        """
        cache.delete('reference_data')
        self.assertEqual(cache.get('reference_data', master=True), None,
                         'cache ref data should be missing after cache.delete()')

        response = self.client.post('/rest/datasets', self.cr_test_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)

    def test_missing_license_identifier_ok(self):
        """
        Missing license identifier is ok if url is provided.
        Works on att and ida datasets
        """
        rd_ida = self.cr_full_ida_test_data['research_dataset']
        rd_ida['access_rights']['license'] = [{
            'license': "http://a.very.nice.custom/url"
        }]
        response = self.client.post('/rest/datasets', self.cr_full_ida_test_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)
        self.assertEqual(len(response.data['research_dataset']['access_rights']['license'][0]), 1, response.data)

        rd_att = self.cr_full_att_test_data['research_dataset']
        rd_att['access_rights']['license'] = [{
            'license': "http://also.fine.custom/uri",
            'description': {
                'en': "This is very informative description of this custom license."
            }
        }]
        rd_att['remote_resources'][0]['license'] = [{
            'license': "http://cool.remote.uri",
            'description': {
                'en': "Proof that also remote licenses can be used with custom urls."
            }
        }]
        response = self.client.post('/rest/datasets', self.cr_full_att_test_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)
        self.assertEqual(len(response.data['research_dataset']['access_rights']['license'][0]), 2, response.data)
        self.assertEqual(len(response.data['research_dataset']['remote_resources'][0]['license'][0]), 2, response.data)

    def test_create_catalog_record_with_invalid_reference_data(self):
        rd_ida = self.cr_full_ida_test_data['research_dataset']
        rd_ida['theme'][0]['identifier'] = 'nonexisting'
        rd_ida['field_of_science'][0]['identifier'] = 'nonexisting'
        rd_ida['language'][0]['identifier'] = 'nonexisting'
        rd_ida['access_rights']['access_type']['identifier'] = 'nonexisting'
        rd_ida['access_rights']['license'][0]['identifier'] = 'nonexisting'
        rd_ida['other_identifier'][0]['type']['identifier'] = 'nonexisting'
        rd_ida['spatial'][0]['place_uri']['identifier'] = 'nonexisting'
        rd_ida['files'][0]['file_type']['identifier'] = 'nonexisting'
        rd_ida['files'][0]['use_category']['identifier'] = 'nonexisting'
        rd_ida['infrastructure'][0]['identifier'] = 'nonexisting'
        rd_ida['creator'][0]['contributor_role'][0]['identifier'] = 'nonexisting'
        rd_ida['curator'][0]['contributor_type'][0]['identifier'] = 'nonexisting'
        rd_ida['is_output_of'][0]['funder_type']['identifier'] = 'nonexisting'
        rd_ida['directories'][0]['use_category']['identifier'] = 'nonexisting'
        rd_ida['relation'][0]['relation_type']['identifier'] = 'nonexisting'
        rd_ida['relation'][0]['entity']['type']['identifier'] = 'nonexisting'
        rd_ida['provenance'][0]['lifecycle_event']['identifier'] = 'nonexisting'
        rd_ida['provenance'][1]['preservation_event']['identifier'] = 'nonexisting'
        rd_ida['provenance'][0]['event_outcome']['identifier'] = 'nonexisting'
        response = self.client.post('/rest/datasets', self.cr_full_ida_test_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual('research_dataset' in response.data.keys(), True)
        self.assertEqual(len(response.data['research_dataset']), 19)

        rd_att = self.cr_full_att_test_data['research_dataset']
        rd_att['remote_resources'][0]['license'][0]['identifier'] = 'nonexisting'
        rd_att['remote_resources'][1]['resource_type']['identifier'] = 'nonexisting'
        rd_att['remote_resources'][0]['use_category']['identifier'] = 'nonexisting'
        response = self.client.post('/rest/datasets', self.cr_full_att_test_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual('research_dataset' in response.data.keys(), True)
        self.assertEqual(len(response.data['research_dataset']), 3)

    def test_create_catalog_record_populate_fields_from_reference_data(self):
        """
        1) Insert codes from cached reference data to dataset identifier fields
           that will be validated, and then populated
        2) Check that that the values in dataset identifier fields are changed from
           codes to uris after a successful create
        3) Check that labels have also been copied to datasets to their approriate fields
        """
        rf = RDM.get_reference_data(cache)
        refdata = rf['reference_data']
        orgdata = rf['organization_data']
        refs = {}

        data_types = [
            'access_type',
            'restriction_grounds',
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
            'contributor_type',
            'funder_type',
            'relation_type',
            'lifecycle_event',
            'preservation_event',
            'event_outcome'
        ]

        # the values in these selected entries will be used throghout the rest of the test case
        for dtype in data_types:
            if dtype == 'location':
                entry = next((obj for obj in refdata[dtype] if obj.get('wkt', False)), None)
                self.assertTrue(entry is not None)
            else:
                entry = refdata[dtype][1]
            refs[dtype] = {
                'code': entry['code'],
                'uri': entry['uri'],
                'label': entry.get('label', None),
                'wkt': entry.get('wkt', None),
                'scheme': entry.get('scheme', None)
            }

        refs['organization'] = {
            'uri': orgdata['organization'][0]['uri'],
            'code': orgdata['organization'][0]['code'],
            'label': orgdata['organization'][0]['label']
        }

        # replace the relations with objects that have only the identifier set with code as value,
        # to easily check that label was populated (= that it appeared in the dataset after create)
        # without knowing its original value from the generated test data
        rd_ida = self.cr_full_ida_test_data['research_dataset']
        rd_ida['theme'][0] = {'identifier': refs['keyword']['code']}
        rd_ida['field_of_science'][0] = {'identifier': refs['field_of_science']['code']}
        rd_ida['language'][0] = {'identifier': refs['language']['code']}
        rd_ida['access_rights']['access_type'] = {'identifier': refs['access_type']['code']}
        rd_ida['access_rights']['restriction_grounds'][0] = {'identifier': refs['restriction_grounds']['code']}
        rd_ida['access_rights']['license'][0] = {'identifier': refs['license']['code']}
        rd_ida['other_identifier'][0]['type'] = {'identifier': refs['identifier_type']['code']}
        rd_ida['spatial'][0]['place_uri'] = {'identifier': refs['location']['code']}
        rd_ida['files'][0]['file_type'] = {'identifier': refs['file_type']['code']}
        rd_ida['files'][0]['use_category'] = {'identifier': refs['use_category']['code']}
        rd_ida['directories'][0]['use_category'] = {'identifier': refs['use_category']['code']}
        rd_ida['infrastructure'][0] = {'identifier': refs['research_infra']['code']}
        rd_ida['creator'][0]['contributor_role'][0] = {'identifier': refs['contributor_role']['code']}
        rd_ida['curator'][0]['contributor_type'][0] = {'identifier': refs['contributor_type']['code']}
        rd_ida['is_output_of'][0]['funder_type'] = {'identifier': refs['funder_type']['code']}
        rd_ida['relation'][0]['relation_type'] = {'identifier': refs['relation_type']['code']}
        rd_ida['relation'][0]['entity']['type'] = {'identifier': refs['resource_type']['code']}
        rd_ida['provenance'][0]['lifecycle_event'] = {'identifier': refs['lifecycle_event']['code']}
        rd_ida['provenance'][1]['preservation_event'] = {'identifier': refs['preservation_event']['code']}
        rd_ida['provenance'][0]['event_outcome'] = {'identifier': refs['event_outcome']['code']}

        # these have other required fields, so only update the identifier with code
        rd_ida['is_output_of'][0]['source_organization'][0]['identifier'] = refs['organization']['code']
        rd_ida['is_output_of'][0]['has_funding_agency'][0]['identifier'] = refs['organization']['code']
        rd_ida['other_identifier'][0]['provider']['identifier'] = refs['organization']['code']
        rd_ida['contributor'][0]['member_of']['identifier'] = refs['organization']['code']
        rd_ida['creator'][0]['member_of']['identifier'] = refs['organization']['code']
        rd_ida['curator'][0]['is_part_of']['identifier'] = refs['organization']['code']
        rd_ida['publisher']['is_part_of']['identifier'] = refs['organization']['code']
        rd_ida['rights_holder'][0]['is_part_of']['identifier'] = refs['organization']['code']

        # Other type of reference data populations
        orig_wkt_value = rd_ida['spatial'][0]['as_wkt'][0]
        rd_ida['spatial'][0]['place_uri']['identifier'] = refs['location']['code']
        rd_ida['spatial'][1]['as_wkt'] = []
        rd_ida['spatial'][1]['place_uri']['identifier'] = refs['location']['code']

        response = self.client.post('/rest/datasets', self.cr_full_ida_test_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)
        self.assertEqual('research_dataset' in response.data.keys(), True)

        new_rd_ida = response.data['research_dataset']
        self._assert_uri_copied_to_identifier(refs, new_rd_ida)
        self._assert_label_copied_to_pref_label(refs, new_rd_ida)
        self._assert_label_copied_to_title(refs, new_rd_ida)
        self._assert_label_copied_to_name(refs, new_rd_ida)

        # Assert if spatial as_wkt field has been populated with a value from ref data which has wkt value having
        # condition that the user has not given own coordinates in the as_wkt field
        self.assertEqual(orig_wkt_value, new_rd_ida['spatial'][0]['as_wkt'][0])
        self.assertEqual(refs['location']['wkt'], new_rd_ida['spatial'][1]['as_wkt'][0])

        # rd from att data catalog
        rd_att = self.cr_full_att_test_data['research_dataset']
        rd_att['remote_resources'][1]['resource_type'] = {'identifier': refs['resource_type']['code']}
        rd_att['remote_resources'][0]['use_category'] = {'identifier': refs['use_category']['code']}
        rd_att['remote_resources'][0]['license'][0] = {'identifier': refs['license']['code']}

        # Assert remote resources related reference datas
        response = self.client.post('/rest/datasets', self.cr_full_att_test_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)
        self.assertEqual('research_dataset' in response.data.keys(), True)
        new_rd_att = response.data['research_dataset']
        self._assert_att_remote_resource_items(refs, new_rd_att)

    def _assert_att_remote_resource_items(self, refs, new_rd):
        self.assertEqual(refs['resource_type']['uri'], new_rd['remote_resources'][1]['resource_type']['identifier'])
        self.assertEqual(refs['use_category']['uri'], new_rd['remote_resources'][0]['use_category']['identifier'])
        self.assertEqual(refs['license']['uri'], new_rd['remote_resources'][0]['license'][0]['identifier'])
        self.assertEqual(refs['resource_type']['label'],
                         new_rd['remote_resources'][1]['resource_type'].get('pref_label', None))
        self.assertEqual(refs['use_category']['label'],
                         new_rd['remote_resources'][0]['use_category'].get('pref_label', None))
        self.assertEqual(refs['license']['label'], new_rd['remote_resources'][0]['license'][0].get('title', None))

    def _assert_uri_copied_to_identifier(self, refs, new_rd):
        self.assertEqual(refs['keyword']['uri'], new_rd['theme'][0]['identifier'])
        self.assertEqual(refs['field_of_science']['uri'], new_rd['field_of_science'][0]['identifier'])
        self.assertEqual(refs['language']['uri'], new_rd['language'][0]['identifier'])
        self.assertEqual(refs['access_type']['uri'], new_rd['access_rights']['access_type']['identifier'])
        self.assertEqual(refs['restriction_grounds']['uri'],
                         new_rd['access_rights']['restriction_grounds'][0]['identifier'])
        self.assertEqual(refs['license']['uri'], new_rd['access_rights']['license'][0]['identifier'])
        self.assertEqual(refs['identifier_type']['uri'], new_rd['other_identifier'][0]['type']['identifier'])
        self.assertEqual(refs['location']['uri'], new_rd['spatial'][0]['place_uri']['identifier'])
        self.assertEqual(refs['file_type']['uri'], new_rd['files'][0]['file_type']['identifier'])
        self.assertEqual(refs['use_category']['uri'], new_rd['files'][0]['use_category']['identifier'])

        self.assertEqual(refs['use_category']['uri'], new_rd['directories'][0]['use_category']['identifier'])
        self.assertEqual(refs['organization']['uri'], new_rd['is_output_of'][0]['source_organization'][0]['identifier'])
        self.assertEqual(refs['organization']['uri'], new_rd['is_output_of'][0]['has_funding_agency'][0]['identifier'])
        self.assertEqual(refs['organization']['uri'], new_rd['other_identifier'][0]['provider']['identifier'])
        self.assertEqual(refs['organization']['uri'], new_rd['contributor'][0]['member_of']['identifier'])
        self.assertEqual(refs['organization']['uri'], new_rd['creator'][0]['member_of']['identifier'])
        self.assertEqual(refs['organization']['uri'], new_rd['curator'][0]['is_part_of']['identifier'])
        self.assertEqual(refs['organization']['uri'], new_rd['publisher']['is_part_of']['identifier'])
        self.assertEqual(refs['organization']['uri'], new_rd['rights_holder'][0]['is_part_of']['identifier'])
        self.assertEqual(refs['research_infra']['uri'], new_rd['infrastructure'][0]['identifier'])
        self.assertEqual(refs['contributor_role']['uri'], new_rd['creator'][0]['contributor_role'][0]['identifier'])
        self.assertEqual(refs['contributor_type']['uri'], new_rd['curator'][0]['contributor_type'][0]['identifier'])
        self.assertEqual(refs['funder_type']['uri'], new_rd['is_output_of'][0]['funder_type']['identifier'])
        self.assertEqual(refs['relation_type']['uri'], new_rd['relation'][0]['relation_type']['identifier'])
        self.assertEqual(refs['resource_type']['uri'], new_rd['relation'][0]['entity']['type']['identifier'])
        self.assertEqual(refs['lifecycle_event']['uri'], new_rd['provenance'][0]['lifecycle_event']['identifier'])
        self.assertEqual(refs['preservation_event']['uri'], new_rd['provenance'][1]['preservation_event']['identifier'])
        self.assertEqual(refs['event_outcome']['uri'], new_rd['provenance'][0]['event_outcome']['identifier'])

    def _assert_scheme_copied_to_in_scheme(self, refs, new_rd):
        self.assertEqual(refs['keyword']['scheme'], new_rd['theme'][0]['in_scheme'])
        self.assertEqual(refs['field_of_science']['scheme'], new_rd['field_of_science'][0]['in_scheme'])
        self.assertEqual(refs['language']['scheme'], new_rd['language'][0]['in_scheme'])
        self.assertEqual(refs['access_type']['scheme'], new_rd['access_rights']['access_type']['in_scheme'])
        self.assertEqual(refs['restriction_grounds']['scheme'],
                         new_rd['access_rights']['restriction_grounds'][0]['in_scheme'])
        self.assertEqual(refs['license']['scheme'], new_rd['access_rights']['license'][0]['in_scheme'])
        self.assertEqual(refs['identifier_type']['scheme'], new_rd['other_identifier'][0]['type']['in_scheme'])
        self.assertEqual(refs['location']['scheme'], new_rd['spatial'][0]['place_uri']['in_scheme'])
        self.assertEqual(refs['file_type']['scheme'], new_rd['files'][0]['file_type']['in_scheme'])
        self.assertEqual(refs['use_category']['scheme'], new_rd['files'][0]['use_category']['in_scheme'])

        self.assertEqual(refs['use_category']['scheme'], new_rd['directories'][0]['use_category']['in_scheme'])
        self.assertEqual(refs['research_infra']['scheme'], new_rd['infrastructure'][0]['in_scheme'])
        self.assertEqual(refs['contributor_role']['scheme'], new_rd['creator'][0]['contributor_role']['in_scheme'])
        self.assertEqual(refs['contributor_type']['scheme'], new_rd['curator'][0]['contributor_type']['in_scheme'])
        self.assertEqual(refs['funder_type']['scheme'], new_rd['is_output_of'][0]['funder_type']['in_scheme'])
        self.assertEqual(refs['relation_type']['scheme'], new_rd['relation'][0]['relation_type']['in_scheme'])
        self.assertEqual(refs['resource_type']['scheme'], new_rd['relation'][0]['entity']['type']['in_scheme'])
        self.assertEqual(refs['lifecycle_event']['scheme'], new_rd['provenance'][0]['lifecycle_event']['in_scheme'])
        self.assertEqual(refs['preservation_event']['scheme'],
                         new_rd['provenance'][1]['preservation_event']['in_scheme'])
        self.assertEqual(refs['event_outcome']['scheme'], new_rd['provenance'][0]['event_outcome']['in_scheme'])

    def _assert_label_copied_to_pref_label(self, refs, new_rd):
        self.assertEqual(refs['keyword']['label'], new_rd['theme'][0].get('pref_label', None))
        self.assertEqual(refs['field_of_science']['label'], new_rd['field_of_science'][0].get('pref_label', None))
        self.assertEqual(refs['access_type']['label'], new_rd['access_rights']['access_type'].get('pref_label', None))
        self.assertEqual(refs['restriction_grounds']['label'],
                         new_rd['access_rights']['restriction_grounds'][0].get('pref_label', None))
        self.assertEqual(refs['identifier_type']['label'],
                         new_rd['other_identifier'][0]['type'].get('pref_label', None))
        self.assertEqual(refs['location']['label'], new_rd['spatial'][0]['place_uri'].get('pref_label', None))
        self.assertEqual(refs['file_type']['label'], new_rd['files'][0]['file_type'].get('pref_label', None))
        self.assertEqual(refs['use_category']['label'], new_rd['files'][0]['use_category'].get('pref_label', None))
        self.assertEqual(refs['use_category']['label'],
                         new_rd['directories'][0]['use_category'].get('pref_label', None))

        self.assertEqual(refs['research_infra']['label'], new_rd['infrastructure'][0].get('pref_label', None))
        self.assertEqual(refs['contributor_role']['label'],
                         new_rd['creator'][0]['contributor_role'][0].get('pref_label', None))
        self.assertEqual(refs['contributor_type']['label'],
                         new_rd['curator'][0]['contributor_type'][0].get('pref_label', None))
        self.assertEqual(refs['funder_type']['label'], new_rd['is_output_of'][0]['funder_type'].get('pref_label', None))
        self.assertEqual(refs['relation_type']['label'], new_rd['relation'][0]['relation_type'].get('pref_label', None))
        self.assertEqual(refs['resource_type']['label'],
                         new_rd['relation'][0]['entity']['type'].get('pref_label', None))
        self.assertEqual(refs['lifecycle_event']['label'],
                         new_rd['provenance'][0]['lifecycle_event'].get('pref_label', None))
        self.assertEqual(refs['preservation_event']['label'],
                         new_rd['provenance'][1]['preservation_event'].get('pref_label', None))
        self.assertEqual(refs['event_outcome']['label'],
                         new_rd['provenance'][0]['event_outcome'].get('pref_label', None))

    def _assert_label_copied_to_title(self, refs, new_rd):
        required_langs = dict((lang, val) for lang, val in refs['language']['label'].items()
            if lang in ['fi', 'sv', 'en', 'und'])
        self.assertEqual(required_langs, new_rd['language'][0].get('title', None))
        self.assertEqual(refs['license']['label'], new_rd['access_rights']['license'][0].get('title', None))

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
        self.assertEqual(refs['organization']['label'], new_rd['rights_holder'][0]['is_part_of'].get('name', None))

    def test_refdata_sub_org_main_org_population(self):
        # Test parent org gets populated when sub org is from ref data and user has not provided is_part_of relation
        self.cr_test_data['research_dataset']['publisher'] = {'@type': 'Organization', 'identifier': '10076-A800'}
        response = self.client.post('/rest/datasets', self.cr_test_data, format="json")

        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)
        self.assertEqual('is_part_of' in response.data['research_dataset']['publisher'], True)
        self.assertEqual('http://uri.suomi.fi/codelist/fairdata/organization/code/10076',
                         response.data['research_dataset']['publisher']['is_part_of']['identifier'])
        self.assertTrue(response.data['research_dataset']['publisher']['is_part_of'].get('name', False))

        # Test parent org does not get populated when sub org is from ref data and user has provided is_part_of relation
        self.cr_test_data['research_dataset']['publisher'] = {
            '@type': 'Organization',
            'identifier': '10076-A800',
            'is_part_of': {
                '@type': 'Organization',
                'identifier': 'test_id',
                'name': {'und': 'test_name'}
            }}
        response = self.client.post('/rest/datasets', self.cr_test_data, format="json")

        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)
        self.assertEqual('is_part_of' in response.data['research_dataset']['publisher'], True)
        self.assertEqual('test_id', response.data['research_dataset']['publisher']['is_part_of']['identifier'])
        self.assertEqual('test_name', response.data['research_dataset']['publisher']['is_part_of']['name']['und'])

        # Test nothing happens when org is a parent org
        self.cr_test_data['research_dataset']['publisher'] = {'@type': 'Organization', 'identifier': '10076'}
        response = self.client.post('/rest/datasets', self.cr_test_data, format="json")

        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)
        self.assertEqual('is_part_of' not in response.data['research_dataset']['publisher'], True)


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
        self.cr_test_data['research_dataset']['preferred_identifier'] = self.preferred_identifier
        self.cr_test_data['data_catalog'] = None

    def test_alternate_record_set_is_created_if_it_doesnt_exist(self):
        """
        Add a record, where a record already existed with the same pref_id, but did not have an
        alternate_record_set yet. Ensure a new set is created, and both records are added to it.
        """

        # new record is saved to catalog 3, which does not support versioning
        self.cr_test_data['data_catalog'] = 3

        existing_records_count = CatalogRecord.objects.filter(
            research_dataset__contains={'preferred_identifier': self.preferred_identifier}).count()
        self.assertEqual(existing_records_count, 1,
                         'in the beginning, there should be only one record with pref id %s'
                         % self.preferred_identifier)

        response = self.client.post('/rest/datasets', self.cr_test_data, format="json")
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
        self.cr_test_data['data_catalog'] = 3

        existing_records_count = CatalogRecord.objects.filter(
            research_dataset__contains={'preferred_identifier': self.preferred_identifier}).count()
        self.assertEqual(existing_records_count, 2,
            'in the beginning, there should be two records with pref id %s' % self.preferred_identifier)

        response = self.client.post('/rest/datasets', self.cr_test_data, format="json")
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
        For a particular record, the set should not contain its own metadata_version_identifier in the set.
        """
        self.cr_test_data['data_catalog'] = 3
        msg_self_should_not_be_listed = 'identifier of the record itself should not be listed'

        response_1 = self.client.post('/rest/datasets', self.cr_test_data, format="json")
        response_2 = self.client.get('/rest/datasets/1', format="json")
        self.assertEqual(response_1.status_code, status.HTTP_201_CREATED)
        self.assertEqual('alternate_record_set' in response_1.data, True)
        self.assertEqual(
            response_1.data['identifier']
            not in response_1.data['alternate_record_set'],
            True,
            msg_self_should_not_be_listed
        )
        self.assertEqual(
            response_2.data['identifier']
            in response_1.data['alternate_record_set'],
            True
        )

        self.cr_test_data.update({'data_catalog': 4})
        response_3 = self.client.post('/rest/datasets', self.cr_test_data, format="json")
        self.assertEqual(response_3.status_code, status.HTTP_201_CREATED)
        self.assertEqual('alternate_record_set' in response_3.data, True)
        self.assertEqual(
            response_1.data['identifier']
            in response_3.data['alternate_record_set'],
            True
        )
        self.assertEqual(
            response_2.data['identifier']
            in response_3.data['alternate_record_set'],
            True
        )
        self.assertEqual(
            response_3.data['identifier']
            not in response_3.data['alternate_record_set'],
            True,
            msg_self_should_not_be_listed
        )

        response_2 = self.client.get('/rest/datasets/1', format="json")
        self.assertEqual('alternate_record_set' in response_2.data, True)
        self.assertEqual(
            response_1.data['identifier']
            in response_2.data['alternate_record_set'],
            True
        )
        self.assertEqual(
            response_3.data['identifier']
            in response_2.data['alternate_record_set'],
            True
        )
        self.assertEqual(
            response_2.data['identifier']
            not in response_2.data['alternate_record_set'],
            True,
            msg_self_should_not_be_listed
        )

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

    def test_update_from_0_to_n_files_does_not_create_new_version(self):
        """
        The FIRST update from 0 to n files in a dataset should be permitted
        without creating a new dataset version.
        """
        data = self.client.get('/rest/datasets/1', format="json").data
        data.pop('id')
        data.pop('identifier')
        data['research_dataset'].pop('preferred_identifier', None)
        files = data['research_dataset'].pop('files', None)
        data['research_dataset'].pop('directories', None)
        self.assertEqual(isinstance(files, list), True)

        # create test record
        response = self.client.post('/rest/datasets', data, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)

        # modify a few times to create metadata versions
        data = response.data
        data['research_dataset']['title']['en'] = 'updated'
        data = self.client.put('/rest/datasets/%d' % data['id'], data, format="json").data
        data['research_dataset']['title']['en'] = 'updated again'
        data = self.client.put('/rest/datasets/%d' % data['id'], data, format="json").data

        response = self.client.get('/rest/datasets', format="json")
        dataset_count_beginning = response.data['count']

        # add files for the first time - should not create a new dataset version
        data['research_dataset']['files'] = files
        response = self.client.put('/rest/datasets/%d' % data['id'], data, format="json")
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        self.assertEqual('new_version_created' in response.data, False)

        # ensure no "ghost datasets" are created as residue
        response = self.client.get('/rest/datasets', format="json")
        self.assertEqual(response.data['count'], dataset_count_beginning, 'no new datasets should be created')

        # remove files again... a new version is created normally
        files = data['research_dataset'].pop('files')
        response = self.client.put('/rest/datasets/%d' % data['id'], data, format="json")
        new_version = self.get_next_version(response.data)

        response = self.client.get('/rest/datasets', format="json")
        self.assertEqual(response.data['count'], dataset_count_beginning + 1)

        # ...and put the files back. this is another 0->n files update. this time
        # should normally create new dataset version.
        new_version['research_dataset']['files'] = files
        response = self.client.put('/rest/datasets/%d' % new_version['id'], new_version, format="json")
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        self.assertEqual('new_version_created' in response.data, True)

        response = self.client.get('/rest/datasets', format="json")
        self.assertEqual(response.data['count'], dataset_count_beginning + 2)

    def test_update_to_non_versioning_catalog_does_not_create_version(self):
        self._set_cr_to_catalog(pk=self.pk, dc=3)
        response = self._get_and_update_title(self.pk)
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        self._assert_metadata_version_count(response.data, 0)

    def test_update_to_versioning_catalog_with_preserve_version_parameter_does_not_create_version(self):
        self._set_cr_to_catalog(pk=self.pk, dc=1)
        response = self._get_and_update_title(self.pk, params='?preserve_version')
        self._assert_metadata_version_count(response.data, 0)

    def test_preserve_version_parameter_does_not_allow_file_changes(self):
        self._set_cr_to_catalog(pk=self.pk, dc=1)
        data = self.client.get('/rest/datasets/%d' % self.pk, format="json").data
        data['research_dataset']['files'][0]['identifier'] = 'pid:urn:11'
        response = self.client.put('/rest/datasets/%d?preserve_version' % self.pk, data, format="json")
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST, response.data)
        self.assertEqual('not supported' in response.data['detail'][0], True, response.data)

    def test_update_rd_title_creates_new_metadata_version(self):
        """
        Updating the title of metadata should create a new metadata version.
        """
        response_1 = self._get_and_update_title(self.pk)
        self.assertEqual(response_1.status_code, status.HTTP_200_OK, response_1.data)
        self._assert_metadata_version_count(response_1.data, 2)

        # get list of metadata versions to access contents...
        response = self.client.get('/rest/datasets/%d/metadata_versions' % response_1.data['id'], format="json")

        response_2 = self.client.get('/rest/datasets/%d/metadata_versions/%s' %
            (self.pk, response.data[0]['metadata_version_identifier']), format="json")
        self.assertEqual(response_2.status_code, status.HTTP_200_OK, response_2.data)
        self.assertEqual('preferred_identifier' in response_2.data, True)

        # note! response_1 == cr, response_2 == rd
        self.assertEqual(response_1.data['research_dataset']['preferred_identifier'],
            response_2.data['preferred_identifier'])

    def test_changing_files_creates_new_dataset_version(self):
        cr = self.client.get('/rest/datasets/1').data
        cr['research_dataset']['files'].pop(0)
        response = self.client.put('/rest/datasets/1', cr, format="json")
        self.assertEqual('next_dataset_version' in response.data, True)
        self.assertEqual('new_version_created' in response.data, True)
        self.assertEqual('dataset_version_set' in response.data, True)

    def test_new_dataset_version_fixes_deprecated(self):
        """
        A dataset is marked as deprecated when some of its files have been deleted. Creating
        a new version should fix it.
        """
        cr = CatalogRecord.objects.get(pk=1)
        cr.deprecated = True
        cr.force_save()

        cr = self.client.get('/rest/datasets/1').data
        cr['research_dataset']['files'].pop(0)
        response = self.client.put('/rest/datasets/1', cr, format="json")
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(CatalogRecord.objects.get(pk=1).next_dataset_version.deprecated, False)

    def test_dataset_version_lists_removed_records(self):
        # create new version
        cr = self.client.get('/rest/datasets/1').data
        cr['research_dataset']['files'].pop(0)
        response = self.client.put('/rest/datasets/1', cr, format="json")

        # delete the new version
        new_ver = response.data['next_dataset_version']
        response = self.client.delete('/rest/datasets/%d' % new_ver['id'], format="json")

        # check deleted record is listed
        response = self.client.get('/rest/datasets/1', format="json")
        self.assertEqual(response.data['dataset_version_set'][0].get('removed', None), True,
            response.data['dataset_version_set'])

    def test_new_dataset_version_pref_id_type_stays_same_as_previous_dataset_version_pref_id_type(self):
        # Create ida data catalog
        dc = self._get_object_from_test_data('datacatalog', requested_index=0)
        dc_id = IDA_CATALOG
        dc['catalog_json']['identifier'] = dc_id
        self.client.post('/rest/datacatalogs', dc, format="json")

        self.cr_test_data['data_catalog'] = IDA_CATALOG
        self.cr_test_data['research_dataset']['preferred_identifier'] = ''

        cr_v1 = self.client.post('/rest/datasets?pid_type=urn', self.cr_test_data, format="json").data
        cr_v1['research_dataset']['files'].pop(0)
        cr_v2 = self.client.put('/rest/datasets/{0}?pid_type=doi'.format(cr_v1['identifier']), cr_v1, format="json")
        self.assertEqual('new_version_created' in cr_v2.data, True)
        self.assertTrue(
            get_identifier_type(cr_v2.data['new_version_created']['preferred_identifier']) == IdentifierType.URN)

        cr_v1 = self.client.post('/rest/datasets?pid_type=doi', self.cr_test_data, format="json").data
        cr_v1['research_dataset']['files'].pop(0)
        cr_v2 = self.client.put('/rest/datasets/{0}'.format(cr_v1['identifier']), cr_v1, format="json")
        self.assertEqual('new_version_created' in cr_v2.data, True)
        self.assertTrue(
            get_identifier_type(cr_v2.data['new_version_created']['preferred_identifier']) == IdentifierType.DOI)

    def _assert_metadata_version_count(self, record, count):
        response = self.client.get('/rest/datasets/%d/metadata_versions' % record['id'], format="json")
        self.assertEqual(len(response.data), count)

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


class CatalogRecordApiWriteAssignFilesToDataset(CatalogRecordApiWriteCommon):

    """
    Test assigning files and directories to datasets and related functionality,
    except: Tests related to file updates/versioning are handled in the
    CatalogRecordApiWriteDatasetVersioning -suite.
    """

    def _get_file_from_test_data(self):
        from_test_data = self._get_object_from_test_data('file', requested_index=0)
        from_test_data.update({
            "checksum": {
                "value": "checksumvalue",
                "algorithm": "sha2",
                "checked": "2017-05-23T10:07:22.559656Z",
            },
            "file_name": "must_replace",
            "file_path": "must_replace",
            "identifier": "must_replace",
            "project_identifier": "must_replace",
            "file_storage": self._get_object_from_test_data('filestorage', requested_index=0)
        })
        return from_test_data

    def _form_test_file_hierarchy(self):
        """
        A file hierarchy that will be created prior to executing the tests.
        Files and dirs from these files will then be added to a dataset.
        """
        file_data_1 = [
            {
                "file_name": "file_01.txt",
                "file_path": "/TestExperiment/Directory_1/Group_1/file_01.txt",
                'project_identifier': 'testproject',
            },
            {
                "file_name": "file_02.txt",
                "file_path": "/TestExperiment/Directory_1/Group_1/file_02.txt",
                'project_identifier': 'testproject',
            },
            {
                "file_name": "file_03.txt",
                "file_path": "/TestExperiment/Directory_1/Group_2/file_03.txt",
                'project_identifier': 'testproject',
            },
            {
                "file_name": "file_04.txt",
                "file_path": "/TestExperiment/Directory_1/Group_2/file_04.txt",
                'project_identifier': 'testproject',
            },
            {
                "file_name": "file_05.txt",
                "file_path": "/TestExperiment/Directory_1/file_05.txt",
                'project_identifier': 'testproject',
            },
            {
                "file_name": "file_06.txt",
                "file_path": "/TestExperiment/Directory_1/file_06.txt",
                'project_identifier': 'testproject',
            },
            {
                "file_name": "file_07.txt",
                "file_path": "/TestExperiment/Directory_2/Group_1/file_07.txt",
                'project_identifier': 'testproject',
            },
            {
                "file_name": "file_08.txt",
                "file_path": "/TestExperiment/Directory_2/Group_1/file_08.txt",
                'project_identifier': 'testproject',
            },
            {
                "file_name": "file_09.txt",
                "file_path": "/TestExperiment/Directory_2/Group_2/file_09.txt",
                'project_identifier': 'testproject',
            },
            {
                "file_name": "file_10.txt",
                "file_path": "/TestExperiment/Directory_2/Group_2/file_10.txt",
                'project_identifier': 'testproject',
            },
            {
                "file_name": "file_11.txt",
                "file_path": "/TestExperiment/Directory_2/Group_2/Group_2_deeper/file_11.txt",
                'project_identifier': 'testproject',
            },
            {
                "file_name": "file_12.txt",
                "file_path": "/TestExperiment/Directory_2/Group_2/Group_2_deeper/file_12.txt",
                'project_identifier': 'testproject',
            },
            {
                "file_name": "file_13.txt",
                "file_path": "/TestExperiment/Directory_2/file_13.txt",
                'project_identifier': 'testproject',
            },
            {
                "file_name": "file_14.txt",
                "file_path": "/TestExperiment/Directory_2/file_14.txt",
                'project_identifier': 'testproject',
            },
        ]

        file_data_2 = [
            {
                "file_name": "file_15.txt",
                "file_path": "/SecondExperiment/Directory_1/Group_1/file_15.txt",
                'project_identifier': 'testproject_2',
            },
            {
                "file_name": "file_16.txt",
                "file_path": "/SecondExperiment/Directory_1/Group_1/file_16.txt",
                'project_identifier': 'testproject_2',
            },
            {
                "file_name": "file_17.txt",
                "file_path": "/SecondExperiment/Directory_1/Group_2/file_18.txt",
                'project_identifier': 'testproject_2',
            },
            {
                "file_name": "file_18.txt",
                "file_path": "/SecondExperiment/Directory_1/Group_2/file_18.txt",
                'project_identifier': 'testproject_2',
            },
            {
                "file_name": "file_19.txt",
                "file_path": "/SecondExperiment/Directory_1/file_19.txt",
                'project_identifier': 'testproject_2',
            },
            {
                "file_name": "file_20.txt",
                "file_path": "/SecondExperiment/Directory_1/file_20.txt",
                'project_identifier': 'testproject_2',
            },
            {
                "file_name": "file_21.txt",
                "file_path": "/SecondExperiment/Data/file_21.txt",
                'project_identifier': 'testproject_2',
            },
            {
                "file_name": "file_22.txt",
                "file_path": "/SecondExperiment/Data_Config/file_22.txt",
                'project_identifier': 'testproject_2',
            },
            {
                "file_name": "file_23.txt",
                "file_path": "/SecondExperiment/Data_Config/file_23.txt",
                'project_identifier': 'testproject_2',
            },
            {
                "file_name": "file_24.txt",
                "file_path": "/SecondExperiment/Data/History/file_24.txt",
                'project_identifier': 'testproject_2',
            },

        ]

        file_template = self._get_file_from_test_data()
        del file_template['id']
        self._single_file_byte_size = file_template['byte_size']

        files_1 = []
        for i, f in enumerate(file_data_1):
            file = deepcopy(file_template)
            file.update(f, identifier='test:file:%s' % f['file_name'][-6:-4])
            files_1.append(file)

        files_2 = []
        for i, f in enumerate(file_data_2):
            file = deepcopy(file_template)
            file.update(f, identifier='test:file:%s' % f['file_name'][-6:-4])
            files_2.append(file)

        return files_1, files_2

    def _add_directory(self, ds, path, project=None):
        params = { 'directory_path': path }
        if project:
            params['project_identifier'] = project

        identifier = Directory.objects.get(**params).identifier

        if 'directories' not in ds['research_dataset']:
            ds['research_dataset']['directories'] = []

        ds['research_dataset']['directories'].append({
            "identifier": identifier,
            "title": "Directory Title",
            "description": "This is directory at %s" % path,
            "use_category": {
                "identifier": "method"
            }
        })

    def _add_file(self, ds, path):
        identifier = File.objects.filter(file_path__startswith=path).first().identifier

        if 'files' not in ds['research_dataset']:
            ds['research_dataset']['files'] = []

        ds['research_dataset']['files'].append({
            "identifier": identifier,
            "title": "File Title",
            "description": "This is file at %s" % path,
            "use_category": {
                "identifier": "method"
            }
        })

    def _add_nonexisting_directory(self, ds):
        ds['research_dataset']['directories'] = [{
            "identifier": "doesnotexist",
            "title": "Directory Title",
            "description": "This is directory does not exist",
            "use_category": {
                "identifier": "method"
            }
        }]

    def _add_nonexisting_file(self, ds):
        ds['research_dataset']['files'] = [{
            "identifier": "doesnotexist",
            "title": "File Title",
            "description": "This is file does not exist",
            "use_category": {
                "identifier": "method"
            }
        }]

    def _remove_directory(self, ds, path):
        if 'directories' not in ds['research_dataset']:
            raise Exception('ds has no dirs')

        identifier = Directory.objects.get(directory_path=path).identifier

        for i, dr in enumerate(ds['research_dataset']['directories']):
            if dr['identifier'] == identifier:
                ds['research_dataset']['directories'].pop(i)
                return
        raise Exception('path %s not found in directories' % path)

    def _remove_file(self, ds, path):
        if 'files' not in ds['research_dataset']:
            raise Exception('ds has no files')

        identifier = File.objects.get(file_path=path).identifier

        for i, f in enumerate(ds['research_dataset']['files']):
            if f['identifier'] == identifier:
                ds['research_dataset']['files'].pop(i)
                return
        raise Exception('path %s not found in files' % path)

    def _freeze_new_files(self):
        file_data = [
            {
                "file_name": "file_90.txt",
                "file_path": "/TestExperiment/Directory_2/Group_3/file_90.txt",
                'project_identifier': 'testproject',
            },
            {
                "file_name": "file_91.txt",
                "file_path": "/TestExperiment/Directory_2/Group_3/file_91.txt",
                'project_identifier': 'testproject',
            },
        ]

        file_template = self._get_file_from_test_data()
        del file_template['id']
        self._single_file_byte_size = file_template['byte_size']
        files = []

        for i, f in enumerate(file_data):
            file = deepcopy(file_template)
            file.update(f, identifier='frozen:later:file:%s' % f['file_name'][-6:-4])
            files.append(file)
        response = self.client.post('/rest/files', files, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED)

    def setUp(self):
        """
        For each test:
        - remove files and dirs from the metadata record that is being created
        - create 12 new files in a new project
        """
        super(CatalogRecordApiWriteAssignFilesToDataset, self).setUp()
        self.cr_test_data['research_dataset'].pop('id', None)
        self.cr_test_data['research_dataset'].pop('preferred_identifier', None)
        self.cr_test_data['research_dataset'].pop('files', None)
        self.cr_test_data['research_dataset'].pop('directories', None)
        project_files = self._form_test_file_hierarchy()
        for p_files in project_files:
            response = self.client.post('/rest/files', p_files, format="json")
            self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)

    def assert_preferred_identifier_changed(self, response, true_or_false):
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        self.assertEqual('next_dataset_version' in response.data, true_or_false,
            'this field should only be present if preferred_identifier changed')
        if true_or_false is True:
            self.assertEqual(response.data['research_dataset']['preferred_identifier'] !=
                response.data['next_dataset_version']['preferred_identifier'], true_or_false)

    def assert_file_count(self, cr, expected_file_count):
        self.assertEqual(CatalogRecord.objects.get(pk=cr['id']).files.count(), expected_file_count)

    def assert_total_files_byte_size(self, cr, expected_size):
        self.assertEqual(cr['research_dataset']['total_files_byte_size'], expected_size)

    def test_adding_filesystem_root_dir_not_permitted(self):
        """
        The root dir of a filesystem ("/") should not be permitted to be added to a dataset.
        """
        self._add_directory(self.cr_test_data, '/', project='testproject')
        self._add_directory(self.cr_test_data, '/TestExperiment/Directory_1/Group_2')
        response = self.client.post('/rest/datasets', self.cr_test_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST, response.data)

    def test_files_are_saved_during_create(self):
        """
        A very simple "add two individual files" test.
        """
        self._add_file(self.cr_test_data, '/TestExperiment/Directory_1/file_05.txt')
        self._add_file(self.cr_test_data, '/TestExperiment/Directory_1/file_06.txt')
        response = self.client.post('/rest/datasets', self.cr_test_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)
        self.assert_file_count(response.data, 2)
        self.assert_total_files_byte_size(response.data, self._single_file_byte_size * 2)

    def test_directories_are_saved_during_create(self):
        """
        A very simple "add two individual directories" test.
        """
        self._add_directory(self.cr_test_data, '/TestExperiment/Directory_1/Group_1')
        self._add_directory(self.cr_test_data, '/TestExperiment/Directory_1/Group_2')
        response = self.client.post('/rest/datasets', self.cr_test_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)
        self.assert_file_count(response.data, 4)
        self.assert_total_files_byte_size(response.data, self._single_file_byte_size * 4)

    def test_single_common_root_directory(self):
        """
        A very simple "there is a single common root directory" test.
        """
        self._add_directory(self.cr_test_data, '/TestExperiment/Directory_2')
        self._add_directory(self.cr_test_data, '/TestExperiment/Directory_2/Group_2')
        self._add_directory(self.cr_test_data, '/TestExperiment/Directory_2/Group_2/Group_2_deeper')
        response = self.client.post('/rest/datasets', self.cr_test_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)
        self.assert_file_count(response.data, 8)
        self.assert_total_files_byte_size(response.data, self._single_file_byte_size * 8)

    def test_directory_names_are_similar(self):
        """
        Ensure similar directory names are not mistaken to have parent/child relations,
        i.e. directory separator-character is the true separator of dirs in the path.
        """
        self._add_directory(self.cr_test_data, '/SecondExperiment/Data')
        self._add_directory(self.cr_test_data, '/SecondExperiment/Data/History')
        self._add_directory(self.cr_test_data, '/SecondExperiment/Data_Config') # the interesting dir

        response = self.client.post('/rest/datasets', self.cr_test_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)
        self.assert_file_count(response.data, 4)
        self.assert_total_files_byte_size(response.data, self._single_file_byte_size * 4)

    def test_files_and_directories_are_saved_during_create(self):
        """
        A very simple "add two individual directories and two files" test.
        """
        self._add_directory(self.cr_test_data, '/TestExperiment/Directory_1/Group_1')
        self._add_directory(self.cr_test_data, '/TestExperiment/Directory_1/Group_2')
        self._add_file(self.cr_test_data, '/TestExperiment/Directory_1/file_05.txt')
        self._add_file(self.cr_test_data, '/TestExperiment/Directory_1/file_06.txt')
        response = self.client.post('/rest/datasets', self.cr_test_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)
        self.assert_file_count(response.data, 6)
        self.assert_total_files_byte_size(response.data, self._single_file_byte_size * 6)

    def test_files_and_directories_are_saved_during_create_2(self):
        """
        Save a directory, and also two files from the same directory.
        """
        self._add_directory(self.cr_test_data, '/TestExperiment/Directory_1/Group_1')
        self._add_file(self.cr_test_data, '/TestExperiment/Directory_1/Group_1/file_01.txt')
        self._add_file(self.cr_test_data, '/TestExperiment/Directory_1/Group_1/file_02.txt')
        response = self.client.post('/rest/datasets', self.cr_test_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)
        self.assert_file_count(response.data, 2)
        self.assert_total_files_byte_size(response.data, self._single_file_byte_size * 2)

    def test_empty_files_and_directories_arrays_are_removed(self):
        """
        If an update is trying to leave empty "files" or "directories" array into
        research_dataset, they should be removed entirely during the update.
        """
        self._add_directory(self.cr_test_data, '/TestExperiment/Directory_1/Group_1')
        self._add_file(self.cr_test_data, '/TestExperiment/Directory_1/Group_1/file_01.txt')
        response = self.client.post('/rest/datasets', self.cr_test_data, format="json")
        cr = response.data
        cr['research_dataset']['directories'] = []
        cr['research_dataset']['files'] = []
        response = self.client.put('/rest/datasets/%d' % cr['id'], cr, format="json")
        new_version = self.get_next_version(response.data)
        self.assertEqual('directories' in new_version['research_dataset'], False, response.data)
        self.assertEqual('files' in new_version['research_dataset'], False, response.data)

    def test_multiple_file_and_directory_changes(self):
        """
        Multiple add file/add directory updates, followed by multiple remove file/remove directory updates.
        Some of the updates add new files since the path was not already included by other directories, and
        some dont.

        Ensure preferred_identifier changes and file counts and byte sizes change as expected.
        """

        # create the original record with just one file
        self._add_file(self.cr_test_data, '/TestExperiment/Directory_2/file_13.txt')
        response = self.client.post('/rest/datasets', self.cr_test_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)
        self.assertEqual(CatalogRecord.objects.get(pk=response.data['id']).files.count(), 1)

        # add one directory, which holds a couple of files and one sub directory which also holds two files.
        # new files are added
        original_version = response.data
        self._add_directory(original_version, '/TestExperiment/Directory_2/Group_2')
        response = self.update_record(original_version)
        self.assert_preferred_identifier_changed(response, True)
        new_version = self.get_next_version(response.data)
        self.assert_file_count(new_version, 5)
        self.assert_total_files_byte_size(new_version, self._single_file_byte_size * 5)

        # separately add (describe) a child directory of the previous dir.
        # no new files are added
        self._add_directory(new_version, '/TestExperiment/Directory_2/Group_2/Group_2_deeper')
        response = self.update_record(new_version)
        self.assertEqual('new_dataset_version' in response.data, False)

        # add a single new file not included by the previously added directories.
        # new files are added
        self._add_file(new_version, '/TestExperiment/Directory_2/file_14.txt')
        response = self.update_record(new_version)
        self.assert_preferred_identifier_changed(response, True)
        new_version = self.get_next_version(response.data)
        self.assert_file_count(new_version, 6)
        self.assert_total_files_byte_size(new_version, self._single_file_byte_size * 6)

        # remove the previously added file, not included by the previously added directories.
        # files are removed
        self._remove_file(new_version, '/TestExperiment/Directory_2/file_14.txt')
        response = self.update_record(new_version)
        self.assert_preferred_identifier_changed(response, True)
        new_version = self.get_next_version(response.data)
        self.assert_file_count(new_version, 5)
        self.assert_total_files_byte_size(new_version, self._single_file_byte_size * 5)

        # add a single new file already included by the previously added directories.
        # new files are not added
        self._add_file(new_version, '/TestExperiment/Directory_2/Group_2/Group_2_deeper/file_11.txt')
        response = self.update_record(new_version)
        self.assertEqual('new_dataset_version' in response.data, False)

        # remove the sub dir added previously. files are also still contained by the other upper dir.
        # files are not removed
        self._remove_directory(new_version, '/TestExperiment/Directory_2/Group_2/Group_2_deeper')
        response = self.update_record(new_version)
        self.assertEqual('new_dataset_version' in response.data, False)

        # remove a previously added file, the file is still contained by the other upper dir.
        # files are not removed
        self._remove_file(new_version, '/TestExperiment/Directory_2/Group_2/Group_2_deeper/file_11.txt')
        response = self.update_record(new_version)
        self.assertEqual('new_dataset_version' in response.data, False)

        # remove the last directory, which should remove the 4 files included by this dir and the sub dir.
        # files are removed.
        # only the originally added single file should be left.
        self._remove_directory(new_version, '/TestExperiment/Directory_2/Group_2')
        response = self.update_record(new_version)
        self.assert_preferred_identifier_changed(response, True)
        new_version = self.get_next_version(response.data)
        self.assert_file_count(new_version, 1)
        self.assert_total_files_byte_size(new_version, self._single_file_byte_size * 1)

    def test_add_files_which_were_frozen_later(self):
        """
        It is possible to append files to a directory by freezing new files later.
        Such new files or directories can specifically be added to a dataset by
        explicitly selecting/describing them, even if their path is already included
        by another directory.

        Ensure preferred_identifier changes and file counts and byte sizes change as expected.
        """

        # create the original record with just one directory
        self._add_directory(self.cr_test_data, '/TestExperiment/Directory_2')
        response = self.client.post('/rest/datasets', self.cr_test_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)
        self.assertEqual(CatalogRecord.objects.get(pk=response.data['id']).files.count(), 8)
        original_version = response.data

        self._freeze_new_files()

        # add one new file
        self._add_file(original_version, '/TestExperiment/Directory_2/Group_3/file_90.txt')
        response = self.update_record(original_version)
        self.assert_preferred_identifier_changed(response, True)
        new_version = self.get_next_version(response.data)
        self.assert_file_count(new_version, 9)
        self.assert_total_files_byte_size(new_version, self._single_file_byte_size * 9)

        # add one new directory, which holds one new file, since the other file was already added
        self._add_directory(new_version, '/TestExperiment/Directory_2/Group_3')
        response = self.update_record(new_version)
        self.assert_preferred_identifier_changed(response, True)
        new_version = self.get_next_version(response.data)
        self.assert_file_count(new_version, 10)
        self.assert_total_files_byte_size(new_version, self._single_file_byte_size * 10)

    def test_metadata_changes_do_not_add_later_frozen_files(self):
        """
        Ensure simple metadata updates do not automatically also include new frozen files.
        New frozen files should only be searched when those new files or directories are
        specifically added in the update.
        """

        # create the original record with just one directory
        self._add_directory(self.cr_test_data, '/TestExperiment/Directory_2')
        response = self.client.post('/rest/datasets', self.cr_test_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)
        self.assertEqual(CatalogRecord.objects.get(pk=response.data['id']).files.count(), 8)
        original_version = response.data

        self._freeze_new_files()

        original_version['research_dataset']['version_notes'] = [str(datetime.now())]
        response = self.update_record(original_version)
        self.assertEqual('next_dataset_version' in response.data, False)
        self.assert_file_count(response.data, 8)

    def test_removing_top_level_directory_does_not_remove_all_files(self):
        """
        Ensure removing a top-level directory does not remove all its sub-files from a dataset,
        if the dataset contains other sub-directories. Only the files contained by the top-level
        directory (and whatever files fall between the old top-level, and the next known new top-level
        directories) should be removed.
        """

        # note: see the method _form_test_file_hierarchy() to inspect what the directories
        # contain in more detail.
        self._add_directory(self.cr_test_data, '/TestExperiment') # 14 files (removed later)
        self._add_directory(self.cr_test_data, '/TestExperiment/Directory_1') # 6 files
        self._add_directory(self.cr_test_data, '/TestExperiment/Directory_2') # 8 files (removed later)
        self._add_directory(self.cr_test_data, '/TestExperiment/Directory_2/Group_2') # 4 files
        response = self.client.post('/rest/datasets', self.cr_test_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)
        self.assert_file_count(response.data, 14)
        self.assert_total_files_byte_size(response.data, self._single_file_byte_size * 14)
        original_version = response.data

        # remove the root dir, and another sub-dir. there should be two directories left. both of them
        # are now "top-level directories", since they have no common parent.
        # files are removed
        self._remove_directory(original_version, '/TestExperiment')
        self._remove_directory(original_version, '/TestExperiment/Directory_2')
        response = self.update_record(original_version)
        self.assert_preferred_identifier_changed(response, True)
        new_version = self.get_next_version(response.data)
        self.assert_file_count(new_version, 10)
        self.assert_total_files_byte_size(new_version, self._single_file_byte_size * 10)

    def test_add_multiple_directories(self):
        """
        Ensure adding multiple directories at once really adds files from all new directories.
        """
        self._add_directory(self.cr_test_data, '/TestExperiment/Directory_2/Group_2/Group_2_deeper')
        response = self.client.post('/rest/datasets', self.cr_test_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)
        self.assert_file_count(response.data, 2)
        self.assert_total_files_byte_size(response.data, self._single_file_byte_size * 2)
        original_version = response.data

        # add new directories.
        # files are added
        self._add_directory(original_version, '/TestExperiment/Directory_2/Group_1')
        self._add_directory(original_version, '/TestExperiment/Directory_2/Group_2')
        self._add_directory(original_version, '/SecondExperiment/Directory_1/Group_1')
        response = self.update_record(original_version)
        self.assert_preferred_identifier_changed(response, True)
        new_version = self.get_next_version(response.data)
        self.assert_file_count(new_version, 8)
        self.assert_total_files_byte_size(new_version, self._single_file_byte_size * 8)

    def test_add_multiple_directories_2(self):
        """
        Ensure adding multiple directories at once really adds files from all new directories.
        """
        self._add_directory(self.cr_test_data, '/TestExperiment/Directory_1')
        self._add_directory(self.cr_test_data, '/TestExperiment/Directory_2')
        self._add_directory(self.cr_test_data, '/TestExperiment/Directory_2/Group_2/Group_2_deeper')
        response = self.client.post('/rest/datasets', self.cr_test_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)
        self.assert_file_count(response.data, 14)
        self.assert_total_files_byte_size(response.data, self._single_file_byte_size * 14)

    def test_add_files_from_different_projects(self):
        """
        Add directories from two different projects, ensure both projects' top-level dirs
        are handled properly, and none of the projects interferes with each other.
        """
        self._add_directory(self.cr_test_data, '/TestExperiment/Directory_2/Group_2/Group_2_deeper')
        self._add_directory(self.cr_test_data, '/SecondExperiment/Directory_1/Group_1')
        response = self.client.post('/rest/datasets', self.cr_test_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)
        self.assert_file_count(response.data, 4)
        self.assert_total_files_byte_size(response.data, self._single_file_byte_size * 4)
        original_version = response.data

        # add a new directory. this is a top-level of the previous dir, which contains new files.
        # files are added
        self._add_directory(original_version, '/TestExperiment/Directory_2/Group_2')
        response = self.update_record(original_version)
        self.assert_preferred_identifier_changed(response, True)
        new_version = self.get_next_version(response.data)
        self.assert_file_count(new_version, 6)
        self.assert_total_files_byte_size(new_version, self._single_file_byte_size * 6)

        # remove the previously added dir, which is now the top-level dir in that project.
        # files are removed
        self._remove_directory(new_version, '/TestExperiment/Directory_2/Group_2')
        response = self.update_record(new_version)
        self.assert_preferred_identifier_changed(response, True)
        new_version = self.get_next_version(response.data)
        self.assert_file_count(new_version, 4)
        self.assert_total_files_byte_size(new_version, self._single_file_byte_size * 4)

        # remove dirs from the second project entirely.
        # files are removed
        self._remove_directory(new_version, '/SecondExperiment/Directory_1/Group_1')
        response = self.update_record(new_version)
        self.assert_preferred_identifier_changed(response, True)
        new_version = self.get_next_version(response.data)
        self.assert_file_count(new_version, 2)
        self.assert_total_files_byte_size(new_version, self._single_file_byte_size * 2)

    def test_file_not_found(self):
        self._add_directory(self.cr_test_data, '/TestExperiment/Directory_2')
        response = self.client.post('/rest/datasets', self.cr_test_data, format="json")
        original_version = response.data
        self._add_nonexisting_file(original_version)
        response = self.update_record(original_version)
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST, response.data)

    def test_directory_not_found(self):
        self._add_directory(self.cr_test_data, '/TestExperiment/Directory_2')
        response = self.client.post('/rest/datasets', self.cr_test_data, format="json")
        original_version = response.data
        self._add_nonexisting_directory(original_version)
        response = self.update_record(original_version)
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST, response.data)

    def test_prevent_file_changes_to_old_dataset_versions(self):
        """
        In old dataset versions, metadata changes are allowed. File changes, which would
        result in new dataset versions being created, is not allowed.
        """

        # create original record
        self._add_file(self.cr_test_data, '/TestExperiment/Directory_2/file_13.txt')
        response = self.client.post('/rest/datasets', self.cr_test_data, format="json")
        original_version = response.data

        # make a file change, so that a new dataset version is created
        self._add_file(original_version, '/TestExperiment/Directory_2/file_14.txt')
        response = self.update_record(original_version)

        # now try to make a file change to the older dataset versions. this should not be permitted
        self._add_file(original_version, '/TestExperiment/Directory_2/Group_2/Group_2_deeper/file_11.txt')
        response = self.update_record(original_version)
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST,
            'file changes in old dataset versions should not be allowed')

    # other tests related to adding files / dirs

    def test_file_and_dir_titles_are_populated_when_omitted(self):
        """
        If field 'title' is omitted from file or dir metadata, their respective file_name or
        directory_name should automatically be populated as title.
        """
        self._add_directory(self.cr_test_data, '/TestExperiment/Directory_2')
        self._add_file(self.cr_test_data, '/TestExperiment/Directory_1/Group_1/file_01.txt')

        # ensure titles are not overwritten when specified by the user

        orig_titles = [
            self.cr_test_data['research_dataset']['files'][0]['title'],
            self.cr_test_data['research_dataset']['directories'][0]['title'],
        ]

        response = self.client.post('/rest/datasets', self.cr_test_data, format="json")

        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)
        self.assertTrue(
            response.data['research_dataset']['files'][0]['title'] in orig_titles,
            response.data['research_dataset']['files'][0]['title']
        )
        self.assertTrue(
            response.data['research_dataset']['directories'][0]['title'] in orig_titles,
            response.data['research_dataset']['directories'][0]['title']
        )

        # ensure titles are automatically populated when omitted by the user

        del self.cr_test_data['research_dataset']['files'][0]['title']
        del self.cr_test_data['research_dataset']['directories'][0]['title']

        response = self.client.post('/rest/datasets', self.cr_test_data, format="json")

        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)
        self.assertTrue(
            response.data['research_dataset']['files'][0]['title'] not in orig_titles,
            response.data['research_dataset']['files'][0]['title']
        )
        self.assertTrue(
            response.data['research_dataset']['directories'][0]['title'] not in orig_titles,
            response.data['research_dataset']['directories'][0]['title']
        )


class CatalogRecordApiWriteRemoteResources(CatalogRecordApiWriteCommon):

    """
    remote_resources related tests
    """

    def test_calculate_total_remote_resources_byte_size(self):
        cr_with_rr = self._get_object_from_test_data('catalogrecord', requested_index=14)
        rr = cr_with_rr['research_dataset']['remote_resources']
        total_remote_resources_byte_size = sum(res['byte_size'] for res in rr)
        self.cr_att_test_data['research_dataset']['remote_resources'] = rr
        response = self.client.post('/rest/datasets', self.cr_att_test_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)
        self.assertEqual('total_remote_resources_byte_size' in response.data['research_dataset'], True)
        self.assertEqual(response.data['research_dataset']['total_remote_resources_byte_size'],
                         total_remote_resources_byte_size)


class CatalogRecordApiWriteLegacyDataCatalogs(CatalogRecordApiWriteCommon):

    """
    Tests related to legacy data catalogs.
    """

    def setUp(self):
        """
        Create a test-datacatalog that plays the role of a legacy catalog.
        """
        super().setUp()
        dc = DataCatalog.objects.filter(catalog_json__research_dataset_schema='att').first()
        dc.catalog_json['identifier'] = LEGACY_CATALOGS[0]
        dc.force_save()
        del self.cr_test_data['research_dataset']['files']
        del self.cr_test_data['research_dataset']['total_files_byte_size']

    def test_legacy_catalog_pids_are_not_unique(self):
        # values provided as pid values in legacy catalogs are not required to be unique
        # within the catalog.
        self.cr_test_data['data_catalog'] = LEGACY_CATALOGS[0]
        self.cr_test_data['research_dataset']['preferred_identifier'] = 'a'
        same_pid_ids = []
        for i in range(3):
            response = self.client.post('/rest/datasets', self.cr_test_data, format="json")
            self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)
            self.assertEqual(response.data['research_dataset']['preferred_identifier'], 'a')
            same_pid_ids.append(response.data['id'])

        # pid can even be same as an existing dataset's pid in an ATT catalog
        real_pid = CatalogRecord.objects.get(pk=1).preferred_identifier
        self.cr_test_data['research_dataset']['preferred_identifier'] = real_pid
        response = self.client.post('/rest/datasets', self.cr_test_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)
        self.assertEqual(response.data['research_dataset']['preferred_identifier'], real_pid)

    def test_legacy_catalog_pid_must_be_provided(self):
        # pid cant be empty string
        self.cr_test_data['data_catalog'] = LEGACY_CATALOGS[0]
        self.cr_test_data['research_dataset']['preferred_identifier'] = ''
        response = self.client.post('/rest/datasets', self.cr_test_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST, response.data)

        # pid cant be omitted
        del self.cr_test_data['research_dataset']['preferred_identifier']
        response = self.client.post('/rest/datasets', self.cr_test_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST, response.data)

    def test_legacy_catalog_pids_update(self):
        # test setup
        self.cr_test_data['data_catalog'] = LEGACY_CATALOGS[0]
        self.cr_test_data['research_dataset']['preferred_identifier'] = 'a'
        response = self.client.post('/rest/datasets', self.cr_test_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)

        # update record. in updates uniqueness also should not be checked
        modify = response.data
        real_pid = CatalogRecord.objects.get(pk=1).preferred_identifier
        modify['research_dataset']['preferred_identifier'] = real_pid
        response = self.client.put('/rest/datasets/%s' % modify['id'], modify, format="json")
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)

    def test_delete_legacy_catalog_dataset(self):
        """
        Datasets in legacy catalogs should be deleted permanently, instead of only marking them
        as 'removed'.
        """

        # test setup
        self.cr_test_data['data_catalog'] = LEGACY_CATALOGS[0]
        self.cr_test_data['research_dataset']['preferred_identifier'] = 'a'
        response = self.client.post('/rest/datasets', self.cr_test_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)
        cr_id = response.data['id']

        # delete record
        response = self.client.delete('/rest/datasets/%s' % cr_id, format="json")
        self.assertEqual(response.status_code, status.HTTP_204_NO_CONTENT, response.data)
        results_count = CatalogRecord.objects_unfiltered.filter(pk=cr_id).count()
        self.assertEqual(results_count, 0, 'record should have been deleted permantly')


class CatalogRecordApiWriteOwnerFields(CatalogRecordApiWriteCommon):

    """
    Owner-fields related tests:
    metadata_owner_org
    metadata_provider_org
    metadata_provider_user
    """

    def test_metadata_owner_org_is_copied_from_metadata_provider_org(self):
        """
        If field metadata_owner_org is omitted when creating or updating a ds, its value should be copied
        from field metadata_provider_org.
        """

        # create
        cr = self.client.get('/rest/datasets/1', format="json").data
        cr.pop('id')
        cr.pop('identifier')
        cr.pop('metadata_owner_org')
        cr['research_dataset'].pop('preferred_identifier')
        response = self.client.post('/rest/datasets', cr, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)
        self.assertEqual(response.data['metadata_owner_org'], response.data['metadata_provider_org'])

        # update to null - update is prevented
        cr = self.client.get('/rest/datasets/1', format="json").data
        original = cr['metadata_owner_org']
        cr['metadata_owner_org'] = None
        response = self.client.put('/rest/datasets/1', cr, format="json")
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        self.assertEqual(response.data['metadata_owner_org'], original)

        # update with patch, where metadata_owner_org field is absent - value is not reverted back
        # to metadata_provider_org
        response = self.client.patch('/rest/datasets/1', { 'metadata_owner_org': 'abc' }, format="json")
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        response = self.client.patch('/rest/datasets/1', { 'contract': 1 }, format="json")
        self.assertEqual(response.data['metadata_owner_org'], 'abc')

    def test_metadata_provider_org_is_readonly_after_creating(self):
        cr = self.client.get('/rest/datasets/1', format="json").data
        original = cr['metadata_provider_org']
        cr['metadata_provider_org'] = 'changed'
        response = self.client.put('/rest/datasets/1', cr, format="json")
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        self.assertEqual(response.data['metadata_provider_org'], original)

    def test_metadata_provider_user_is_readonly_after_creating(self):
        cr = self.client.get('/rest/datasets/1', format="json").data
        original = cr['metadata_provider_user']
        cr['metadata_provider_user'] = 'changed'
        response = self.client.put('/rest/datasets/1', cr, format="json")
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        self.assertEqual(response.data['metadata_provider_user'], original)


class CatalogRecordApiEndUserAccess(CatalogRecordApiWriteCommon):

    """
    End User Access -related permission testing.
    """

    def setUp(self):
        super().setUp()

        # create catalogs with end user access permitted
        dc = DataCatalog.objects.get(pk=1)
        catalog_json = dc.catalog_json
        for identifier in END_USER_ALLOWED_DATA_CATALOGS:
            catalog_json['identifier'] = identifier
            dc = DataCatalog.objects.create(catalog_json=catalog_json, date_created=get_tz_aware_now_without_micros())

        self.token = get_test_oidc_token()

        # by default, use the unmodified token. to use a different/modified token
        # for various test scenarions, alter self.token, and call the below method again
        self._use_http_authorization(method='bearer', token=self.token)

        # no reason to test anything related to failed authentication, since failed
        # authentication stops the request from proceeding anywhere
        self._mock_token_validation_succeeds()

    def _set_cr_owner_to_token_user(self, cr_id):
        cr = CatalogRecord.objects.get(pk=cr_id)
        cr.user_created = self.token['sub']
        cr.metadata_provider_user = self.token['sub']
        cr.editor = None # pretend the record was created by user directly
        cr.force_save()

    def _set_cr_to_permitted_catalog(self, cr_id):
        cr = CatalogRecord.objects.get(pk=cr_id)
        cr.data_catalog_id = DataCatalog.objects.get(catalog_json__identifier=END_USER_ALLOWED_DATA_CATALOGS[0]).id
        cr.force_save()

    @responses.activate
    def test_user_can_create_dataset(self):
        '''
        Ensure end user can create a new dataset, and required fields are
        automatically placed and the user is only able to affect allowed
        fields
        '''
        user_created = self.token['sub']
        metadata_provider_user = self.token['sub']
        metadata_provider_org = self.token['schacHomeOrganization']
        metadata_owner_org = self.token['schacHomeOrganization']

        self.cr_test_data['data_catalog'] = END_USER_ALLOWED_DATA_CATALOGS[0] # ida
        self.cr_test_data['contract'] = 1
        self.cr_test_data['editor'] = { 'nope': 'discarded by metax' }
        self.cr_test_data['preservation_description'] = 'discarded by metax'
        self.cr_test_data['preservation_reason_description'] = 'discarded by metax'
        self.cr_test_data['preservation_state'] = 10
        self.cr_test_data.pop('metadata_provider_user', None)
        self.cr_test_data.pop('metadata_provider_org', None)
        self.cr_test_data.pop('metadata_owner_org', None)

        # test file permission checking in another test
        self.cr_test_data['research_dataset'].pop('files', None)
        self.cr_test_data['research_dataset'].pop('directories', None)

        response = self.client.post('/rest/datasets', self.cr_test_data, format="json")

        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.content)
        self.assertEqual(response.data['user_created'], user_created)
        self.assertEqual(response.data['metadata_provider_user'], metadata_provider_user)
        self.assertEqual(response.data['metadata_provider_org'], metadata_provider_org)
        self.assertEqual(response.data['metadata_owner_org'], metadata_owner_org)
        self.assertEqual('contract' in response.data, False)
        self.assertEqual('editor' in response.data, False)
        self.assertEqual('preservation_description' in response.data, False)
        self.assertEqual('preservation_reason_description' in response.data, False)
        self.assertEqual(response.data['preservation_state'], 0)

    @responses.activate
    def test_user_can_create_datasets_only_to_limited_catalogs(self):
        '''
        End users should not be able to create datasets for example to harvested
        data catalogs.
        '''

        # test file permission checking in another test
        self.cr_test_data['research_dataset'].pop('files', None)
        self.cr_test_data['research_dataset'].pop('directories', None)

        # should not work
        self.cr_test_data['data_catalog'] = 1
        response = self.client.post('/rest/datasets', self.cr_test_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN, response.data)
        # check error has expected error description
        self.assertEqual('selected data catalog' in response.data['detail'][0], True, response.data)

        # should work
        for identifier in END_USER_ALLOWED_DATA_CATALOGS:
            if identifier in LEGACY_CATALOGS:
                self.cr_test_data['research_dataset']['preferred_identifier'] = 'a'
            self.cr_test_data['data_catalog'] = identifier
            response = self.client.post('/rest/datasets', self.cr_test_data, format="json")
            self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)

    @responses.activate
    def test_owner_can_edit_dataset(self):
        '''
        Ensure end users are able to edit datasets owned by them.
        Ensure end users can only edit permitted fields.
        Note: File project permissions should not be checked, since files are not changed.
        '''

        # create test record
        self.cr_test_data['data_catalog'] = END_USER_ALLOWED_DATA_CATALOGS[0]
        self.cr_test_data['research_dataset'].pop('files', None) # test file permission checking in another test
        self.cr_test_data['research_dataset'].pop('directories', None)
        response = self.client.post('/rest/datasets', self.cr_test_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)

        modified_data = response.data
        # research_dataset is the only permitted field to edit
        modified_data['research_dataset']['value'] = 112233
        modified_data['contract'] = 1
        modified_data['editor'] = { 'nope': 'discarded by metax' }
        modified_data['preservation_description'] = 'discarded by metax'
        modified_data['preservation_reason_description'] = 'discarded by metax'
        modified_data['preservation_state'] = 10

        response = self.client.put('/rest/datasets/%d' % modified_data['id'], modified_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        self.assertEqual(response.data['research_dataset']['value'], 112233) # value we set
        self.assertEqual(response.data['user_modified'], self.token['sub']) # set by metax

        # none of these should have been affected
        self.assertEqual('contract' in response.data, False)
        self.assertEqual('editor' in response.data, False)
        self.assertEqual('preservation_description' in response.data, False)
        self.assertEqual('preservation_reason_description' in response.data, False)
        self.assertEqual(response.data['preservation_state'], 0)

    @responses.activate
    def test_owner_can_edit_datasets_only_in_permitted_catalogs(self):
        '''
        Ensure end users are able to edit datasets only in permitted catalogs, even if they
        own the record (catalog may be disabled from end user editing for reason or another).
        '''

        # create test record
        self.cr_test_data['data_catalog'] = 1
        self.cr_test_data['user_created'] = self.token['sub']
        self.cr_test_data['metadata_provider_user'] = self.token['sub']
        self.cr_test_data.pop('editor', None)

        self._use_http_authorization() # create cr as a service-user
        response = self.client.post('/rest/datasets', self.cr_test_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)

        modified_data = response.data
        modified_data['research_dataset']['value'] = 112233

        self._use_http_authorization(method='bearer', token=self.token)
        response = self.client.put('/rest/datasets/%d' % modified_data['id'], modified_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN, response.data)

    @responses.activate
    def test_owner_can_edit_dataset_check_perms_from_editor_field(self):
        '''
        Ensure end user perms are also checked from the field 'editor', which may be
        set by .e.g. qvain.
        '''
        self.cr_test_data['data_catalog'] = END_USER_ALLOWED_DATA_CATALOGS[0]
        self.cr_test_data['user_created'] = 'editor field is checked before this field, so should be ok'
        self.cr_test_data['editor'] = { 'owner_id': self.token['sub'] }

        self._use_http_authorization() # create cr as a service-user to ensure editor-field is set
        response = self.client.post('/rest/datasets', self.cr_test_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)

        response = self.client.get('/rest/datasets/%d' % response.data['id'], format="json")
        modified_data = response.data
        modified_data['research_dataset']['value'] = 112233
        response = self.client.put('/rest/datasets/%d' % response.data['id'], modified_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_200_OK)

    @responses.activate
    def test_other_users_cant_edit_dataset(self):
        '''
        Ensure end users are unable edit datasets not owned by them.
        '''
        response = self.client.get('/rest/datasets/1', format="json")
        modified_data = response.data
        modified_data['research_dataset']['value'] = 112233

        response = self.client.put('/rest/datasets/1', modified_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN)

        response = self.client.put('/rest/datasets', [modified_data], format="json")
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        # ^ individual errors do not have error codes, only the general request
        # has an error code for a failed request.

    @responses.activate
    def test_user_can_delete_dataset(self):
        self._set_cr_owner_to_token_user(1)
        self._set_cr_to_permitted_catalog(1)
        response = self.client.delete('/rest/datasets/1', format="json")
        self.assertEqual(response.status_code, status.HTTP_204_NO_CONTENT, response.data)

    @responses.activate
    def test_user_file_permissions_are_checked_during_dataset_create(self):
        '''
        Ensure user's association with a project is checked during dataset create when
        attaching files or directories to a dataset.
        '''

        # try creating without proper permisisons
        self.cr_test_data['data_catalog'] = END_USER_ALLOWED_DATA_CATALOGS[0] # ida
        response = self.client.post('/rest/datasets', self.cr_test_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN, response.content)

        # add project membership to user's token and try again
        file_identifier = self.cr_test_data['research_dataset']['files'][0]['identifier']
        project_identifier = File.objects.get(identifier=file_identifier).project_identifier
        self.token['group_names'].append('fairdata:IDA01:%s' % project_identifier)
        self._use_http_authorization(method='bearer', token=self.token)

        response = self.client.post('/rest/datasets', self.cr_test_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.content)

    @responses.activate
    def test_user_file_permissions_are_checked_during_dataset_update(self):
        '''
        Ensure user's association with a project is checked during dataset update when
        attaching files or directories to a dataset. The permissions should be checked
        only for changed files (newly added, or removed).
        '''
        # get some files to add to another dataset
        response = self.client.get('/rest/datasets/2', format="json")
        new_files = response.data['research_dataset']['files']

        # this is the dataset we'll modify
        self._set_cr_owner_to_token_user(1)
        self._set_cr_to_permitted_catalog(1)
        response = self.client.get('/rest/datasets/1', format="json")
        # ensure the files really are new
        for f in new_files:
            for existing_f in response.data['research_dataset']['files']:
                assert f['identifier'] != existing_f['identifier'], 'test preparation failure, files should differ'
        modified_data = response.data
        modified_data['research_dataset']['files'].extend(new_files)

        # should fail, since user's token has no permission for the newly added files
        response = self.client.put('/rest/datasets/1', modified_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN, response.content)

        # add project membership to user's token and try again
        project_identifier = File.objects.get(identifier=new_files[0]['identifier']).project_identifier
        self.token['group_names'].append('fairdata:IDA01:%s' % project_identifier)
        self._use_http_authorization(method='bearer', token=self.token)

        response = self.client.put('/rest/datasets/1', modified_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.content)

    @responses.activate
    def test_owner_receives_unfiltered_dataset_data(self):
        '''
        The general public will have some fields filtered out from the dataset,
        in order to protect sensitive data. The owner of a dataset however should
        always receive full data.
        '''
        self._set_cr_owner_to_token_user(1)

        def _check_fields(obj):
            for sensitive_field in ['email', 'telephone', 'phone']:
                self.assertEqual(sensitive_field in obj['research_dataset']['curator'][0], True,
                    'field %s should be present' % sensitive_field)

        for cr in CatalogRecord.objects.filter(pk=1):
            cr.research_dataset['curator'][0].update({
                'email': 'email@mail.com',
                'phone': '123124',
                'telephone': '123124',
            })
            cr.force_save()

        response = self.client.get('/rest/datasets/1')
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        _check_fields(response.data)


class CatalogRecordApiEndUserAccessV2(CatalogRecordApiEndUserAccess):

    """
    End User Access -related permission testing for new auth proxy.

    These will eventually be removed. These are basically a bunch of
    dull copypasted tests.
    """

    def setUp(self):
        super().setUp()
        self.token = get_test_oidc_token(new_proxy=True)
        self._use_http_authorization(method='bearer', token=self.token)

    def _set_cr_owner_to_token_user(self, cr_id):
        cr = CatalogRecord.objects.get(pk=cr_id)
        cr.user_created = self.token['CSCUserName']
        cr.metadata_provider_user = self.token['CSCUserName']
        cr.editor = None # pretend the record was created by user directly
        cr.force_save()

    @responses.activate
    def test_user_can_create_dataset(self):
        '''
        Ensure end user can create a new dataset, and required fields are
        automatically placed and the user is only able to affect allowed
        fields
        '''
        user_created = self.token['CSCUserName']
        metadata_provider_user = self.token['CSCUserName']
        metadata_provider_org = self.token['schacHomeOrganization']
        metadata_owner_org = self.token['schacHomeOrganization']

        self.cr_test_data['data_catalog'] = END_USER_ALLOWED_DATA_CATALOGS[0] # ida
        self.cr_test_data['contract'] = 1
        self.cr_test_data['editor'] = { 'nope': 'discarded by metax' }
        self.cr_test_data['preservation_description'] = 'discarded by metax'
        self.cr_test_data['preservation_reason_description'] = 'discarded by metax'
        self.cr_test_data['preservation_state'] = 10
        self.cr_test_data.pop('metadata_provider_user', None)
        self.cr_test_data.pop('metadata_provider_org', None)
        self.cr_test_data.pop('metadata_owner_org', None)

        # test file permission checking in another test
        self.cr_test_data['research_dataset'].pop('files', None)
        self.cr_test_data['research_dataset'].pop('directories', None)

        response = self.client.post('/rest/datasets', self.cr_test_data, format="json")

        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.content)
        self.assertEqual(response.data['user_created'], user_created)
        self.assertEqual(response.data['metadata_provider_user'], metadata_provider_user)
        self.assertEqual(response.data['metadata_provider_org'], metadata_provider_org)
        self.assertEqual(response.data['metadata_owner_org'], metadata_owner_org)
        self.assertEqual('contract' in response.data, False)
        self.assertEqual('editor' in response.data, False)
        self.assertEqual('preservation_description' in response.data, False)
        self.assertEqual('preservation_reason_description' in response.data, False)
        self.assertEqual(response.data['preservation_state'], 0)

    @responses.activate
    def test_owner_can_edit_dataset(self):
        '''
        Ensure end users are able to edit datasets owned by them.
        Ensure end users can only edit permitted fields.
        Note: File project permissions should not be checked, since files are not changed.
        '''

        # create test record
        self.cr_test_data['data_catalog'] = END_USER_ALLOWED_DATA_CATALOGS[0]
        self.cr_test_data['research_dataset'].pop('files', None) # test file permission checking in another test
        self.cr_test_data['research_dataset'].pop('directories', None)
        response = self.client.post('/rest/datasets', self.cr_test_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)

        modified_data = response.data
        # research_dataset is the only permitted field to edit
        modified_data['research_dataset']['value'] = 112233
        modified_data['contract'] = 1
        modified_data['editor'] = { 'nope': 'discarded by metax' }
        modified_data['preservation_description'] = 'discarded by metax'
        modified_data['preservation_reason_description'] = 'discarded by metax'
        modified_data['preservation_state'] = 10

        response = self.client.put('/rest/datasets/%d' % modified_data['id'], modified_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        self.assertEqual(response.data['research_dataset']['value'], 112233) # value we set
        self.assertEqual(response.data['user_modified'], self.token['CSCUserName']) # set by metax

        # none of these should have been affected
        self.assertEqual('contract' in response.data, False)
        self.assertEqual('editor' in response.data, False)
        self.assertEqual('preservation_description' in response.data, False)
        self.assertEqual('preservation_reason_description' in response.data, False)
        self.assertEqual(response.data['preservation_state'], 0)

    @responses.activate
    def test_owner_can_edit_datasets_only_in_permitted_catalogs(self):
        '''
        Ensure end users are able to edit datasets only in permitted catalogs, even if they
        own the record (catalog may be disabled from end user editing for reason or another).
        '''

        # create test record
        self.cr_test_data['data_catalog'] = 1
        self.cr_test_data['user_created'] = self.token['CSCUserName']
        self.cr_test_data['metadata_provider_user'] = self.token['CSCUserName']
        self.cr_test_data.pop('editor', None)

        self._use_http_authorization() # create cr as a service-user
        response = self.client.post('/rest/datasets', self.cr_test_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)

        modified_data = response.data
        modified_data['research_dataset']['value'] = 112233

        self._use_http_authorization(method='bearer', token=self.token)
        response = self.client.put('/rest/datasets/%d' % modified_data['id'], modified_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN, response.data)

    @responses.activate
    def test_owner_can_edit_dataset_check_perms_from_editor_field(self):
        '''
        Ensure end user perms are also checked from the field 'editor', which may be
        set by .e.g. qvain.
        '''
        self.cr_test_data['data_catalog'] = END_USER_ALLOWED_DATA_CATALOGS[0]
        self.cr_test_data['user_created'] = 'editor field is checked before this field, so should be ok'
        self.cr_test_data['editor'] = { 'owner_id': self.token['CSCUserName'] }

        self._use_http_authorization() # create cr as a service-user to ensure editor-field is set
        response = self.client.post('/rest/datasets', self.cr_test_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)

        response = self.client.get('/rest/datasets/%d' % response.data['id'], format="json")
        modified_data = response.data
        modified_data['research_dataset']['value'] = 112233
        response = self.client.put('/rest/datasets/%d' % response.data['id'], modified_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_200_OK)

    @responses.activate
    def test_user_file_permissions_are_checked_during_dataset_create(self):
        '''
        Ensure user's association with a project is checked during dataset create when
        attaching files or directories to a dataset.
        '''

        # try creating without proper permisisons
        self.cr_test_data['data_catalog'] = END_USER_ALLOWED_DATA_CATALOGS[0] # ida
        response = self.client.post('/rest/datasets', self.cr_test_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN, response.content)

        # add project membership to user's token and try again
        file_identifier = self.cr_test_data['research_dataset']['files'][0]['identifier']
        project_identifier = File.objects.get(identifier=file_identifier).project_identifier
        self.token['group_names'].append('IDA01:%s' % project_identifier)
        self._use_http_authorization(method='bearer', token=self.token)

        response = self.client.post('/rest/datasets', self.cr_test_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.content)

    @responses.activate
    def test_user_file_permissions_are_checked_during_dataset_update(self):
        '''
        Ensure user's association with a project is checked during dataset update when
        attaching files or directories to a dataset. The permissions should be checked
        only for changed files (newly added, or removed).
        '''
        # get some files to add to another dataset
        response = self.client.get('/rest/datasets/2', format="json")
        new_files = response.data['research_dataset']['files']

        # this is the dataset we'll modify
        self._set_cr_owner_to_token_user(1)
        self._set_cr_to_permitted_catalog(1)
        response = self.client.get('/rest/datasets/1', format="json")
        # ensure the files really are new
        for f in new_files:
            for existing_f in response.data['research_dataset']['files']:
                assert f['identifier'] != existing_f['identifier'], 'test preparation failure, files should differ'
        modified_data = response.data
        modified_data['research_dataset']['files'].extend(new_files)

        # should fail, since user's token has no permission for the newly added files
        response = self.client.put('/rest/datasets/1', modified_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN, response.content)

        # add project membership to user's token and try again
        project_identifier = File.objects.get(identifier=new_files[0]['identifier']).project_identifier
        self.token['group_names'].append('IDA01:%s' % project_identifier)
        self._use_http_authorization(method='bearer', token=self.token)

        response = self.client.put('/rest/datasets/1', modified_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.content)
