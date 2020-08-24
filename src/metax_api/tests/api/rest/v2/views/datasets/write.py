# This file is part of the Metax API service
#
# Copyright 2017-2020 Ministry of Education and Culture, Finland
#
# :author: CSC - IT Center for Science Ltd., Espoo Finland <servicedesk@csc.fi>
# :license: MIT

from datetime import timedelta

import responses
from django.conf import settings as django_settings
from django.core.management import call_command
from rest_framework import status
from rest_framework.test import APITestCase
from metax_api.models.catalog_record import ACCESS_TYPES

from metax_api.models import (
    AlternateRecordSet,
    CatalogRecordV2,
    Contract,
    DataCatalog,
    File
)

from metax_api.tests.utils import test_data_file_path, TestClassUtils
from metax_api.utils import get_tz_aware_now_without_micros
from metax_api.tests.utils import get_test_oidc_token


CR = CatalogRecordV2

VALIDATE_TOKEN_URL = django_settings.VALIDATE_TOKEN_URL
END_USER_ALLOWED_DATA_CATALOGS = django_settings.END_USER_ALLOWED_DATA_CATALOGS
LEGACY_CATALOGS = django_settings.LEGACY_CATALOGS
IDA_CATALOG = django_settings.IDA_DATA_CATALOG_IDENTIFIER
EXT_CATALOG = django_settings.EXT_DATA_CATALOG_IDENTIFIER
ATT_CATALOG = django_settings.ATT_DATA_CATALOG_IDENTIFIER


def create_end_user_catalogs():
    dc = DataCatalog.objects.get(pk=1)
    catalog_json = dc.catalog_json
    for identifier in END_USER_ALLOWED_DATA_CATALOGS:
        catalog_json['identifier'] = identifier
        dc = DataCatalog.objects.create(
            catalog_json=catalog_json,
            date_created=get_tz_aware_now_without_micros(),
            catalog_record_services_create='testuser,api_auth_user,metax',
            catalog_record_services_edit='testuser,api_auth_user,metax'
        )


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
        return self.client.put('/rest/v2/datasets/%d' % record['id'], record, format="json")

    def get_next_version(self, record):
        self.assertEqual('next_dataset_version' in record, True)
        response = self.client.get('/rest/v2/datasets/%d' % record['next_dataset_version']['id'], format="json")
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
    def setUp(self):
        super().setUp()

    def test_issued_date_is_generated(self):
        ''' Issued date is generated for all but harvested catalogs if it doesn't exists '''

        for catalog in [ATT_CATALOG, IDA_CATALOG]:
            dc = DataCatalog.objects.get(pk=2)
            dc.catalog_json['identifier'] = catalog
            dc.force_save()

            # Create a catalog record in att catalog
            self.cr_test_data['data_catalog'] = dc.catalog_json
            self.cr_test_data['research_dataset'].pop('issued', None)

            response = self.client.post('/rest/v2/datasets', self.cr_test_data, format="json")
            self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)
            self.assertTrue('issued' in response.data['research_dataset'], response.data)

    def test_create_catalog_record(self):
        self.cr_test_data['research_dataset']['preferred_identifier'] = 'this_should_be_overwritten'
        response = self.client.post('/rest/v2/datasets', self.cr_test_data, format="json")
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
        cr = CatalogRecordV2.objects.get(pk=response.data['id'])
        self.assertEqual(cr.date_created >= get_tz_aware_now_without_micros() - timedelta(seconds=5), True,
                         'Timestamp should have been updated during object creation')

    def test_create_catalog_record_as_harvester(self):
        self.cr_test_data['research_dataset']['preferred_identifier'] = 'this_should_be_saved'
        self.cr_test_data['data_catalog'] = 3
        response = self.client.post('/rest/v2/datasets', self.cr_test_data, format="json")
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
        cr = CatalogRecordV2.objects.filter(data_catalog_id=3).first()
        response = self.client.delete('/rest/v2/datasets/%d' % cr.id)
        self.assertEqual(response.status_code, status.HTTP_204_NO_CONTENT)

        self.cr_test_data['research_dataset']['preferred_identifier'] = cr.preferred_identifier
        self.cr_test_data['data_catalog'] = 3
        response = self.client.post('/rest/v2/datasets', self.cr_test_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual('already exists' in response.data['research_dataset'][0], True, response.data)

    def test_create_catalog_contract_string_identifier(self):
        contract_identifier = Contract.objects.first().contract_json['identifier']
        self.cr_test_data['contract'] = contract_identifier
        response = self.client.post('/rest/v2/datasets', self.cr_test_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)
        self.assertEqual(response.data['contract']['identifier'], contract_identifier, response.data)

    def test_create_catalog_error_contract_string_identifier_not_found(self):
        self.cr_test_data['contract'] = 'doesnotexist'
        response = self.client.post('/rest/v2/datasets', self.cr_test_data, format="json")
        # self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND, 'Should have raised 404 not found')
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual('contract' in response.data, True, 'Error should have been about contract not found')

    def test_create_catalog_record_json_validation_error_1(self):
        """
        Ensure the json path of the error is returned along with other details
        """
        self.cr_test_data['research_dataset']["title"] = 1234456
        response = self.client.post('/rest/v2/datasets', self.cr_test_data, format="json")
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
        response = self.client.post('/rest/v2/datasets', self.cr_test_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual(len(response.data), 2, 'there should be two errors (error_identifier is one of them)')
        self.assertEqual('research_dataset' in response.data.keys(), True,
                         'The error should concern the field research_dataset')
        self.assertEqual('is not valid' in response.data['research_dataset'][0], True, response.data)
        self.assertEqual('was_associated_with' in response.data['research_dataset'][0], True, response.data)

    def test_create_catalog_record_allowed_projects_ok(self):
        response = self.client.post('/rest/v2/datasets?allowed_projects=project_x', self.cr_test_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)

    def test_create_catalog_record_allowed_projects_fail(self):
        # dataset file not in allowed projects
        response = self.client.post(
            '/rest/v2/datasets?allowed_projects=no,permission', self.cr_test_data, format="json"
        )
        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN, response.data)

        # ensure list is properly handled (separated by comma, end result should be list)
        response = self.client.post('/rest/v2/datasets?allowed_projects=no_good_project_x,another',
            self.cr_test_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN, response.data)

        # handle empty value
        response = self.client.post('/rest/v2/datasets?allowed_projects=', self.cr_test_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN, response.data)

        # Other trickery
        response = self.client.post('/rest/v2/datasets?allowed_projects=,', self.cr_test_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN, response.data)

    #
    # create list operations
    #

    def test_create_catalog_record_list(self):
        response = self.client.post('/rest/v2/datasets',
                                    [self.cr_test_data, self.cr_test_data_new_identifier], format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED)
        self.assertEqual('success' in response.data.keys(), True)
        self.assertEqual('failed' in response.data.keys(), True)
        self.assertEqual('object' in response.data['success'][0].keys(), True)
        self.assertEqual(len(response.data['success']), 2)
        self.assertEqual(len(response.data['failed']), 0)

    def test_create_catalog_record_list_error_one_fails(self):
        self.cr_test_data['research_dataset']["title"] = 1234456
        response = self.client.post('/rest/v2/datasets',
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

        response = self.client.post('/rest/v2/datasets',
                                    [self.cr_test_data, self.cr_test_data_new_identifier], format="json")
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual('success' in response.data.keys(), True)
        self.assertEqual('failed' in response.data.keys(), True)
        self.assertEqual('object' in response.data['failed'][0].keys(), True)
        self.assertEqual(len(response.data['success']), 0)
        self.assertEqual(len(response.data['failed']), 2)

    def test_parameter_migration_override_preferred_identifier_when_creating(self):
        """
        Normally, when saving to att/ida catalogs, providing a custom preferred_identifier is not
        permitted. Using the optional query parameter ?migration_override=bool a custom preferred_identifier
        can be passed.
        """
        custom_pid = 'custom-pid-value'
        self.cr_test_data['research_dataset']['preferred_identifier'] = custom_pid
        response = self.client.post('/rest/v2/datasets?migration_override', self.cr_test_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)
        self.assertEqual(response.data['research_dataset']['preferred_identifier'], custom_pid)

    def test_parameter_migration_override_no_preferred_identifier_when_creating(self):
        """
        Normally, when saving to att/ida catalogs, providing a custom preferred_identifier is not
        permitted. Using the optional query parameter ?migration_override=bool a custom preferred_identifier
        can be passed.
        """
        self.cr_test_data['research_dataset']['preferred_identifier'] = ''
        response = self.client.post('/rest/v2/datasets?migration_override', self.cr_test_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)
        self.assertTrue(len(response.data['research_dataset']['preferred_identifier']) > 0)

        self.cr_test_data['research_dataset'].pop('preferred_identifier', None)
        response = self.client.post('/rest/v2/datasets?migration_override', self.cr_test_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)
        self.assertTrue(len(response.data['research_dataset']['preferred_identifier']) > 0)

    def test_create_catalog_record_using_pid_type(self):
        # Test with pid_type = urn
        self.cr_test_data['research_dataset']['preferred_identifier'] = ''
        response = self.client.post('/rest/v2/datasets?pid_type=urn', self.cr_test_data, format="json")
        self.assertTrue(response.data['research_dataset']['preferred_identifier'].startswith('urn:'))

        # Test with pid_type = doi AND not ida catalog
        self.cr_test_data['research_dataset']['preferred_identifier'] = ''
        response = self.client.post('/rest/v2/datasets?pid_type=doi', self.cr_test_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST, response.data)

        # Create ida data catalog
        dc = self._get_object_from_test_data('datacatalog', requested_index=0)
        dc_id = IDA_CATALOG
        dc['catalog_json']['identifier'] = dc_id
        self.client.post('/rest/v2/datacatalogs', dc, format="json")
        # Test with pid_type = doi AND ida catalog
        self.cr_test_data['research_dataset']['preferred_identifier'] = ''
        self.cr_test_data['data_catalog'] = IDA_CATALOG
        response = self.client.post('/rest/v2/datasets?pid_type=doi', self.cr_test_data, format="json")
        self.assertTrue(response.data['research_dataset']['preferred_identifier'].startswith('doi:10.'))

        # Test with pid_type = not_known
        self.cr_test_data['research_dataset']['preferred_identifier'] = ''
        response = self.client.post('/rest/v2/datasets?pid_type=not_known', self.cr_test_data, format="json")
        self.assertTrue(response.data['research_dataset']['preferred_identifier'].startswith('urn:'))

        # Test without pid_type
        self.cr_test_data['research_dataset']['preferred_identifier'] = ''
        response = self.client.post('/rest/v2/datasets', self.cr_test_data, format="json")
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
        existing_metadata_version_identifier = CatalogRecordV2.objects.get(pk=1).metadata_version_identifier
        self.cr_test_data['research_dataset']['preferred_identifier'] = existing_metadata_version_identifier

        # setting preferred_identifier is only allowed in harvested catalogs.
        self.cr_test_data['data_catalog'] = 3

        response = self.client.post('/rest/v2/datasets', self.cr_test_data, format="json")
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
        cr_1 = self.client.post('/rest/v2/datasets', self.cr_test_data, format="json").data

        self.cr_test_data['research_dataset']['preferred_identifier'] = \
            cr_1['research_dataset']['preferred_identifier']

        response = self.client.post('/rest/v2/datasets', self.cr_test_data, format="json")
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

        response = self.client.post('/rest/v2/datasets', self.cr_test_data, format="json")
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

        cr = CatalogRecordV2.objects.get(pk=3)
        cr.data_catalog_id = 3
        cr.save()

        data = self.client.get('/rest/v2/datasets/3').data
        data['research_dataset']['preferred_identifier'] = unique_identifier

        response = self.client.patch('/rest/v2/datasets/3', data, format="json")
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

        data = self.client.get('/rest/v2/datasets/3').data
        data['research_dataset']['preferred_identifier'] = unique_identifier
        data['data_catalog'] = 3

        response = self.client.patch('/rest/v2/datasets/3', data, format="json")
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

        response = self.client.patch('/rest/v2/datasets/2', data, format="json")
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
        cr = CatalogRecordV2.objects.get(pk=pk)
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

    def _set_data_catalog_schema_to_harvester(self):
        dc = DataCatalog.objects.get(pk=1)
        dc.catalog_json['research_dataset_schema'] = 'harvester'
        dc.save()

    def setUp(self):
        super().setUp()
        self._set_data_catalog_schema_to_harvester()
        self.cr_test_data['research_dataset']['preferred_identifier'] = 'unique_pid'

    def test_catalog_record_with_not_found_json_schema_gets_default_schema(self):
        # catalog has dataset schema, but it is not found on the server
        dc = DataCatalog.objects.get(pk=1)
        dc.catalog_json['research_dataset_schema'] = 'nonexisting'
        dc.save()

        response = self.client.post('/rest/v2/datasets', self.cr_test_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)

        # catalog has no dataset schema at all
        dc = DataCatalog.objects.get(pk=1)
        dc.catalog_json.pop('research_dataset_schema')
        dc.save()

        response = self.client.post('/rest/v2/datasets', self.cr_test_data, format="json")
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

        response = self.client.post('/rest/v2/datasets', self.cr_test_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)

        self.cr_test_data['research_dataset']['remote_resources'] = [
            {'title': 'title'},
            {'title': 'title'},
            {'woah': 'this should give a failure, since title is a required field, and it is missing'}
        ]

        response = self.client.post('/rest/v2/datasets', self.cr_test_data, format="json")
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

        response = self.client.post('/rest/v2/datasets', self.cr_test_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)
        self.assertEqual(
            'uri.suomi.fi' in response.data['research_dataset']['other_identifier'][0]['type']['identifier'],
            True,
            'Identifier type should have been populated with data from ref data'
        )


class CatalogRecordApiWriteUpdateTests(CatalogRecordApiWriteCommon):
    #
    #
    # update apis PUT
    #
    #

    def test_update_catalog_record(self):
        cr = self.client.get('/rest/v2/datasets/1').data
        cr['preservation_description'] = 'what'

        response = self.client.put('/rest/v2/datasets/1', cr, format="json")
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        self.assertEqual(response.data['preservation_description'], 'what')
        cr = CatalogRecordV2.objects.get(pk=1)
        self.assertEqual(cr.date_modified >= get_tz_aware_now_without_micros() - timedelta(seconds=5), True,
                         'Timestamp should have been updated during object update')

    def test_update_catalog_record_error_using_preferred_identifier(self):
        cr = self.client.get('/rest/v2/datasets/1').data
        response = self.client.put('/rest/v2/datasets/%s' % cr['research_dataset']['preferred_identifier'],
                                   { 'whatever': 123 }, format="json")
        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND,
                         'Update operation should return 404 when using preferred_identifier')

    def test_update_catalog_record_error_required_fields(self):
        """
        Field 'research_dataset' is missing, which should result in an error, since PUT
        replaces an object and requires all 'required' fields to be present.
        """
        cr = self.client.get('/rest/v2/datasets/1').data
        cr.pop('research_dataset')
        response = self.client.put('/rest/v2/datasets/1', cr, format="json")

        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual('research_dataset' in response.data.keys(), True,
                         'Error for field \'research_dataset\' is missing from response.data')

    def test_update_catalog_record_not_found(self):
        response = self.client.put('/rest/v2/datasets/doesnotexist', self.cr_test_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND)

    def test_update_catalog_record_contract(self):
        # take any cr that has a contract set
        cr = CatalogRecordV2.objects.filter(contract_id__isnull=False).first()
        old_contract_id = cr.contract.id

        # update contract to any different contract
        cr_1 = self.client.get('/rest/v2/datasets/%d' % cr.id).data
        cr_1['contract'] = Contract.objects.all().exclude(pk=old_contract_id).first().id

        response = self.client.put('/rest/v2/datasets/%d' % cr.id, cr_1, format="json")
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        new_contract_id = CatalogRecordV2.objects.get(pk=cr.id).contract.id
        self.assertNotEqual(old_contract_id, new_contract_id, 'Contract should have changed')

    #
    # update list operations PUT
    #

    def test_catalog_record_update_list(self):
        cr_1 = self.client.get('/rest/v2/datasets/1').data
        cr_1['preservation_description'] = 'updated description'

        cr_2 = self.client.get('/rest/v2/datasets/2').data
        cr_2['preservation_description'] = 'second updated description'

        response = self.client.put('/rest/v2/datasets', [ cr_1, cr_2 ], format="json")
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        self.assertEqual(len(response.data['success']), 2)

        updated_cr = CatalogRecordV2.objects.get(pk=1)
        self.assertEqual(updated_cr.preservation_description, 'updated description')
        updated_cr = CatalogRecordV2.objects.get(pk=2)
        self.assertEqual(updated_cr.preservation_description, 'second updated description')

    def test_catalog_record_update_list_error_one_fails(self):
        cr_1 = self.client.get('/rest/v2/datasets/1').data
        cr_1['preservation_description'] = 'updated description'

        # data catalog is a required field, should therefore fail
        cr_2 = self.client.get('/rest/v2/datasets/2').data
        cr_2.pop('data_catalog', None)

        response = self.client.put('/rest/v2/datasets', [ cr_1, cr_2 ], format="json")
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        self.assertEqual('success' in response.data.keys(), True)
        self.assertEqual('failed' in response.data.keys(), True)
        self.assertEqual(isinstance(response.data['success'], list), True,
                         'return data should contain key success, which is a list')
        self.assertEqual(len(response.data['success']), 1)
        self.assertEqual(len(response.data['failed']), 1)

        updated_cr = CatalogRecordV2.objects.get(pk=1)
        self.assertEqual(updated_cr.preservation_description, 'updated description')

    def test_catalog_record_update_list_error_key_not_found(self):
        # does not have identifier key
        cr_1 = self.client.get('/rest/v2/datasets/1').data
        cr_1.pop('id')
        cr_1.pop('identifier')
        cr_1['research_dataset'].pop('metadata_version_identifier')

        cr_2 = self.client.get('/rest/v2/datasets/2').data
        cr_2['preservation_description'] = 'second updated description'

        response = self.client.put('/rest/v2/datasets', [ cr_1, cr_2 ], format="json")
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
        response = self.client.post('/rest/v2/datasets', self.cr_test_data, format="json")
        self.assertEqual(response.data['deprecated'], False)
        self.assertTrue('date_deprecated' not in response.data)

        response_json = self.client.get('/rest/v2/datasets/1').data
        initial_deprecated = response_json['deprecated']
        response_json['deprecated'] = not initial_deprecated
        response_json['date_deprecated'] = '2018-01-01T00:00:00'
        response = self.client.put('/rest/v2/datasets/1', response_json, format="json")
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        self.assertEqual(response.data['deprecated'], initial_deprecated)
        self.assertTrue('date_deprecated' not in response.data)

        initial_deprecated = self.client.get('/rest/v2/datasets/1').data['deprecated']
        response = self.client.patch('/rest/v2/datasets/1', { 'deprecated': not initial_deprecated }, format="json")
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        self.assertEqual(response.data['deprecated'], initial_deprecated)
        self.assertTrue('date_deprecated' not in response.data)

    def test_catalog_record_deprecation_updates_date_modified(self):
        cr = CatalogRecordV2.objects.filter(files__id=1)
        cr_id = cr[0].identifier

        response = self.client.delete('/rest/v2/files/1')
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)

        cr_depr = CatalogRecordV2.objects.get(identifier=cr_id)
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
        response = self.client.patch('/rest/v2/datasets/%s' % self.identifier, new_data, format="json")

        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        self.assertEqual('research_dataset' in response.data.keys(), True, 'PATCH operation should return full content')
        self.assertEqual(response.data['data_catalog']['id'], new_data_catalog, 'Field data_catalog was not updated')

    #
    # update list operations PATCH
    #

    def test_catalog_record_partial_update_list(self):
        test_data = {}
        test_data['id'] = 1
        test_data['preservation_description'] = 'description'

        second_test_data = {}
        second_test_data['id'] = 2
        second_test_data['preservation_description'] = 'description 2'

        response = self.client.patch('/rest/v2/datasets', [test_data, second_test_data], format="json")
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        self.assertEqual('success' in response.data, True, 'response.data should contain list of changed objects')
        self.assertEqual(len(response.data), 2, 'response.data should contain 2 changed objects')
        self.assertEqual('research_dataset' in response.data['success'][0]['object'], True,
                         'response.data should contain full objects')

        updated_cr = CatalogRecordV2.objects.get(pk=1)
        self.assertEqual(updated_cr.preservation_description, 'description')

    def test_catalog_record_partial_update_list_error_one_fails(self):
        test_data = {}
        test_data['id'] = 1
        test_data['preservation_description'] = 'description'

        second_test_data = {}
        second_test_data['preservation_state'] = 555  # value not allowed
        second_test_data['id'] = 2

        response = self.client.patch('/rest/v2/datasets', [test_data, second_test_data], format="json")
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

        response = self.client.patch('/rest/v2/datasets', [test_data, second_test_data], format="json")
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
        url = '/rest/v2/datasets/%s' % self.identifier
        response = self.client.delete(url)
        self.assertEqual(response.status_code, status.HTTP_204_NO_CONTENT)
        response = self.client.get(url)
        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND)

        try:
            deleted_catalog_record = CatalogRecordV2.objects.get(identifier=self.identifier)
            raise Exception('Deleted CatalogRecord should not be retrievable from the default objects table')
        except CatalogRecordV2.DoesNotExist:
            # successful test should go here, instead of raising the expection in try: block
            pass

        try:
            deleted_catalog_record = CatalogRecordV2.objects_unfiltered.get(identifier=self.identifier)
        except CatalogRecordV2.DoesNotExist:
            raise Exception('Deleted CatalogRecord should not be deleted from the db, but marked as removed')

        self.assertEqual(deleted_catalog_record.removed, True)
        self.assertEqual(deleted_catalog_record.identifier, self.identifier)
        self.assertEqual(deleted_catalog_record.date_modified, deleted_catalog_record.date_removed,
                        'date_modified should be updated')

    def test_delete_catalog_record_error_using_preferred_identifier(self):
        url = '/rest/v2/datasets/%s' % self.preferred_identifier
        response = self.client.delete(url)
        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND)


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

        existing_records_count = CatalogRecordV2.objects.filter(
            research_dataset__contains={'preferred_identifier': self.preferred_identifier}).count()
        self.assertEqual(existing_records_count, 1,
                         'in the beginning, there should be only one record with pref id %s'
                         % self.preferred_identifier)

        response = self.client.post('/rest/v2/datasets', self.cr_test_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)

        records = CatalogRecordV2.objects.filter(
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

        existing_records_count = CatalogRecordV2.objects.filter(
            research_dataset__contains={'preferred_identifier': self.preferred_identifier}).count()
        self.assertEqual(existing_records_count, 2,
            'in the beginning, there should be two records with pref id %s' % self.preferred_identifier)

        response = self.client.post('/rest/v2/datasets', self.cr_test_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED)

        records = CatalogRecordV2.objects.filter(
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

        response = self.client.delete('/rest/v2/datasets/2', format="json")
        self.assertEqual(response.status_code, status.HTTP_204_NO_CONTENT)

        # check resulting conditions
        records = CatalogRecordV2.objects.filter(
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
        old_ars_id = CatalogRecordV2.objects.get(pk=2).alternate_record_set.id

        # retrieve record id=2, and change its preferred identifier
        response = self.client.get('/rest/v2/datasets/2', format="json")
        data = {'research_dataset': response.data['research_dataset']}
        data['research_dataset']['preferred_identifier'] = 'a:new:identifier:here'

        # updating preferred_identifier - a new version is NOT created
        response = self.client.patch('/rest/v2/datasets/2', data=data, format="json")
        self.assertEqual(response.status_code, status.HTTP_200_OK)

        records = CatalogRecordV2.objects.filter(
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
        old_ars_id = CatalogRecordV2.objects.get(pk=2).alternate_record_set.id

        response = self.client.delete('/rest/v2/datasets/2', format="json")
        self.assertEqual(response.status_code, status.HTTP_204_NO_CONTENT)

        records = CatalogRecordV2.objects.filter(
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

        response_1 = self.client.post('/rest/v2/datasets', self.cr_test_data, format="json")
        response_2 = self.client.get('/rest/v2/datasets/1', format="json")
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
        response_3 = self.client.post('/rest/v2/datasets', self.cr_test_data, format="json")
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

        response_2 = self.client.get('/rest/v2/datasets/1', format="json")
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
        cr = CatalogRecordV2.objects.get(pk=pk)
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
        records = CatalogRecordV2.objects.filter(
            research_dataset__contains={'preferred_identifier': self.preferred_identifier})
        self.assertEqual(len(records), 3,
            'in the beginning, there should be three records with pref id %s' % self.preferred_identifier)
        ars_id = records[0].alternate_record_set.id
        self.assertEqual(records[0].alternate_record_set.id, ars_id)
        self.assertEqual(records[1].alternate_record_set.id, ars_id)
        self.assertEqual(records[2].alternate_record_set.id, ars_id)

    def test_update_rd_title_creates_new_metadata_version(self):
        """
        Updating the title of metadata should create a new metadata version.
        """
        response_1 = self._get_and_update_title(self.pk)
        self.assertEqual(response_1.status_code, status.HTTP_200_OK, response_1.data)
        self._assert_metadata_version_count(response_1.data, 2)

        # get list of metadata versions to access contents...
        response = self.client.get('/rest/v2/datasets/%d/metadata_versions' % response_1.data['id'], format="json")

        response_2 = self.client.get('/rest/v2/datasets/%d/metadata_versions/%s' %
            (self.pk, response.data[0]['metadata_version_identifier']), format="json")
        self.assertEqual(response_2.status_code, status.HTTP_200_OK, response_2.data)
        self.assertEqual('preferred_identifier' in response_2.data, True)

        # note! response_1 == cr, response_2 == rd
        self.assertEqual(response_1.data['research_dataset']['preferred_identifier'],
            response_2.data['preferred_identifier'])

    def test_dataset_version_lists_removed_records(self):

        # create version2 of a record
        response = self.client.post('/rpc/v2/datasets/create_new_version?identifier=1', format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)

        new_version_id = response.data['id']

        # publish the new version
        response = self.client.post(f'/rpc/v2/datasets/publish_dataset?identifier={new_version_id}', format="json")
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)

        # delete version2
        response = self.client.delete(f'/rest/v2/datasets/{new_version_id}', format="json")
        self.assertEqual(response.status_code, status.HTTP_204_NO_CONTENT, response.data)

        # check date_removed is listed and not None in deleted version
        response = self.client.get('/rest/v2/datasets/1', format="json")
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)

        self.assertTrue(response.data['dataset_version_set'][0].get('date_removed'))
        self.assertTrue(response.data['dataset_version_set'][0].get('date_removed') is not None)
        self.assertFalse(response.data['dataset_version_set'][1].get('date_removed'))

    def _assert_metadata_version_count(self, record, count):
        response = self.client.get('/rest/v2/datasets/%d/metadata_versions' % record['id'], format="json")
        self.assertEqual(len(response.data), count)

    def _set_cr_to_catalog(self, pk=None, dc=None):
        cr = CatalogRecordV2.objects.get(pk=pk)
        cr.data_catalog_id = dc
        cr.force_save()

    def _get_and_update_title(self, pk, params=None):
        """
        Get, modify, and update data for given pk. The modification should cause a new
        version to be created if the catalog permits.

        Should not force preferred_identifier to change.
        """
        data = self.client.get('/rest/v2/datasets/%d' % pk, format="json").data
        data['research_dataset']['title']['en'] = 'modified title'
        return self.client.put('/rest/v2/datasets/%d%s' % (pk, params or ''), data, format="json")

    def test_allow_metadata_changes_after_deprecation(self):
        """
        For deprecated datasets metadata changes are still allowed. Changing user metadata for files that
        are marked as removed (caused the deprecation) is not possible.
        """
        response = self.client.get('/rest/v2/datasets/1?include_user_metadata')
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        cr = response.data

        response = self.client.delete('/rest/v2/files/1')
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)

        # after the dataset is deprecated, metadata updates should still be ok
        cr['research_dataset']['description'] = {
            "en": "Updating new description for deprecated dataset should not create any problems"
        }

        response = self.client.put('/rest/v2/datasets/%s' % cr['id'], cr, format="json")
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        self.assertTrue('new description' in response.data['research_dataset']['description']['en'],
            'description field should be updated')

        file_changes = {
            'files': [ cr['research_dataset']['files'][0] ]
        }

        file_changes['files'][0]['title'] = 'Brand new title 1'

        response = self.client.put('/rest/v2/datasets/%s/files/user_metadata' % cr['id'], file_changes, format="json")
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST, response.data)
        self.assertEqual('The following files are not' in response.data['detail'][0], True, response.data)


class CatalogRecordApiWriteRemoteResources(CatalogRecordApiWriteCommon):

    """
    remote_resources related tests
    """

    def test_calculate_total_remote_resources_byte_size(self):
        cr_with_rr = self._get_object_from_test_data('catalogrecord', requested_index=14)
        rr = cr_with_rr['research_dataset']['remote_resources']
        total_remote_resources_byte_size = sum(res['byte_size'] for res in rr)
        self.cr_att_test_data['research_dataset']['remote_resources'] = rr
        response = self.client.post('/rest/v2/datasets', self.cr_att_test_data, format="json")
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
            response = self.client.post('/rest/v2/datasets', self.cr_test_data, format="json")
            self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)
            self.assertEqual(response.data['research_dataset']['preferred_identifier'], 'a')
            same_pid_ids.append(response.data['id'])

        # pid can even be same as an existing dataset's pid in an ATT catalog
        real_pid = CatalogRecordV2.objects.get(pk=1).preferred_identifier
        self.cr_test_data['research_dataset']['preferred_identifier'] = real_pid
        response = self.client.post('/rest/v2/datasets', self.cr_test_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)
        self.assertEqual(response.data['research_dataset']['preferred_identifier'], real_pid)

    def test_legacy_catalog_pid_must_be_provided(self):
        # pid cant be empty string
        self.cr_test_data['data_catalog'] = LEGACY_CATALOGS[0]
        self.cr_test_data['research_dataset']['preferred_identifier'] = ''
        response = self.client.post('/rest/v2/datasets', self.cr_test_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST, response.data)

        # pid cant be omitted
        del self.cr_test_data['research_dataset']['preferred_identifier']
        response = self.client.post('/rest/v2/datasets', self.cr_test_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST, response.data)

    def test_legacy_catalog_pids_update(self):
        # test setup
        self.cr_test_data['data_catalog'] = LEGACY_CATALOGS[0]
        self.cr_test_data['research_dataset']['preferred_identifier'] = 'a'
        response = self.client.post('/rest/v2/datasets', self.cr_test_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)

        # update record. in updates uniqueness also should not be checked
        modify = response.data
        real_pid = CatalogRecordV2.objects.get(pk=1).preferred_identifier
        modify['research_dataset']['preferred_identifier'] = real_pid
        response = self.client.put('/rest/v2/datasets/%s' % modify['id'], modify, format="json")
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)

    def test_delete_legacy_catalog_dataset(self):
        """
        Datasets in legacy catalogs should be deleted permanently, instead of only marking them
        as 'removed'.
        """

        # test setup
        self.cr_test_data['data_catalog'] = LEGACY_CATALOGS[0]
        self.cr_test_data['research_dataset']['preferred_identifier'] = 'a'
        response = self.client.post('/rest/v2/datasets', self.cr_test_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)
        cr_id = response.data['id']

        # delete record
        response = self.client.delete('/rest/v2/datasets/%s' % cr_id, format="json")
        self.assertEqual(response.status_code, status.HTTP_204_NO_CONTENT, response.data)
        results_count = CatalogRecordV2.objects_unfiltered.filter(pk=cr_id).count()
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
        cr = self.client.get('/rest/v2/datasets/1', format="json").data
        cr.pop('id')
        cr.pop('identifier')
        cr.pop('metadata_owner_org')
        cr['research_dataset'].pop('preferred_identifier')
        response = self.client.post('/rest/v2/datasets', cr, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)
        self.assertEqual(response.data['metadata_owner_org'], response.data['metadata_provider_org'])

        # update to null - update is prevented
        cr = self.client.get('/rest/v2/datasets/1', format="json").data
        original = cr['metadata_owner_org']
        cr['metadata_owner_org'] = None
        response = self.client.put('/rest/v2/datasets/1', cr, format="json")
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        self.assertEqual(response.data['metadata_owner_org'], original)

        # update with patch, where metadata_owner_org field is absent - value is not reverted back
        # to metadata_provider_org
        response = self.client.patch('/rest/v2/datasets/1', { 'metadata_owner_org': 'abc' }, format="json")
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        response = self.client.patch('/rest/v2/datasets/1', { 'contract': 1 }, format="json")
        self.assertEqual(response.data['metadata_owner_org'], 'abc')

    def test_metadata_provider_org_is_readonly_after_creating(self):
        cr = self.client.get('/rest/v2/datasets/1', format="json").data
        original = cr['metadata_provider_org']
        cr['metadata_provider_org'] = 'changed'
        response = self.client.put('/rest/v2/datasets/1', cr, format="json")
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        self.assertEqual(response.data['metadata_provider_org'], original)

    def test_metadata_provider_user_is_readonly_after_creating(self):
        cr = self.client.get('/rest/v2/datasets/1', format="json").data
        original = cr['metadata_provider_user']
        cr['metadata_provider_user'] = 'changed'
        response = self.client.put('/rest/v2/datasets/1', cr, format="json")
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
            dc = DataCatalog.objects.create(
                catalog_json=catalog_json,
                date_created=get_tz_aware_now_without_micros(),
                catalog_record_services_create='testuser,api_auth_user,metax',
                catalog_record_services_edit='testuser,api_auth_user,metax'
            )

        self.token = get_test_oidc_token()

        # by default, use the unmodified token. to use a different/modified token
        # for various test scenarions, alter self.token, and call the below method again
        self._use_http_authorization(method='bearer', token=self.token)

        # no reason to test anything related to failed authentication, since failed
        # authentication stops the request from proceeding anywhere
        self._mock_token_validation_succeeds()

    def _set_cr_owner_to_token_user(self, cr_id):
        cr = CatalogRecordV2.objects.get(pk=cr_id)
        cr.user_created = self.token['CSCUserName']
        cr.metadata_provider_user = self.token['CSCUserName']
        cr.editor = None # pretend the record was created by user directly
        cr.force_save()

    def _set_cr_to_permitted_catalog(self, cr_id):
        cr = CatalogRecordV2.objects.get(pk=cr_id)
        cr.data_catalog_id = DataCatalog.objects.get(catalog_json__identifier=END_USER_ALLOWED_DATA_CATALOGS[0]).id
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

        response = self.client.post('/rest/v2/datasets', self.cr_test_data, format="json")

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
        response = self.client.post('/rest/v2/datasets', self.cr_test_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN, response.data)
        # check error has expected error description
        self.assertEqual('selected data catalog' in response.data['detail'][0], True, response.data)

        # should work
        for identifier in END_USER_ALLOWED_DATA_CATALOGS:
            if identifier in LEGACY_CATALOGS:
                self.cr_test_data['research_dataset']['preferred_identifier'] = 'a'
            self.cr_test_data['data_catalog'] = identifier
            response = self.client.post('/rest/v2/datasets', self.cr_test_data, format="json")
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
        response = self.client.post('/rest/v2/datasets', self.cr_test_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)

        modified_data = response.data
        # research_dataset is the only permitted field to edit
        modified_data['research_dataset']['value'] = 112233
        modified_data['contract'] = 1
        modified_data['editor'] = { 'nope': 'discarded by metax' }
        modified_data['preservation_description'] = 'discarded by metax'
        modified_data['preservation_reason_description'] = 'discarded by metax'
        modified_data['preservation_state'] = 10

        response = self.client.put('/rest/v2/datasets/%d' % modified_data['id'], modified_data, format="json")
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
        response = self.client.post('/rest/v2/datasets', self.cr_test_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)

        modified_data = response.data
        modified_data['research_dataset']['value'] = 112233

        self._use_http_authorization(method='bearer', token=self.token)
        response = self.client.put('/rest/v2/datasets/%d' % modified_data['id'], modified_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN, response.data)

    @responses.activate
    def test_other_users_cant_edit_dataset(self):
        '''
        Ensure end users are unable edit datasets not owned by them.
        '''
        response = self.client.get('/rest/v2/datasets/1', format="json")
        modified_data = response.data
        modified_data['research_dataset']['value'] = 112233

        response = self.client.put('/rest/v2/datasets/1', modified_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN)

        response = self.client.put('/rest/v2/datasets', [modified_data], format="json")
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        # ^ individual errors do not have error codes, only the general request
        # has an error code for a failed request.

    @responses.activate
    def test_user_can_delete_dataset(self):
        self._set_cr_owner_to_token_user(1)
        self._set_cr_to_permitted_catalog(1)
        response = self.client.delete('/rest/v2/datasets/1', format="json")
        self.assertEqual(response.status_code, status.HTTP_204_NO_CONTENT, response.data)

    @responses.activate
    def test_user_file_permissions_are_checked_during_dataset_create(self):
        '''
        Ensure user's association with a project is checked during dataset create when
        attaching files or directories to a dataset.
        '''

        # try creating without proper permisisons
        self.cr_test_data['data_catalog'] = END_USER_ALLOWED_DATA_CATALOGS[0] # ida
        response = self.client.post('/rest/v2/datasets', self.cr_test_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN, response.content)

        # add project membership to user's token and try again
        file_identifier = self.cr_test_data['research_dataset']['files'][0]['identifier']
        project_identifier = File.objects.get(identifier=file_identifier).project_identifier
        self.token['group_names'].append('IDA01:%s' % project_identifier)
        self._use_http_authorization(method='bearer', token=self.token)

        response = self.client.post('/rest/v2/datasets', self.cr_test_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.content)

    @responses.activate
    def test_user_file_permissions_are_checked_during_dataset_update(self):
        '''
        Ensure user's association with a project is checked during dataset update when
        attaching files or directories to a dataset. The permissions should be checked
        only for changed files (newly added, or removed).
        '''
        # get some files to add to another dataset
        new_files = CatalogRecordV2.objects.get(pk=1).research_dataset['files']

        self.cr_test_data['data_catalog'] = END_USER_ALLOWED_DATA_CATALOGS[0] # ida
        self.cr_test_data['research_dataset'].pop('files', None)
        self.cr_test_data['research_dataset'].pop('directories', None)
        response = self.client.post('/rest/v2/datasets', self.cr_test_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)

        cr_id = response.data['id']

        file_changes = {
            'files': new_files
        }

        # should fail, since user's token has no permission for the newly added files
        response = self.client.post(f'/rest/v2/datasets/{cr_id}/files', file_changes, format="json")
        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN, response.content)

        # add project membership to user's token and try again
        project_identifier = File.objects.get(identifier=new_files[0]['identifier']).project_identifier
        self.token['group_names'].append('IDA01:%s' % project_identifier)
        self._use_http_authorization(method='bearer', token=self.token)

        response = self.client.post(f'/rest/v2/datasets/{cr_id}/files', file_changes, format="json")
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

        for cr in CatalogRecordV2.objects.filter(pk=1):
            cr.research_dataset['curator'][0].update({
                'email': 'email@mail.com',
                'phone': '123124',
                'telephone': '123124',
            })
            cr.force_save()

        response = self.client.get('/rest/v2/datasets/1')
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        _check_fields(response.data)


class CatalogRecordExternalServicesAccess(CatalogRecordApiWriteCommon):

    """
    Testing access of services to external catalogs with harvested flag and vice versa.
    """

    def setUp(self):
        """
        Create a test-datacatalog that plays the role as a external catalog.
        """
        super().setUp()

        self.dc = DataCatalog.objects.filter(catalog_json__research_dataset_schema='att').first()
        self.dc.catalog_json['identifier'] = EXT_CATALOG
        self.dc.catalog_json['harvested'] = True
        self.dc.catalog_record_services_create = 'external'
        self.dc.catalog_record_services_edit = 'external'
        self.dc.force_save()

        self.cr_test_data['data_catalog'] = self.dc.catalog_json['identifier']
        del self.cr_test_data['research_dataset']['files']
        del self.cr_test_data['research_dataset']['total_files_byte_size']

        self._use_http_authorization(username=django_settings.API_EXT_USER['username'],
            password=django_settings.API_EXT_USER['password'])

    def test_external_service_can_not_read_all_metadata_in_other_catalog(self):
        ''' External service should get the same output from someone elses catalog than anonymous user '''
        # create a catalog that does not belong to our external service
        dc2 = DataCatalog.objects.get(pk=2)
        dc2.catalog_json['identifier'] = 'Some other catalog'
        dc2.catalog_record_services_read = 'metax'
        dc2.force_save()

        # Create a catalog record that belongs to some other user & our catalog nr2
        cr = CatalogRecordV2.objects.get(pk=12)
        cr.user_created = '#### Some owner who is not you ####'
        cr.metadata_provider_user = '#### Some owner who is not you ####'
        cr.data_catalog = dc2
        cr.editor = None
        cr.research_dataset['access_rights']['access_type']['identifier'] = ACCESS_TYPES['restricted']
        cr.force_save()

        # Let's try to return the data with our external services credentials
        response_service_user = self.client.get('/rest/v2/datasets/12')
        self.assertEqual(response_service_user.status_code, status.HTTP_200_OK, response_service_user.data)

        # Test access as unauthenticated user
        self.client._credentials = {}
        response_anonymous = self.client.get('/rest/v2/datasets/12')
        self.assertEqual(response_anonymous.status_code, status.HTTP_200_OK, response_anonymous.data)

        self.assertEqual(response_anonymous.data, response_service_user.data,
            "External service with no read-rights should not see any more metadata than anonymous user from a catalog")

    def test_external_service_can_add_catalog_record_to_own_catalog(self):
        self.cr_test_data['research_dataset']['preferred_identifier'] = '123456'
        response = self.client.post('/rest/v2/datasets', self.cr_test_data, format="json")

        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)
        self.assertEqual(response.data['research_dataset']['preferred_identifier'], '123456')

    def test_external_service_can_update_catalog_record_in_own_catalog(self):
        self.cr_test_data['research_dataset']['preferred_identifier'] = '123456'
        response = self.client.post('/rest/v2/datasets', self.cr_test_data, format="json")

        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)
        self.assertEqual(response.data['research_dataset']['preferred_identifier'], '123456')

        cr_id = response.data['id']
        self.cr_test_data['research_dataset']['preferred_identifier'] = '654321'
        response = self.client.put('/rest/v2/datasets/{}'.format(cr_id), self.cr_test_data, format="json")

        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        self.assertEqual(response.data['research_dataset']['preferred_identifier'], '654321')

    def test_external_service_can_delete_catalog_record_from_own_catalog(self):
        self.cr_test_data['research_dataset']['preferred_identifier'] = '123456'
        response = self.client.post('/rest/v2/datasets', self.cr_test_data, format="json")

        cr_id = response.data['id']
        response = self.client.delete('/rest/v2/datasets/{}'.format(cr_id))
        self.assertEqual(response.status_code, status.HTTP_204_NO_CONTENT, response.data)

        response = self.client.get('/rest/v2/datasets/{}'.format(cr_id), format="json")
        self.assertEqual('not found' in response.json()['detail'].lower(), True)

    def test_external_service_can_not_add_catalog_record_to_other_catalog(self):
        dc = self._get_object_from_test_data('datacatalog', requested_index=1)
        self.cr_test_data['data_catalog'] = dc['catalog_json']['identifier']
        self.cr_test_data['research_dataset']['preferred_identifier'] = 'temp-pid'
        response = self.client.post('/rest/v2/datasets', self.cr_test_data, format="json")

        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN, response.data)

    def test_external_service_can_not_update_catalog_record_in_other_catalog(self):
        response = self.client.put('/rest/v2/datasets/1', {}, format="json")

        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN, response.data)

    def test_external_service_can_not_delete_catalog_record_from_other_catalog(self):
        response = self.client.delete('/rest/v2/datasets/1')

        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN, response.data)

    def test_harvested_catalogs_must_have_preferred_identifier_create(self):
        # create without preferred identifier

        response = self.client.post('/rest/v2/datasets', self.cr_test_data, format="json")

        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST, response.data)
        self.assertEqual('must have preferred identifier' in
                response.data['research_dataset']['preferred_identifier'][0], True)

        self.cr_test_data['research_dataset']['preferred_identifier'] = ''
        response = self.client.post('/rest/v2/datasets', self.cr_test_data, format="json")

        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST, response.data)
        self.assertEqual('must have preferred identifier' in
                response.data['research_dataset']['preferred_identifier'][0], True)
