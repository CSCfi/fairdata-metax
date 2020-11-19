# This file is part of the Metax API service
#
# Copyright 2017-2020 Ministry of Education and Culture, Finland
#
# :author: CSC - IT Center for Science Ltd., Espoo Finland <servicedesk@csc.fi>
# :license: MIT

import unittest
from copy import deepcopy

import responses
from django.conf import settings as django_settings
from rest_framework import status

from metax_api.services import ReferenceDataMixin as RDM, RedisCacheService as cache
from metax_api.tests.utils import get_test_oidc_token
from .write import CatalogRecordApiWriteCommon

IDA_CATALOG = django_settings.IDA_DATA_CATALOG_IDENTIFIER


@unittest.skipIf(django_settings.REMS['ENABLED'] is not True, 'Only run if REMS is enabled')
class CatalogRecordApiWriteREMS(CatalogRecordApiWriteCommon):

    rf = RDM.get_reference_data(cache)
    # get by code to prevent failures if list ordering changes
    access_permit = [type for type in rf['reference_data']['access_type'] if type['code'] == 'permit'][0]
    access_open = [type for type in rf['reference_data']['access_type'] if type['code'] == 'open'][0]

    permit_rights = {
        # license type does not matter
        "license": [
            {
                "title": rf['reference_data']['license'][0]['label'],
                "identifier": rf['reference_data']['license'][0]['uri']
            }
        ],
        "access_type": {
            "in_scheme": access_permit['scheme'],
            "identifier": access_permit['uri'],
            "pref_label": access_permit['label']
        }
    }

    open_rights = {
        "access_type": {
            "in_scheme": access_open['scheme'],
            "identifier": access_open['uri'],
            "pref_label": access_open['label']
        }
    }

    # any other than what is included in permit_rights is sufficient
    other_license = rf['reference_data']['license'][1]

    def setUp(self):
        super().setUp()
        # Create ida data catalog
        dc = self._get_object_from_test_data('datacatalog', requested_index=0)
        dc_id = IDA_CATALOG
        dc['catalog_json']['identifier'] = dc_id
        self.client.post('/rest/v2/datacatalogs', dc, format="json")

        # token for end user access
        self.token = get_test_oidc_token(new_proxy=True)

        # mock successful rems access for creation, add fails later if needed.
        # Not using regex to allow individual access failures
        for entity in ['user', 'workflow', 'license', 'resource', 'catalogue-item']:
            self._mock_rems_write_access_succeeds('POST', entity, 'create')

        self._mock_rems_read_access_succeeds('license')

        # mock successful rems access for deletion. Add fails later
        for entity in ['catalogue-item', 'workflow', 'resource']:
            self._mock_rems_write_access_succeeds(method='PUT', entity=entity, action='archived')
            self._mock_rems_write_access_succeeds(method='PUT', entity=entity, action='enabled')

        self._mock_rems_read_access_succeeds('catalogue-item')
        self._mock_rems_read_access_succeeds('application')
        self._mock_rems_write_access_succeeds(method='POST', entity='application', action='close')

        responses.add(
            responses.GET,
            f"{django_settings.REMS['BASE_URL']}/health",
            json={'healthy': True},
            status=200
        )

    def _get_access_granter(self, malformed=False):
        """
        Returns user information
        """
        access_granter = {
            "userid": "testcaseuser" if not malformed else 1234,
            "name": "Test User",
            "email": "testcase@user.com"
        }

        return access_granter

    def _mock_rems_write_access_succeeds(self, method, entity, action):
        """
        method: HTTP method to be mocked [PUT, POST]
        entity: REMS entity [application, catalogue-item, license, resource, user, workflow]
        action: Action taken to entity [archived, close, create, edit, enabled]
        """
        req_type = responses.POST if method == 'POST' else responses.PUT

        body = {"success": True}

        if method == 'POST' and action != 'close':
            # action condition needed because applications are closed with POST method
            body['id'] = 6

        responses.add(
            req_type,
            f"{django_settings.REMS['BASE_URL']}/{entity}s/{action}",
            json=body,
            status=200
        )

    def _mock_rems_read_access_succeeds(self, entity):
        if entity == 'license':
            resp = [
                {
                    "id": 7,
                    "licensetype": "link",
                    "enabled": True,
                    "archived": False,
                    "localizations": {
                        "fi": {
                            "title": self.rf['reference_data']['license'][0]['label']['fi'],
                            "textcontent": self.rf['reference_data']['license'][0]['uri']
                        },
                        "und": {
                            "title": self.rf['reference_data']['license'][0]['label']['und'],
                            "textcontent": self.rf['reference_data']['license'][0]['uri']
                        }
                    }
                },
                {
                    "id": 8,
                    "licensetype": "link",
                    "enabled": True,
                    "archived": False,
                    "localizations": {
                        "en": {
                            "title": self.rf['reference_data']['license'][1]['label']['en'],
                            "textcontent": self.rf['reference_data']['license'][1]['uri']
                        }
                    }
                }
            ]

        elif entity == 'catalogue-item':
            resp = [
                {
                    "archived": False,
                    "localizations": {
                        "en": {
                            "id": 18,
                            "langcode": "en",
                            "title": "Removal test",
                            "infourl": "https://url.to.etsin.fi"
                        }
                    },
                    "resource-id": 19,
                    "start": "2020-01-02T14:06:13.496Z",
                    "wfid": 15,
                    "resid": "preferred identifier",
                    "formid": 3,
                    "id": 18,
                    "expired": False,
                    "end": None,
                    "enabled": True
                }
            ]

        elif entity == 'application':
            # only mock relevant data
            resp = [
                {
                    'application/workflow': {
                        'workflow.dynamic/handlers': [
                            {
                                'userid': 'somehandler'
                            }
                        ]
                    },
                    "application/id": 3,
                    'application/applicant': {
                        'userid': 'someapplicant'
                    },
                    "application/resources": [
                        {
                            "catalogue-item/title": {
                                "en": "Removal test"
                            },
                            "resource/ext-id": "some:pref:id",
                            "catalogue-item/id": 5
                        }
                    ],
                    "application/state": 'application.state/draft'
                },
                {
                    'application/workflow': {
                        'workflow.dynamic/handlers': [
                            {
                                'userid': 'someid'
                            }
                        ]
                    },
                    "application/id": 2,
                    'application/applicant': {
                        'userid': 'someotherapplicant'
                    },
                    "application/resources": [
                        {
                            "catalogue-item/title": {
                                "en": "Removal test"
                            },
                            "resource/ext-id": "some:pref:id",
                            "catalogue-item/id": 5
                        }
                    ],
                    "application/state": 'application.state/approved'
                },
                {
                    'application/workflow': {
                        'workflow.dynamic/handlers': [
                            {
                                'userid': 'remsuid'
                            }
                        ]
                    },
                    "application/id": 1,
                    'application/applicant': {
                        'userid': 'someapplicant'
                    },
                    "application/resources": [
                        {
                            "catalogue-item/title": {
                                "en": "Removal test"
                            },
                            "resource/ext-id": 'Same:title:with:different:catalogue:item',
                            "catalogue-item/id": 18
                        }
                    ],
                    "application/state": 'application.state/draft'
                }
            ]

        responses.add(
            responses.GET,
            f"{django_settings.REMS['BASE_URL']}/{entity}s",
            json=resp,
            status=200
        )

    def _mock_rems_access_return_403(self, method, entity, action=''):
        """
        Works also for GET method since failure responses from rems are identical for write and read operations
        """
        req_type = responses.POST if method == 'POST' else responses.PUT if method == 'PUT' else responses.GET

        responses.replace(
            req_type,
            f"{django_settings.REMS['BASE_URL']}/{entity}s/{action}",
            status=403 # anything else than 200 is a fail
        )

    def _mock_rems_access_return_error(self, method, entity, action=''):
        """
        operation status is defined in the body so 200 response can also be failure.
        """
        req_type = responses.POST if method == 'POST' else responses.PUT if method == 'PUT' else responses.GET

        errors = [
            {
                "type": "some kind of identifier of this error",
                "somedetail": "entity identifier the error is conserning"
            }
        ]

        responses.replace(
            req_type,
            f"{django_settings.REMS['BASE_URL']}/{entity}s/{action}",
            json={"success": False, "errors": errors},
            status=200
        )

    def _mock_rems_access_crashes(self, method, entity, action=''):
        """
        Crash happens for example if there is a network error. Can be used for GET also
        """
        req_type = responses.POST if method == 'POST' else responses.PUT if method == 'PUT' else responses.GET

        responses.replace(
            req_type,
            f"{django_settings.REMS['BASE_URL']}/{entity}s/{action}",
            body=Exception('REMS_service should catch this one also')
        )

    def _create_new_rems_dataset(self):
        """
        Modifies catalog record to be REMS managed and post it to Metax
        """
        self.cr_test_data['research_dataset']['access_rights'] = self.permit_rights
        self.cr_test_data['data_catalog'] = IDA_CATALOG
        self.cr_test_data['access_granter'] = self._get_access_granter()

        response = self.client.post('/rest/v2/datasets?include_user_metadata', self.cr_test_data, format="json")

        return response

    @responses.activate
    def test_creating_permit_dataset_creates_catalogue_item_service_succeeds(self):
        """
        Tests that catalogue item in REMS is created correctly on permit dataset creation
        """
        response = self._create_new_rems_dataset()
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)
        self.assertTrue(response.data.get('rems_identifier') is not None, 'rems_identifier should be present')
        self.assertTrue(response.data.get('access_granter') is not None, 'access_granter should be present')

    @responses.activate
    def test_creating_permit_dataset_creates_catalogue_item_service_fails_1(self):
        """
        Test unsuccessful rems access
        """
        self._mock_rems_access_return_403('POST', 'workflow', 'create')

        response = self._create_new_rems_dataset()
        self.assertEqual(response.status_code, status.HTTP_503_SERVICE_UNAVAILABLE, response.data)
        self.assertTrue('failed to publish updates' in response.data['detail'][0], response.data)

    @responses.activate
    def test_creating_permit_dataset_creates_catalogue_item_service_fails_2(self):
        """
        Test unsuccessful rems access
        """
        self._mock_rems_access_return_error('POST', 'catalogue-item', 'create')

        response = self._create_new_rems_dataset()
        self.assertEqual(response.status_code, status.HTTP_503_SERVICE_UNAVAILABLE, response.data)

    @responses.activate
    def test_creating_permit_dataset_creates_catalogue_item_service_fails_3(self):
        """
        Test unsuccessful rems access
        """
        self._mock_rems_access_crashes('POST', 'resource', 'create')

        response = self._create_new_rems_dataset()
        self.assertEqual(response.status_code, status.HTTP_503_SERVICE_UNAVAILABLE, response.data)
        self.assertTrue('failed to publish updates' in response.data['detail'][0], response.data)

    @responses.activate
    def test_changing_dataset_to_permit_creates_new_catalogue_item_succeeds(self):
        """
        Test that changing access type to permit invokes the REMS update
        """

        # create dataset without rems managed access
        self.cr_test_data['research_dataset']['access_rights'] = self.open_rights
        self.cr_test_data['data_catalog'] = IDA_CATALOG

        response = self.client.post('/rest/v2/datasets', self.cr_test_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)

        # change to rems managed
        cr = response.data
        cr['research_dataset']['access_rights'] = self.permit_rights
        cr['access_granter'] = self._get_access_granter()

        response = self.client.put(f'/rest/v2/datasets/{cr["id"]}', cr, format="json")
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        self.assertTrue(response.data.get('rems_identifier') is not None, 'rems_identifier should be present')
        self.assertTrue(response.data.get('access_granter') is not None, 'access_granter should be present')

    @responses.activate
    def test_changing_dataset_to_permit_creates_new_catalogue_item_fails(self):
        """
        Test error handling on metax update operation
        """
        self._mock_rems_access_return_error('POST', 'user', 'create')

        # create dataset without rems managed access
        self.cr_test_data['research_dataset']['access_rights'] = self.open_rights
        self.cr_test_data['data_catalog'] = IDA_CATALOG

        response = self.client.post('/rest/v2/datasets', self.cr_test_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)

        # change to rems managed
        cr = response.data
        cr['research_dataset']['access_rights'] = self.permit_rights
        cr['access_granter'] = self._get_access_granter()

        response = self.client.put(f'/rest/v2/datasets/{cr["id"]}', cr, format="json")
        self.assertEqual(response.status_code, status.HTTP_503_SERVICE_UNAVAILABLE, response.data)

    @responses.activate
    def test_changing_access_type_to_other_closes_rems_entities_succeeds(self):
        response = self._create_new_rems_dataset()
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)

        cr = response.data
        cr['research_dataset']['access_rights'] = self.open_rights

        response = self.client.put(f'/rest/v2/datasets/{cr["id"]}', cr, format="json")
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)

    @responses.activate
    def test_changing_access_type_to_other_closes_rems_entities_fails(self):
        response = self._create_new_rems_dataset()
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)

        self._mock_rems_access_return_error('POST', 'application', 'close')

        cr = response.data
        cr['research_dataset']['access_rights'] = self.open_rights

        response = self.client.put(f'/rest/v2/datasets/{cr["id"]}', cr, format="json")
        self.assertEqual(response.status_code, status.HTTP_503_SERVICE_UNAVAILABLE, response.data)

    @responses.activate
    def test_changing_dataset_license_updates_rems(self):
        """
        Create REMS dataset and change it's license. Ensure that
        request is successful and that dataset's rems_identifier is changed.
        """
        response = self._create_new_rems_dataset()
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)

        cr_before = response.data

        rems_id_before = cr_before['rems_identifier']
        cr_before['research_dataset']['access_rights']['license'] = [
            {
                "title": self.other_license['label'],
                "identifier": self.other_license['uri']
            }
        ]

        response = self.client.put(f'/rest/v2/datasets/{cr_before["id"]}', cr_before, format="json")
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)

        cr_after = response.data
        self.assertNotEqual(rems_id_before, cr_after['rems_identifier'], 'REMS identifier should have been changed')

    @responses.activate
    def test_changing_license_dont_allow_access_granter_changes(self):
        """
        Create REMS dataset and change it's license. Ensure that
        request is successful and that dataset's access_granter is not changed.
        """
        response = self._create_new_rems_dataset()
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)

        cr_before = response.data

        cr_before['access_granter']['userid'] = 'newid'
        cr_before['research_dataset']['access_rights']['license'] = [
            {
                "identifier": self.other_license['uri']
            }
        ]

        response = self.client.put(f'/rest/v2/datasets/{cr_before["id"]}', cr_before, format="json")
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)

        cr_after = response.data
        self.assertNotEqual('newid', cr_after['access_granter']['userid'], 'userid should not have been changed')

    @responses.activate
    def test_deleting_license_updates_rems(self):
        """
        Create REMS dataset and delete it's license. Ensure that rems_identifier is removed and no failures occur.
        """
        response = self._create_new_rems_dataset()
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)

        cr_before = response.data

        cr_before['research_dataset']['access_rights'].pop('license')

        response = self.client.put(f'/rest/v2/datasets/{cr_before["id"]}', cr_before, format="json")
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)

        cr_after = response.data
        self.assertTrue(cr_after.get('rems_identifier') is None, 'REMS identifier should have been deleted')
        self.assertTrue(cr_after.get('access_granter') is None, 'access_granter should have been deleted')

    @responses.activate
    def test_creating_permit_dataset_creates_catalogue_item_end_user(self):
        """
        Tests that catalogue item in REMS is created correctly on permit dataset creation.
        User information is fetch'd from token.
        """
        self._set_http_authorization('owner')

        # modify catalog record
        self.cr_test_data['user_created']                       = self.token['CSCUserName']
        self.cr_test_data['metadata_provider_user']             = self.token['CSCUserName']
        self.cr_test_data['metadata_provider_org']              = self.token['schacHomeOrganization']
        self.cr_test_data['metadata_owner_org']                 = self.token['schacHomeOrganization']
        self.cr_test_data['research_dataset']['access_rights']  = self.permit_rights
        self.cr_test_data['data_catalog']                       = IDA_CATALOG

        # end user doesn't have permissions to the files and they are also not needed in this test
        del self.cr_test_data['research_dataset']['files']

        response = self.client.post('/rest/v2/datasets', self.cr_test_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)

    @responses.activate
    def test_deleting_permit_dataset_removes_catalogue_item_succeeds(self):
        response = self._create_new_rems_dataset()
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)

        cr_id = response.data['id']

        # delete dataset
        response = self.client.delete(f'/rest/v2/datasets/{cr_id}')
        self.assertEqual(response.status_code, status.HTTP_204_NO_CONTENT, response.data)

        cr = self.client.get(f'/rest/v2/datasets/{cr_id}?removed').data
        self.assertTrue(cr.get('rems_identifier') is None, 'rems_identifier should not be present')
        self.assertTrue(cr.get('access_granter') is None, 'access_granter should not be present')

    @responses.activate
    def test_deleting_permit_dataset_removes_catalogue_item_fails(self):
        response = self._create_new_rems_dataset()
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)

        # delete dataset
        self._mock_rems_access_return_error('PUT', 'catalogue-item', 'enabled')

        response = self.client.delete(f'/rest/v2/datasets/{response.data["id"]}')
        self.assertEqual(response.status_code, status.HTTP_503_SERVICE_UNAVAILABLE, response.data)

    @responses.activate
    def test_deprecating_permit_dataset_removes_catalogue_item_succeeds(self):
        response = self._create_new_rems_dataset()
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)

        cr_before = response.data
        # deprecate dataset
        response = self.client.delete(f"/rest/v2/files/{cr_before['research_dataset']['files'][0]['identifier']}")
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)

        cr_after = self.client.get(f'/rest/v2/datasets/{cr_before["id"]}').data
        self.assertTrue(cr_after.get('rems_identifier') is None, 'rems_identifier should not be present')
        self.assertTrue(cr_after.get('access_granter') is None, 'access_granter should not be present')

    @responses.activate
    def test_deprecating_permit_dataset_removes_catalogue_item_fails(self):
        response = self._create_new_rems_dataset()
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)

        # deprecate dataset
        self._mock_rems_access_crashes('PUT', 'workflow', 'archived')

        response = self.client.delete(f"/rest/v2/files/{response.data['research_dataset']['files'][0]['identifier']}")
        self.assertEqual(response.status_code, status.HTTP_503_SERVICE_UNAVAILABLE, response.data)
        self.assertTrue('failed to publish' in response.data['detail'][0], response.data)

    def test_missing_access_granter(self):
        """
        Access_granter field is required when dataset is made REMS managed and
        user is service.
        """

        # test on create
        self.cr_test_data['research_dataset']['access_rights'] = self.permit_rights
        self.cr_test_data['data_catalog'] = IDA_CATALOG

        response = self.client.post('/rest/v2/datasets', self.cr_test_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST, response.data)
        self.assertTrue('access_granter' in response.data['detail'][0], response.data)

        # test on update
        self.cr_test_data['research_dataset']['access_rights'] = self.open_rights
        response = self.client.post('/rest/v2/datasets', self.cr_test_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)

        cr = response.data
        cr['research_dataset']['access_rights'] = self.permit_rights
        response = self.client.put(f'/rest/v2/datasets/{cr["id"]}', cr, format="json")
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST, response.data)
        self.assertTrue('access_granter' in response.data['detail'][0], response.data)

    def test_bad_access_granter_parameter(self):
        """
        Access_granter values must be strings
        """
        self.cr_test_data['research_dataset']['access_rights'] = self.permit_rights
        self.cr_test_data['data_catalog'] = IDA_CATALOG
        self.cr_test_data['access_granter'] = self._get_access_granter(malformed=True)

        response = self.client.post(
            '/rest/v2/datasets',
            self.cr_test_data,
            format="json"
        )
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST, response.data)
        self.assertTrue('must be string' in response.data['detail'][0], response.data)

    def test_missing_license_in_dataset(self):
        """
        License is required when dataset is REMS managed
        """
        self.cr_test_data['research_dataset']['access_rights'] = deepcopy(self.permit_rights)
        del self.cr_test_data['research_dataset']['access_rights']['license']
        self.cr_test_data['data_catalog'] = IDA_CATALOG

        response = self.client.post(
            f'/rest/v2/datasets?access_granter={self._get_access_granter()}',
            self.cr_test_data,
            format="json"
        )
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST, response.data)
        self.assertTrue('must define license' in response.data['detail'][0], response.data)

    @responses.activate
    def test_only_return_rems_info_to_privileged(self):
        self._set_http_authorization('service')

        response = self._create_new_rems_dataset()
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)
        self.assertTrue(response.data.get('rems_identifier') is not None, 'rems_identifier should be returned to owner')
        self.assertTrue(response.data.get('access_granter') is not None, 'access_granter should be returned to owner')

        self._set_http_authorization('no')
        response = self.client.get(f'/rest/v2/datasets/{response.data["id"]}')
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        self.assertTrue(response.data.get('rems_identifier') is None, 'rems_identifier should not be returned to Anon')
        self.assertTrue(response.data.get('access_granter') is None, 'access_granter should not be returned to Anon')

    @responses.activate
    def test_rems_info_cannot_be_changed(self):
        response = self._create_new_rems_dataset()
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)

        cr = response.data

        cr['rems_identifier'] = 'some:new:identifier'
        cr['access_granter']['name'] = 'New Name'

        response = self.client.put(f'/rest/v2/datasets/{cr["id"]}', cr, format="json")
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        self.assertNotEqual(response.data['rems_identifier'], 'some:new:identifier', 'rems_id should not be changed')
        self.assertNotEqual(response.data['access_granter'], 'New Name', 'access_granter should not be changed')
