# This file is part of the Metax API service
#
# Copyright 2017-2018 Ministry of Education and Culture, Finland
#
# :author: CSC - IT Center for Science Ltd., Espoo Finland <servicedesk@csc.fi>
# :license: MIT

from django.utils import timezone
from pytz import timezone as tz
from rest_framework import status
from rest_framework.test import APITestCase

from metax_api.utils import parse_timestamp_string_to_tz_aware_datetime
from metax_api.tests.api.rest.base.views.datasets.write import CatalogRecordApiWriteCommon
from metax_api.tests.utils import TestClassUtils

FORBIDDEN = status.HTTP_403_FORBIDDEN


class ApiAuthnzTestV1(APITestCase, TestClassUtils):
    """
    Test use of HTTP Authorization header for authnz for POST, PUT, PATCH and
    DELETE requests. API caller identification is performed as the first step
    of each request, so any API is as good as any, even with invalid payload,
    for testing purposes.
    """

    def setUp(self):
        self._use_http_authorization()

    #
    #
    #
    # read requests
    #
    #
    #

    def test_authorization_not_required(self):
        """
        GET operations are allowed for all.
        """

        # reset credentials
        self.client.credentials()

        response = self.client.get('/rest/datasets')
        self.assertNotEqual(response.status_code, FORBIDDEN)
        response = self.client.get('/rest/datasets/1')
        self.assertNotEqual(response.status_code, FORBIDDEN)

    def test_optional_authorizatiaon_during_get(self):
        """
        If auth headers are passed during GET, the user should then be identified by them.
        """
        response = self.client.get('/rest/datasets')
        self.assertNotEqual(response.status_code, FORBIDDEN)
        response = self.client.get('/rest/datasets/1')
        self.assertNotEqual(response.status_code, FORBIDDEN)

    def test_optional_authorizatiaon_during_get_fails(self):
        """
        If auth headers are passed during GET, the user should then be identified by them.
        And if credentials are wrong, then access is forbidden
        """
        self._use_http_authorization(username='nope', password='wrong')
        response = self.client.get('/rest/datasets')
        self.assertEqual(response.status_code, FORBIDDEN)

    #
    #
    #
    # write requests
    #
    #
    #

    def test_authorization_ok(self):
        """
        All write operations require proper authnz using HTTP Authorization header.
        The following requests are invalid by their content, but none should fail to
        the very first step of identifying the api caller.
        """
        response = self.client.post('/rest/datasets')
        self.assertNotEqual(response.status_code, FORBIDDEN)
        response = self.client.put('/rest/datasets')
        self.assertNotEqual(response.status_code, FORBIDDEN)
        response = self.client.patch('/rest/datasets')
        self.assertNotEqual(response.status_code, FORBIDDEN)
        response = self.client.delete('/rest/datasets')
        self.assertNotEqual(response.status_code, FORBIDDEN)

    def test_unknown_user(self):
        """
        Unknown user credentials, every request should fail to the very first step
        of identifying the api caller.
        """
        self._use_http_authorization(username='other', password='pw')
        response = self.client.post('/rest/datasets')
        self.assertEqual(response.status_code, FORBIDDEN)
        response = self.client.put('/rest/datasets')
        self.assertEqual(response.status_code, FORBIDDEN)
        response = self.client.patch('/rest/datasets')
        self.assertEqual(response.status_code, FORBIDDEN)
        response = self.client.delete('/rest/datasets')
        self.assertEqual(response.status_code, FORBIDDEN)

    def test_wrong_password(self):
        self._use_http_authorization(password='wrongpassword')
        response = self.client.post('/rest/datasets')
        self.assertEqual(response.status_code, FORBIDDEN)
        response = self.client.put('/rest/datasets')
        self.assertEqual(response.status_code, FORBIDDEN)
        response = self.client.patch('/rest/datasets')
        self.assertEqual(response.status_code, FORBIDDEN)
        response = self.client.delete('/rest/datasets')
        self.assertEqual(response.status_code, FORBIDDEN)

    #
    #
    #
    # invalid auth header
    #
    # All errors during the auth header processing should return 403, without
    # giving more specific errors.
    #
    #
    #

    def test_malformed_auth_header(self):
        self._use_http_authorization(header_value='Basic hubbabubba')
        response = self.client.post('/rest/datasets')
        self.assertEqual(response.status_code, FORBIDDEN)

    def test_invalid_auth_method(self):
        self._use_http_authorization(header_value='NotSupported hubbabubba')
        response = self.client.post('/rest/datasets')
        self.assertEqual(response.status_code, FORBIDDEN)


class ApiModifyResponseTestV1(CatalogRecordApiWriteCommon):

    def test_catalog_record_get_last_modified_header(self):
        response = self.client.get('/rest/datasets/1')
        self._validate_response(response)

    def test_catalog_record_post_last_modified_header(self):
        response = self.client.post('/rest/datasets', self.cr_test_data, format="json")
        self._validate_response(response)

    def test_catalog_record_put_last_modified_header(self):
        cr = self.client.get('/rest/datasets/1', format="json").data
        cr['preservation_description'] = 'what'
        response = self.client.put('/rest/datasets/1', cr, format="json")
        self._validate_response(response)

    def test_catalog_record_patch_last_modified_header(self):
        cr = self.client.get('/rest/datasets/1', format="json").data
        cr['preservation_description'] = 'what'
        response = self.client.patch('/rest/datasets/1', cr, format="json")
        self._validate_response(response)

    def test_catalog_record_delete_does_not_contain_last_modified_header(self):
        response = self.client.delete('/rest/datasets/1')
        self.assertFalse(response.has_header('Last-Modified'))

    def test_catalog_record_bulk_create_get_last_modified_header(self):
        response = self.client.post('/rest/datasets', [self.cr_test_data, self.cr_test_data], format="json")
        self._validate_response(response)

    def _validate_response(self, response):
        data = response.data.get('success', response.data)
        obj = data[0].get('object', None) if isinstance(data, list) else data
        self.assertIsNotNone(obj)

        expected_modified_str = obj['date_modified'] if 'date_modified' in obj else obj.get('date_created', None)
        expected_modified = timezone.localtime(parse_timestamp_string_to_tz_aware_datetime(expected_modified_str),
                                               timezone=tz('GMT'))

        self.assertTrue(response.has_header('Last-Modified'))
        actual_modified = timezone.localtime(parse_timestamp_string_to_tz_aware_datetime(response.get('Last-Modified')),
                                             timezone=tz('GMT'))

        self.assertEqual(expected_modified, actual_modified)


class ApiStreamHttpResponse(CatalogRecordApiWriteCommon):

    def test_no_streaming_with_paging(self):
        response = self.client.get('/rest/datasets?stream=true')
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.streaming, False)

    def test_streaming_json(self):
        response = self.client.get('/rest/datasets?pagination=false&stream=true')
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.streaming, True)

        response = self.client.get('/rest/files?pagination=false&stream=true')
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.streaming, True)
