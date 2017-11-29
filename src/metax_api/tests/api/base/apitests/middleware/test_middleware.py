from django.core.management import call_command
from django.utils import timezone
from pytz import timezone as tz
from rest_framework import status
from rest_framework.test import APITestCase

from metax_api.utils import parse_timestamp_string_to_tz_aware_datetime
from metax_api.tests.utils import test_data_file_path, TestClassUtils

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
        self._use_http_authorization(header_value=b'Basic hubbabubba')
        response = self.client.post('/rest/datasets')
        self.assertEqual(response.status_code, FORBIDDEN)

    def test_invalid_auth_method(self):
        self._use_http_authorization(header_value=b'NotSupported hubbabubba')
        response = self.client.post('/rest/datasets')
        self.assertEqual(response.status_code, FORBIDDEN)


class ApiModifyResponseTestV1(APITestCase, TestClassUtils):

    def setUp(self):
        call_command('loaddata', test_data_file_path, verbosity=0)
        catalog_record_from_test_data = self._get_object_from_test_data('catalogrecord')
        self.urn_identifier = catalog_record_from_test_data['research_dataset']['urn_identifier']
        self.preferred_identifier = catalog_record_from_test_data['research_dataset']['preferred_identifier']
        self.new_test_data = self._get_new_test_data()
        self._use_http_authorization()

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

    def test_catalog_record_get_last_modified_header(self):
        response = self.client.get('/rest/datasets/1')
        self._validate_response(response)

    def test_catalog_record_post_last_modified_header(self):
        response = self.client.post('/rest/datasets', self.new_test_data, format="json")
        self._validate_response(response)

    # TODO: Uncomment this once PUT returns the updated object
    # def test_catalog_record_put_last_modified_header(self):
    #     self.new_test_data['research_dataset']['preferred_identifier'] = self.preferred_identifier
    #     response = self.client.put('/rest/datasets/1', self.new_test_data, format="json")
    #     self._validate_response(response)

    def test_catalog_record_patch_last_modified_header(self):
        self.new_test_data['research_dataset']['preferred_identifier'] = self.preferred_identifier
        response = self.client.patch('/rest/datasets/1', self.new_test_data, format="json")
        self._validate_response(response)

    def test_catalog_record_delete_does_not_contain_last_modified_header(self):
        response = self.client.delete('/rest/datasets/1')
        self.assertFalse(response.has_header('Last-Modified'))

    def _validate_response(self, response):
        expected_modified_str = response.data['modified_by_api'] if 'modified_by_api' in response.data \
            else response.data.get('created_by_api', None)

        expected_modified = timezone.localtime(parse_timestamp_string_to_tz_aware_datetime(expected_modified_str),
                                               timezone=tz('GMT'))

        self.assertTrue(response.has_header('Last-Modified'))
        actual_modified = timezone.localtime(parse_timestamp_string_to_tz_aware_datetime(response.get('Last-Modified')),
                                             timezone=tz('GMT'))

        self.assertEqual(expected_modified, actual_modified)
