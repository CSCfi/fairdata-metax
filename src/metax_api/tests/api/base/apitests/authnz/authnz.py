from rest_framework import status
from rest_framework.test import APITestCase

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
        self._use_http_authorization(header_value=b'Basic hubbabubba')
        response = self.client.post('/rest/datasets')
        self.assertEqual(response.status_code, FORBIDDEN)

    def test_invalid_auth_method(self):
        self._use_http_authorization(header_value=b'NotSupported hubbabubba')
        response = self.client.post('/rest/datasets')
        self.assertEqual(response.status_code, FORBIDDEN)
