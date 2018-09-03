# This file is part of the Metax API service
#
# Copyright 2017-2018 Ministry of Education and Culture, Finland
#
# :author: CSC - IT Center for Science Ltd., Espoo Finland <servicedesk@csc.fi>
# :license: MIT

import responses
from django.conf import settings as django_settings
from rest_framework import status

from metax_api.tests.api.rest.base.views.datasets.write import CatalogRecordApiWriteCommon
from metax_api.tests.utils import get_test_oidc_token


VALIDATE_TOKEN_URL = django_settings.VALIDATE_TOKEN_URL


class ApiServiceAccessAuthorization(CatalogRecordApiWriteCommon):

    """
    Test API-wide service access restriction rules
    """

    def setUp(self):
        super().setUp()
        # test user api_auth_user has some custom api permissions set in settings.py
        self._use_http_authorization(username='api_auth_user')

    def test_write_access_ok(self):
        """
        User api_auth_user should have write and read access to datasets api.
        """
        response = self.client.get('/rest/datasets/1')
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        cr = response.data
        cr['contract'] = 1

        response = self.client.put('/rest/datasets/1', cr, format='json')
        self.assertEqual(response.status_code, status.HTTP_200_OK)

    def test_write_access_error(self):
        """
        User api_auth_user should have read access to files api, but not write.
        """
        response = self.client.get('/rest/files/1')
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        file = response.data
        file['file_format'] = 'text/html'

        response = self.client.put('/rest/files/1', file, format='json')
        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN)

    def test_read_for_world_ok(self):
        """
        Reading datasets api should be permitted even without any authorization.
        """
        self.client._credentials = {}
        response = self.client.get('/rest/datasets/1')
        self.assertEqual(response.status_code, status.HTTP_200_OK)


class ApiEndUserAccessAuthorization(CatalogRecordApiWriteCommon):

    """
    Test End User authentication and authorization.

    Since the End User authnz utilizes OIDC, and there is no legit local OIDC OP,
    responses from /secure/validate_token are mocked. The endpoint only returns
    200 OK for successful token validation, or 403 for failed validation.
    """

    def setUp(self):
        super().setUp()
        self._use_http_authorization(method='bearer', token=get_test_oidc_token())

    def mock_token_validation_succeeds(self):
        responses.add(responses.GET, VALIDATE_TOKEN_URL, status=200)

    def mock_token_validation_fails(self):
        responses.add(responses.GET, VALIDATE_TOKEN_URL, status=403)

    @responses.activate
    def test_valid_token(self):
        """
        Test api authentication with a valid token. Validation is mocked, ensures code following
        valid authentication works. Should return successfully.
        """
        self.mock_token_validation_succeeds()
        response = self.client.get('/rest/datasets/1')
        self.assertEqual(response.status_code, status.HTTP_200_OK)

    @responses.activate
    def test_invalid_token(self):
        """
        Test api authentication with an invalid token. Validation is mocked, ensures code following
        failed authentication works. Should return 403.

        Note: This test basically takes care of a lot of test cases regarding token validation. Since
        token validation is executed by apache, it is rather opaque for testing purposes here, returning
        only 403 on failures (or possibly 401 on some cases).

        Possible reasons for failures include:
        - expired token
        - invalid signature
        - malformed token
        - bad claims (such as intended audience)

        In all cases, metax code execution stops at the middleware where authentication failed.
        """
        self.mock_token_validation_fails()
        response = self.client.get('/rest/datasets/1')
        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN)

    @responses.activate
    def test_end_user_has_api_access(self):
        """
        Not all apis are open for end users. Ensure end users are recognized in api access permissions.
        """
        self.mock_token_validation_succeeds()

        # datasets-api should be allowed for end users
        response = self.client.get('/rest/datasets/1')
        self.assertEqual(response.status_code, status.HTTP_200_OK)

        # contracts-api should not be allowed for end users
        response = self.client.get('/rest/contracts/1')
        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN)

    def test_removing_bearer_from_allowed_auth_methods_disables_oidc(self):
        pass
        # ALLOWED_AUTH_METHODS
