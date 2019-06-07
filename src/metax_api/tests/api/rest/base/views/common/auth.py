# This file is part of the Metax API service
#
# Copyright 2017-2018 Ministry of Education and Culture, Finland
#
# :author: CSC - IT Center for Science Ltd., Espoo Finland <servicedesk@csc.fi>
# :license: MIT

import os
import json
import responses
from rest_framework import status
from django.conf import settings

from metax_api.tests.api.rest.base.views.datasets.write import CatalogRecordApiWriteCommon
from metax_api.tests.utils import get_test_oidc_token


class ApiServiceAccessAuthorization(CatalogRecordApiWriteCommon):

    """
    Test API-wide service access restriction rules
    """

    def setUp(self):
        super().setUp()
        # test user api_auth_user has some custom api permissions set in settings.py
        self._use_http_authorization(username='api_auth_user')

    def test_read_access_ok(self):
        """
        User api_auth_user should have read access to files api.
        """
        response = self.client.get('/rest/files/1')
        self.assertEqual(response.status_code, status.HTTP_200_OK)

    def test_create_access_ok(self):
        """
        User api_auth_user should have create access to datasets api.
        """
        response = self.client.get('/rest/datasets/1')
        cr = response.data
        cr['contract'] = 1
        response = self.client.put('/rest/datasets/1', cr, format='json')
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)

    def test_update_access_error(self):
        """
        User api_auth_user should not have update access to files api.
        """
        response = self.client.get('/rest/files/1')
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        file = response.data
        file['file_format'] = 'text/html'

        response = self.client.put('/rest/files/1', file, format='json')
        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN)

    def test_delete_access_error(self):
        """
        User api_auth_user should not have delete access to files api.
        """
        response = self.client.delete('/rest/files/1')
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

    def tearDown(self):
        super().tearDown()
        if os.path.exists(settings.ADDITIONAL_USER_PROJECTS_PATH):
            os.remove(settings.ADDITIONAL_USER_PROJECTS_PATH)

    @responses.activate
    def test_valid_token(self):
        """
        Test api authentication with a valid token. Validation is mocked, ensures code following
        valid authentication works. Should return successfully.
        """
        self._mock_token_validation_succeeds()
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

        In all cases, metax code execution stops at the middleware where authentication failed.print(user_projects)
        """
        self._mock_token_validation_fails()
        response = self.client.get('/rest/datasets/1')
        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN)

    @responses.activate
    def test_end_user_read_access(self):
        """
        Ensure end users are recognized in api read access permissions.
        """
        self._mock_token_validation_succeeds()

        # datasets-api should be allowed for end users
        response = self.client.get('/rest/datasets/1')
        self.assertEqual(response.status_code, status.HTTP_200_OK)

        # contracts-api should not be allowed for end users
        response = self.client.get('/rest/contracts/1')
        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN)

    @responses.activate
    def test_end_user_create_access_error(self):
        """
        Ensure end users are recognized in api create access permissions.
        """
        self._mock_token_validation_succeeds()
        # end users should not have create access to files api.
        response = self.client.post('/rest/files', {}, format='json')
        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN, response.data)

    @responses.activate
    def test_additional_projects_success(self):
        """
        Ensures user's file projects are also fetched from local file.
        """
        self._use_http_authorization(method='bearer', token=get_test_oidc_token(new_proxy=True))
        self._mock_token_validation_succeeds()
        testdata = { "testuser": ["project_x"] }
        with open(settings.ADDITIONAL_USER_PROJECTS_PATH, 'w+') as testfile:
            testfile.write(json.dumps(testdata, indent=4))
            os.chmod(settings.ADDITIONAL_USER_PROJECTS_PATH, 0o400)

        response = self.client.get('/rest/files?project_identifier=project_x', {}, format='json')
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)

    @responses.activate
    def test_additional_projects_no_file(self):
        """
        Projects are fetched also from token when local file is not available.
        """
        self._use_http_authorization(method='bearer', token=get_test_oidc_token(new_proxy=True))
        self._mock_token_validation_succeeds()
        response = self.client.get('/rest/files?project_identifier=2001036', {}, format='json')
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)

    @responses.activate
    def test_additional_projects_bad_file(self):
        """
        Returns forbidden since values on local file are not list of strings.
        """
        self._use_http_authorization(method='bearer', token=get_test_oidc_token(new_proxy=True))
        self._mock_token_validation_succeeds()
        testdata = { "testuser": "project_x" }
        with open(settings.ADDITIONAL_USER_PROJECTS_PATH, 'w+') as testfile:
            testfile.write(json.dumps(testdata, indent=4))
            os.chmod(settings.ADDITIONAL_USER_PROJECTS_PATH, 0o400)

        response = self.client.get('/rest/files?project_identifier=project_x', {}, format='json')
        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN, response.data)

    def test_removing_bearer_from_allowed_auth_methods_disables_oidc(self):
        pass
        # ALLOWED_AUTH_METHODS
