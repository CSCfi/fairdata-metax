# This file is part of the Metax API service
#
# Copyright 2017-2018 Ministry of Education and Culture, Finland
#
# :author: CSC - IT Center for Science Ltd., Espoo Finland <servicedesk@csc.fi>
# :license: MIT

import logging

from rest_framework import status

from metax_api.tests.api.rest.base.views.datasets.write import CatalogRecordApiWriteCommon

_logger = logging.getLogger(__name__)


class ApiServiceAccessAuthorization(CatalogRecordApiWriteCommon):

    """
    Test API-wide service access restriction rules
    """

    def setUp(self):
        super().setUp()
        # test user api_auth_user has some custom api permissions set in settings.py
        self._use_http_authorization(username="api_auth_user")

    def test_read_for_world_ok(self):
        """
        Reading datasets api should be permitted even without any authorization.
        """
        self.client._credentials = {}
        response = self.client.get("/rest/v2/datasets/1")
        self.assertEqual(response.status_code, status.HTTP_200_OK)
