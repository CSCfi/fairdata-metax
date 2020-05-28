# This file is part of the Metax API service
#
# Copyright 2017-2018 Ministry of Education and Culture, Finland
#
# :author: CSC - IT Center for Science Ltd., Espoo Finland <servicedesk@csc.fi>
# :license: MIT

from rest_framework import status
from rest_framework.test import APITestCase

from metax_api.tests.utils import TestClassUtils


class CommonRPCTests(APITestCase, TestClassUtils):

    def test_list_valid_methods(self):
        """
        When an invalid (or mistyped) method name is attempted, the api should list valid methods
        names for that RPC endpoint.
        """
        response = self.client.get('/rpc/v2/datasets/nonexisting')
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual('methods are: ' in response.data['detail'][0], True, response.content)
