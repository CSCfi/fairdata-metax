# This file is part of the Metax API service
#
# Copyright 2017-2018 Ministry of Education and Culture, Finland
#
# :author: CSC - IT Center for Science Ltd., Espoo Finland <servicedesk@csc.fi>
# :license: MIT

from django.core.management import call_command
from rest_framework import status
from rest_framework.test import APITestCase

from metax_api.tests.utils import test_data_file_path, TestClassUtils


class DataCatalogApiReadBasicTests(APITestCase, TestClassUtils):
    @classmethod
    def setUpClass(cls):
        """
        Loaded only once for test cases inside this class.
        """
        call_command('loaddata', test_data_file_path, verbosity=0)
        super(DataCatalogApiReadBasicTests, cls).setUpClass()

    def setUp(self):
        data_catalog_from_test_data = self._get_object_from_test_data('datacatalog', requested_index=0)
        self._use_http_authorization()
        self.pk = data_catalog_from_test_data['id']
        self.identifier = data_catalog_from_test_data['catalog_json']['identifier']

    def test_basic_get(self):
        response = self.client.get('/rest/datacatalogs/%s' % self.identifier)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
