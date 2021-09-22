# This file is part of the Metax API service
#
# Copyright 2017-2018 Ministry of Education and Culture, Finland
#
# :author: CSC - IT Center for Science Ltd., Espoo Finland <servicedesk@csc.fi>
# :license: MIT

from django.core.management import call_command
from rest_framework import status
from rest_framework.test import APITestCase

from metax_api.tests.utils import TestClassUtils, test_data_file_path


class DataCatalogApiWriteCommon(APITestCase, TestClassUtils):
    @classmethod
    def setUpClass(cls):
        """
        Loaded only once for test cases inside this class.
        """
        call_command("loaddata", test_data_file_path, verbosity=0)
        super(DataCatalogApiWriteCommon, cls).setUpClass()

    def setUp(self):
        self.new_test_data = self._get_object_from_test_data("datacatalog")
        self.new_test_data.pop("id")
        self.new_test_data["catalog_json"]["identifier"] = "new-data-catalog"
        self._use_http_authorization()


class DataCatalogApiWriteBasicTests(DataCatalogApiWriteCommon):

    def test_research_dataset_schema_missing_ok(self):
        self.new_test_data["catalog_json"].pop("research_dataset_schema", None)
        response = self.client.post("/rest/v2/datacatalogs", self.new_test_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED)

    def test_research_dataset_schema_not_found_error(self):
        self.new_test_data["catalog_json"]["research_dataset_schema"] = "notfound"
        response = self.client.post("/rest/v2/datacatalogs", self.new_test_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST, response.data)

    def test_disallow_versioning_in_harvested_catalogs(self):
        self.new_test_data["catalog_json"]["dataset_versioning"] = True
        self.new_test_data["catalog_json"]["harvested"] = True
        response = self.client.post("/rest/v2/datacatalogs", self.new_test_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual("versioning" in response.data["detail"][0], True, response.data)