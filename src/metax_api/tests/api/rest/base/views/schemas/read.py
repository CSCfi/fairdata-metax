# This file is part of the Metax API service
#
# Copyright 2017-2018 Ministry of Education and Culture, Finland
#
# :author: CSC - IT Center for Science Ltd., Espoo Finland <servicedesk@csc.fi>
# :license: MIT

from rest_framework import status
from rest_framework.test import APITestCase

from metax_api.tests.utils import TestClassUtils


class SchemaApiReadTests(APITestCase, TestClassUtils):
    def test_read_schemas_list(self):
        response = self.client.get("/rest/schemas")
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertTrue(response.data["count"] > 0)

    def test_read_schemas_list_html(self):
        headers = {"HTTP_ACCEPT": "text/html"}
        response = self.client.get("/rest/schemas", **headers)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        # self.assertTrue(response.headers["content-type"][1].find("json") >= 0)

    def test_read_schema_retrieve_existing(self):
        list_response = self.client.get("/rest/schemas")
        self.assertEqual(list_response.status_code, status.HTTP_200_OK)
        self.assertTrue(list_response.data["count"] > 0, "No schemas available")
        response = self.client.get("/rest/schemas/%s" % list_response.data["results"][0])
        self.assertEqual(response.status_code, status.HTTP_200_OK)

    def test_read_schema_retrieve_datacite(self):
        response = self.client.get("/rest/schemas/datacite_4.1")
        self.assertEqual(response.status_code, status.HTTP_200_OK)

    def test_read_schema_not_exists(self):
        response = self.client.get("/rest/schemas/thisshouldnotexist")
        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND)
