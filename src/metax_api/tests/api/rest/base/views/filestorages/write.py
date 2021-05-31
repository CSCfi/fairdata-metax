# This file is part of the Metax API service
#
# Copyright 2017-2018 Ministry of Education and Culture, Finland
#
# :author: CSC - IT Center for Science Ltd., Espoo Finland <servicedesk@csc.fi>
# :license: MIT

from django.core.management import call_command
from rest_framework import status
from rest_framework.test import APITestCase

from metax_api.models import FileStorage
from metax_api.tests.utils import TestClassUtils, test_data_file_path


class FileStorageApiWriteCommon(APITestCase, TestClassUtils):
    @classmethod
    def setUpClass(cls):
        """
        Loaded only once for test cases inside this class.
        """
        call_command("loaddata", test_data_file_path, verbosity=0)
        super(FileStorageApiWriteCommon, cls).setUpClass()

    def setUp(self):
        self.new_test_data = self._get_object_from_test_data("filestorage")
        self.new_test_data.pop("id")
        self.new_test_data["file_storage_json"]["identifier"] = "new-file-storage"
        self._use_http_authorization()


class FileStorageApiWriteBasicTests(FileStorageApiWriteCommon):
    def test_create(self):
        response = self.client.post("/rest/filestorages", self.new_test_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED)

    def test_create_identifier_already_exists(self):
        response = self.client.post("/rest/filestorages", self.new_test_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED)
        response = self.client.post("/rest/filestorages", self.new_test_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual(
            "already exists" in response.data["file_storage_json"]["identifier"][0],
            True,
            response.data,
        )

    def test_delete(self):
        response = self.client.delete("/rest/filestorages/1")
        self.assertEqual(response.status_code, status.HTTP_204_NO_CONTENT)
        fs = FileStorage.objects_unfiltered.get(pk=1)
        self.assertEqual(fs.removed, True, "should be deleted")
        self.assertEqual(fs.date_removed, fs.date_modified)
