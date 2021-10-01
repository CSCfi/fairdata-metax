# This file is part of the Metax API service
#
# Copyright 2017-2018 Ministry of Education and Culture, Finland
#
# :author: CSC - IT Center for Science Ltd., Espoo Finland <servicedesk@csc.fi>
# :license: MIT

from django.core.management import call_command
from rest_framework import status
from rest_framework.test import APITestCase

from metax_api.services.redis_cache_service import RedisClient
from metax_api.tests.utils import TestClassUtils, test_data_file_path


class FileApiWriteCommon(APITestCase, TestClassUtils):
    @classmethod
    def setUpClass(cls):
        """
        Loaded only once for test cases inside this class.
        """
        call_command("loaddata", test_data_file_path, verbosity=0)
        super(FileApiWriteCommon, cls).setUpClass()

    def setUp(self):
        """
        Reloaded for every test case
        """
        call_command("loaddata", test_data_file_path, verbosity=0)
        file_from_test_data = self._get_object_from_test_data("file")
        self.identifier = file_from_test_data["identifier"]
        self.pidentifier = file_from_test_data["project_identifier"]
        self.file_name = file_from_test_data["file_name"]

        """
        New data that is sent to the server for POST, PUT, PATCH requests. Modified
        slightly as approriate for different purposes
        """
        self.test_new_data = self._get_new_test_data()
        self.second_test_new_data = self._get_second_new_test_data()
        self._use_http_authorization()

    def _get_new_test_data(self):
        from_test_data = self._get_object_from_test_data("file", requested_index=0)
        from_test_data.update(
            {
                "checksum": {
                    "value": "habeebit",
                    "algorithm": "SHA-256",
                    "checked": "2017-05-23T10:07:22.559656Z",
                },
                "file_name": "file_name_1",
                "file_path": from_test_data["file_path"].replace("/some/path", "/some/other_path"),
                "identifier": "urn:nbn:fi:csc-ida201401200000000001",
                "file_storage": self._get_object_from_test_data("filestorage", requested_index=0),
            }
        )
        from_test_data["file_path"] = from_test_data["file_path"].replace(
            "/Experiment_X/", "/test/path/"
        )
        from_test_data["project_identifier"] = "test_project_identifier"
        del from_test_data["id"]
        return from_test_data

    def _get_second_new_test_data(self):
        from_test_data = self._get_new_test_data()
        from_test_data["identifier"] = "urn:nbn:fi:csc-ida201401200000000002"
        self._change_file_path(from_test_data, "file_name_2")
        return from_test_data

    def _change_file_path(self, file, new_name):
        file["file_path"] = file["file_path"].replace(file["file_name"], new_name)
        file["file_name"] = new_name


class FileApiWriteReferenceDataValidationTests(FileApiWriteCommon):
    @classmethod
    def setUpClass(cls):
        """
        Loaded only once for test cases inside this class.
        """
        call_command("updatereferencedata", verbosity=0)
        super(FileApiWriteReferenceDataValidationTests, cls).setUpClass()

    def setUp(self):
        super().setUp()
        cache = RedisClient()
        ffv_refdata = cache.get("reference_data")["reference_data"]["file_format_version"]

        # File format version entry in reference data that has some output_format_version
        self.ff_with_version = None
        # File format version entry in reference data that has same input_file_format than ff_with_versions but
        # different output_format_version
        self.ff_with_different_version = None
        # File format version entry in reference data which has not output_format_version
        self.ff_without_version = None

        for ffv_obj in ffv_refdata:
            if self.ff_with_different_version is None and self.ff_with_version is not None:
                if ffv_obj["input_file_format"] == self.ff_with_version["input_file_format"]:
                    self.ff_with_different_version = ffv_obj
            if self.ff_with_version is None and ffv_obj["output_format_version"]:
                self.ff_with_version = ffv_obj
            if self.ff_without_version is None and not ffv_obj["output_format_version"]:
                self.ff_without_version = ffv_obj

        self.assertTrue(self.ff_with_version["output_format_version"] != "")
        self.assertTrue(self.ff_with_different_version["output_format_version"] != "")
        self.assertTrue(
            self.ff_with_version["input_file_format"]
            == self.ff_with_different_version["input_file_format"]
        )
        self.assertTrue(
            self.ff_with_version["output_format_version"]
            != self.ff_with_different_version["output_format_version"]
        )
        self.assertTrue(self.ff_without_version["output_format_version"] == "")

    # update tests

    def test_format_version_is_removed(self):
        """
        Empty file format version should be removed
        """
        self.test_new_data["file_characteristics"]["file_format"] = "text/csv"
        self.test_new_data["file_characteristics"]["format_version"] = ""

        response = self.client.post("/rest/v2/files", self.test_new_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)
        self.assertTrue("format_version" not in response.data["file_characteristics"])