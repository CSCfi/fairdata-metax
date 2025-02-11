# This file is part of the Metax API service
#
# Copyright 2017-2018 Ministry of Education and Culture, Finland
#
# :author: CSC - IT Center for Science Ltd., Espoo Finland <servicedesk@csc.fi>
# :license: MIT
import responses
import json

from unittest.mock import ANY

from django.core.management import call_command
from django.test import override_settings
from django.utils.dateparse import parse_datetime
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

    def _get_original_test_data(self):
        return self._get_object_from_test_data("file", requested_index=0)

    def _get_new_test_data(self):
        from_test_data = self._get_object_from_test_data("file", requested_index=0)
        from_test_data.update(
            {
                "checksum": {
                    "value": "othervalue",
                    "algorithm": "SHA-256",
                    "checked": "2024-05-23T10:07:22.559656Z",
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


@override_settings(
    METAX_V3={
        "HOST": "metax-test",
        "TOKEN": "test-token",
        "INTEGRATION_ENABLED": True,
        "PROTOCOL": "https",
    }
)
class FileApiWriteSyncToV3Tests(FileApiWriteCommon):

    maxDiff = None  # Show full diff when assert fails

    def mock_responses(self):
        responses.add(
            responses.POST,
            url="https://metax-test/v3/files/from-legacy",
            json={"created": 1, "updated": 0, "unchanged": 0},
        )

    def remove_dates(self, data: dict):
        """Remove datetimes from result comparsion.

        Metax may generate different datetime strings depending on operation
        (e.g. timezone conversion in 'create' and 'bulk create' works different)
        and some dates (creation/modification date) depend on current time
        so it's simpler to ignore them as they're not really relevant for these
        tests."""
        date_fields = [
            "file_frozen",
            "file_modified",
            "file_uploaded",
            "file_deleted",
            "date_modified",
            "date_created",
            "date_removed",
        ]
        for field in date_fields:
            data.pop(field, None)

        characteristics_data = data["file_characteristics"]
        for field in ["file_created", "metadata_modified"]:
            characteristics_data.pop(field, None)
        data["checksum"].pop("checked", None)

    def normalize_file_data(self, data: dict):
        self.remove_dates(data)
        data.pop("parent_directory")
        return data

    def _get_original_expected_file_data(self):
        return {
            "id": ANY,
            "byte_size": 100,
            "checksum": {
                "algorithm": "SHA-256",
                "value": "habeebit",
            },
            "file_format": "html/text",
            "file_name": "file_name_1",
            "file_path": "/project_x_FROZEN/Experiment_X/file_name_1",
            "file_storage": {"id": 1, "identifier": "pid:urn:storageidentifier1"},
            "identifier": "pid:urn:1",
            "file_characteristics": {
                "application_name": "Application Name",
                "description": "A nice description 120",
                "encoding": "UTF-8",
                "title": "A title 120",
            },
            "open_access": False,
            "pas_compatible": True,
            "project_identifier": "project_x",
            "service_created": "metax",
            "removed": False,
        }

    def _get_expected_new_file_data(self):
        data = self._get_original_expected_file_data()
        data.update(
            {
                "checksum": {
                    "value": "othervalue",
                    "algorithm": "SHA-256",
                },
                "file_name": "file_name_1",
                "file_path": data["file_path"].replace("/some/path", "/some/other_path"),
                "identifier": "urn:nbn:fi:csc-ida201401200000000001",
                "service_created": "testuser",
            }
        )
        data["file_path"] = data["file_path"].replace("/Experiment_X/", "/test/path/")
        data["project_identifier"] = "test_project_identifier"
        return data

    def _get_updated_test_data(self):
        data = self._get_original_test_data()
        data["checksum_value"] = "greatvalue"
        return data

    def _get_expected_updated_file_data(self):
        expected_file = self._get_original_expected_file_data()
        expected_file["checksum"]["value"] = "greatvalue"
        expected_file["service_modified"] = "testuser"
        return expected_file

    def _get_expected_removed_file_data(self):
        expected_file = self._get_original_expected_file_data()
        expected_file["removed"] = True
        return expected_file

    def process_response(self, response, expected_status=200):
        self.assertEqual(response.status_code, expected_status)
        self.assertEqual(len(responses.calls), 1)
        self.assertEqual(responses.calls[0].request.url, "https://metax-test/v3/files/from-legacy")
        data = json.loads(responses.calls[0].request.body)
        return [self.normalize_file_data(f) for f in data]

    @responses.activate
    def test_create(self):
        self.mock_responses()
        data = self._get_new_test_data()
        response = self.process_response(
            self.client.post("/rest/v2/files", data, format="json"), expected_status=201
        )
        expected_body = [self._get_expected_new_file_data()]
        self.assertEqual(response, expected_body)

    @responses.activate
    def test_bulk_create(self):
        self.mock_responses()
        data = self._get_new_test_data()
        response = self.process_response(
            self.client.post("/rest/v2/files", [data], format="json"), expected_status=201
        )
        expected_body = [self._get_expected_new_file_data()]
        self.assertEqual(response, expected_body)

    @responses.activate
    def test_update(self):
        self.mock_responses()
        data = self._get_updated_test_data()
        response = self.process_response(
            self.client.put(f"/rest/v2/files/{self.identifier}", data, format="json")
        )
        expected_body = [self._get_expected_updated_file_data()]
        self.assertEqual(response, expected_body)

    @responses.activate
    def test_bulk_update(self):
        self.mock_responses()
        data = self._get_updated_test_data()
        response = self.process_response(self.client.put("/rest/v2/files", [data], format="json"))
        expected_body = [self._get_expected_updated_file_data()]
        self.assertEqual(response, expected_body)

    @responses.activate
    def test_partial_update(self):
        self.mock_responses()
        data = self._get_updated_test_data()
        response = self.process_response(
            self.client.patch(f"/rest/v2/files/{self.identifier}", data, format="json")
        )
        expected_body = [self._get_expected_updated_file_data()]
        self.assertEqual(response, expected_body)

    @responses.activate
    def test_bulk_partial_update(self):
        self.mock_responses()
        data = self._get_updated_test_data()
        response = self.process_response(self.client.patch("/rest/v2/files", [data], format="json"))
        expected_body = [self._get_expected_updated_file_data()]
        self.assertEqual(response, expected_body)

    @responses.activate
    def test_delete(self):
        self.mock_responses()
        response = self.process_response(
            self.client.delete(f"/rest/v2/files/{self.identifier}", format="json")
        )
        expected_body = [self._get_expected_removed_file_data()]
        self.assertEqual(response, expected_body)

    @responses.activate
    def test_bulk_delete(self):
        self.mock_responses()
        data = [self.identifier]
        response = self.process_response(
            self.client.delete("/rest/v2/files", data, format="json")
        )
        expected_body = [self._get_expected_removed_file_data()]
        self.assertEqual(response, expected_body)

    @responses.activate
    def test_restore(self):
        self.mock_responses()
        data = [self.identifier]
        delete_resp = self.client.delete(f"/rest/v2/files/{self.identifier}", data, format="json")
        self.assertEqual(delete_resp.status_code, 200)
        responses.calls.reset()

        response = self.process_response(
            self.client.post("/rest/v2/files/restore", data, format="json")
        )
        expected_body = [
            {**self._get_original_expected_file_data(), "service_modified": "testuser"}
        ]
        self.assertEqual(response, expected_body)
