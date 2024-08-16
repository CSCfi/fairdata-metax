# This file is part of the Metax API service
#
# Copyright 2017-2018 Ministry of Education and Culture, Finland
#
# :author: CSC - IT Center for Science Ltd., Espoo Finland <servicedesk@csc.fi>
# :license: MIT

from django.core.management import call_command
from django.test import override_settings
from rest_framework import status
from rest_framework.test import APITestCase
import responses

from metax_api.models import CatalogRecord, Directory, File
from metax_api.tests.utils import TestClassUtils, test_data_file_path


class FileRPCTests(APITestCase, TestClassUtils):
    def setUp(self):
        """
        Reloaded for every test case
        """
        super().setUp()
        call_command("loaddata", test_data_file_path, verbosity=0)
        self._use_http_authorization()


class DeleteProjectTests(FileRPCTests):
    """
    Tests cover user authentication, wrong type of requests and
    correct result for successful operations.
    """

    def test_wrong_parameters(self):
        # correct user, no project identifier
        response = self.client.post("/rpc/files/delete_project")
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)

        # nonexisting project identifier:
        response = self.client.post("/rpc/files/delete_project?project_identifier=non_existing")
        self.assertEqual(response.data["deleted_files_count"], 0)

        # wrong request method
        response = self.client.delete(
            "/rpc/files/delete_project?project_identifier=research_project_112"
        )
        self.assertEqual(response.status_code, 501)

        # wrong user
        self._use_http_authorization("api_auth_user")
        response = self.client.post(
            "/rpc/files/delete_project?project_identifier=research_project_112"
        )
        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN)

    def test_known_project_identifier(self):
        response = self.client.post(
            "/rpc/files/delete_project?project_identifier=research_project_112"
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK)

    def test_files_are_marked_deleted(self):
        files_count_before = File.objects.filter(project_identifier="research_project_112").count()
        response = self.client.post(
            "/rpc/files/delete_project?project_identifier=research_project_112"
        )
        self.assertEqual(files_count_before, response.data["deleted_files_count"])

    def test_directories_are_deleted(self):
        self.client.post("/rpc/files/delete_project?project_identifier=research_project_112")
        directories_count_after = Directory.objects.filter(
            project_identifier="research_project_112"
        ).count()
        self.assertEqual(directories_count_after, 0)

    def test_datasets_are_marked_deprecated(self):
        file_ids = File.objects.filter(project_identifier="project_x").values_list("id", flat=True)
        related_dataset = CatalogRecord.objects.filter(files__in=file_ids).distinct("id")[0]
        self.client.post("/rpc/files/delete_project?project_identifier=project_x")
        response = self.client.get("/rest/datasets/%s" % related_dataset.identifier)
        self.assertEqual(response.data["deprecated"], True)


class FlushProjectTests(FileRPCTests):
    """
    Checks that an entire project's files and directories can be deleted.
    """

    def test_wrong_parameters(self):
        # correct user, no project identifier
        self._use_http_authorization("metax")
        response = self.client.post("/rpc/files/flush_project")
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)

        # nonexisting project identifier:
        response = self.client.post("/rpc/files/flush_project?project_identifier=non_existing")
        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND)

        # wrong request method
        response = self.client.delete(
            "/rpc/files/flush_project?project_identifier=research_project_112"
        )
        self.assertEqual(response.status_code, 501)

        # wrong user
        self._use_http_authorization("api_auth_user")
        response = self.client.post(
            "/rpc/files/flush_project?project_identifier=research_project_112"
        )
        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN)

    def test_known_project_identifier(self):
        self._use_http_authorization("metax")
        response = self.client.post(
            "/rpc/files/flush_project?project_identifier=research_project_112"
        )
        self.assertEqual(response.status_code, status.HTTP_204_NO_CONTENT)

    def test_files_are_deleted(self):
        self._use_http_authorization("metax")

        # make sure project has files before deleting them
        files_count_before = File.objects.filter(project_identifier="research_project_112").count()
        self.assertNotEqual(files_count_before, 0)

        # delete all files for project
        response = self.client.post(
            "/rpc/files/flush_project?project_identifier=research_project_112"
        )
        self.assertEqual(response.status_code, status.HTTP_204_NO_CONTENT)

        # make sure all files are now deleted
        files_count_after = File.objects.filter(project_identifier="research_project_112").count()
        self.assertEqual(files_count_after, 0)

    def test_directories_are_deleted(self):
        self._use_http_authorization("metax")

        # make sure project has directories before deleting them
        dirs_count_before = Directory.objects.filter(
            project_identifier="research_project_112"
        ).count()
        self.assertNotEqual(dirs_count_before, 0)

        # delete all directories for project
        response = self.client.post(
            "/rpc/files/flush_project?project_identifier=research_project_112"
        )
        self.assertEqual(response.status_code, status.HTTP_204_NO_CONTENT)

        # make sure all directories are now deleted
        dirs_count_after = Directory.objects.filter(
            project_identifier="research_project_112"
        ).count()
        self.assertEqual(dirs_count_after, 0)


@override_settings(
    METAX_V3={
        "HOST": "metax-test",
        "TOKEN": "test-token",
        "INTEGRATION_ENABLED": True,
        "PROTOCOL": "https",
    }
)
class FileRPCSyncToV3Tests(FileRPCTests):
    """
    Checks that deleting or flushing projects is synced to V3.
    """

    def setUp(self):
        super().setUp()
        self._use_http_authorization("metax")

    def mock_responses(self):
        responses.add(
            responses.DELETE,
            url="https://metax-test/v3/files",
            body="",
        )

    @responses.activate
    def test_delete_project(self):
        self.mock_responses()
        # delete all files for project
        response = self.client.post(
            "/rpc/files/delete_project?project_identifier=research_project_112"
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(responses.calls), 1)
        self.assertEqual(
            responses.calls[0].request.url,
            "https://metax-test/v3/files?csc_project=research_project_112&flush=false",
        )

    @responses.activate
    def test_flush_project(self):
        self.mock_responses()
        # delete all files for project
        response = self.client.post(
            "/rpc/files/flush_project?project_identifier=research_project_112"
        )
        self.assertEqual(response.status_code, status.HTTP_204_NO_CONTENT)
        self.assertEqual(len(responses.calls), 1)
        self.assertEqual(
            responses.calls[0].request.url,
            "https://metax-test/v3/files?csc_project=research_project_112&flush=true",
        )
