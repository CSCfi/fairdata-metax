# This file is part of the Metax API service
#
# Copyright 2017-2018 Ministry of Education and Culture, Finland
#
# :author: CSC - IT Center for Science Ltd., Espoo Finland <servicedesk@csc.fi>
# :license: MIT

from django.core.management import call_command
from rest_framework import status
from rest_framework.test import APITestCase

from metax_api.models import CatalogRecordV2, File
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

    def test_datasets_are_marked_deprecated(self):
        file_ids = File.objects.filter(project_identifier="project_x").values_list("id", flat=True)
        related_dataset = CatalogRecordV2.objects.filter(files__in=file_ids).distinct("id")[0]

        response = self.client.post("/rpc/v2/files/delete_project?project_identifier=project_x")
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)

        response = self.client.get("/rest/v2/datasets/%s" % related_dataset.identifier)
        self.assertEqual(response.data["deprecated"], True)
