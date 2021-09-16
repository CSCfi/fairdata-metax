# This file is part of the Metax API service
#
# Copyright 2017-2018 Ministry of Education and Culture, Finland
#
# :author: CSC - IT Center for Science Ltd., Espoo Finland <servicedesk@csc.fi>
# :license: MIT
import logging
from unittest.mock import patch
from uuid import uuid4

from django.core.management import call_command
from rest_framework import status
from rest_framework.test import APITestCase

from metax_api.models import ApiError
from metax_api.tests.utils import TestClassUtils, test_data_file_path, testcase_log_console

_logger = logging.getLogger(__name__)


class ApiErrorReadBasicTests(APITestCase, TestClassUtils):

    """
    Basic read operations
    """

    @classmethod
    def setUpClass(cls):
        """
        Loaded only once for test cases inside this class.
        """
        call_command("loaddata", test_data_file_path, verbosity=0)
        super(ApiErrorReadBasicTests, cls).setUpClass()

    def setUp(self):
        super(ApiErrorReadBasicTests, self).setUp()
        self._use_http_authorization(username="metax")

    def mock_api_error_consume(self):
        """
        ApiErrors are created when fetching Rabbitmq queue but when running testcases Rabbitmq is not available.
        This mocks the consume part while the publishing part is actually not doing anything.
        """
        error = {
            "method": "POST",
            "user": "metax",
            "data": { "metadata_owner_org": "abc-org-123" },
            "headers": {
                "HTTP_COOKIE": ""
            },
            "status_code": 400,
            "response": {
                "data_catalog": [
                    "ErrorDetail(string=This field is required., code=required)"
                ]
            },
            "traceback":
                "Traceback(most recent call last): File/usr/local/lib/python3.8/site-packages/rest_framework/views.py",
            "url": "/rest/datasets",
            "identifier": f"2021-06-29T11:10:54-{str(uuid4())[:8]}",
            "exception_time": "2021-06-29T11:10:54+00:00",
            "other": {
                "bulk_request": True,
                "data_row_count": 2
            }
        }
        ApiError.objects.create(identifier=error["identifier"], error=error)

    def _assert_fields_presence(self, error_json):
        """
        Check presence and absence of some key information.
        """
        self.assertEqual("identifier" in error_json, True, error_json)
        self.assertEqual("data" in error_json, True, error_json)
        self.assertEqual("response" in error_json, True, error_json)
        self.assertEqual("traceback" in error_json, True, error_json)
        self.assertEqual("url" in error_json, True, error_json)
        self.assertEqual(
            "HTTP_AUTHORIZATION" in error_json["headers"],
            False,
            error_json["headers"],
        )

    @patch("metax_api.services.rabbitmq_service._RabbitMQServiceDummy.consume_api_errors", mock_api_error_consume)
    def test_list_errors(self):
        """
        Each requesting resulting in an error should leave behind one API error entry.
        """
        cr_1 = self.client.get("/rest/v2/datasets/1").data
        cr_1.pop("id")
        cr_1.pop("identifier")
        cr_1.pop("data_catalog")  # causes an error

        response = self.client.post("/rest/v2/datasets", cr_1, format="json")
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        response = self.client.post("/rest/v2/datasets", cr_1, format="json")
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)

        response = self.client.get("/rest/v2/apierrors")
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)

    @patch("metax_api.services.rabbitmq_service._RabbitMQServiceDummy.consume_api_errors", mock_api_error_consume)
    def test_get_error_details(self):
        self.apierror_identifier = f"2021-06-29T11:10:54-{str(uuid4())[:8]}"

        cr_1 = self.client.get("/rest/v2/datasets/1").data
        cr_1.pop("id")
        cr_1.pop("identifier")
        cr_1.pop("data_catalog")  # causes an error
        cr_1["research_dataset"]["title"] = {"en": "Abc"}

        response = self.client.post("/rest/v2/datasets", cr_1, format="json")
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)

        error = ApiError.objects.last()

        response = self.client.get(f"/rest/v2/apierrors/{error.identifier}")
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)

        error_json = response.data["error"]

        self._assert_fields_presence(error_json)
        self.assertTrue("data_catalog" in error_json["response"], error_json["response"])

    @patch("metax_api.services.rabbitmq_service._RabbitMQServiceDummy.consume_api_errors", mock_api_error_consume)
    @testcase_log_console(_logger)
    def test_delete_error_details(self):
        cr_1 = self.client.get("/rest/v2/datasets/1").data
        cr_1.pop("id")
        cr_1.pop("identifier")
        cr_1.pop("data_catalog")  # causes an error

        response = self.client.post("/rest/v2/datasets", cr_1, format="json")
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)

        error = ApiError.objects.last()

        response = self.client.delete(f"/rest/v2/apierrors/{error.identifier}")
        self.assertEqual(response.status_code, status.HTTP_204_NO_CONTENT, response.data)

        response = self.client.get("/rest/v2/apierrors")
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)

    @patch("metax_api.services.rabbitmq_service._RabbitMQServiceDummy.consume_api_errors", mock_api_error_consume)
    @testcase_log_console(_logger)
    def test_delete_all_error_details(self):
        cr_1 = self.client.get("/rest/v2/datasets/1").data
        cr_1.pop("id")
        cr_1.pop("identifier")
        cr_1.pop("data_catalog")  # causes an error

        response = self.client.post("/rest/v2/datasets", cr_1, format="json")
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST, response.data)
        response = self.client.post("/rest/v2/datasets", cr_1, format="json")
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST, response.data)

        # ensure something was produced...
        response = self.client.get("/rest/v2/apierrors")

        response = self.client.post("/rest/v2/apierrors/flush")
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)

        response = self.client.get("/rest/v2/apierrors")
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)

    @patch("metax_api.services.rabbitmq_service._RabbitMQServiceDummy.consume_api_errors", mock_api_error_consume)
    def test_bulk_operation_produces_error_entry(self):
        """
        Ensure also bulk operations produce error entries.
        """
        cr_1 = self.client.get("/rest/v2/datasets/1").data
        cr_1.pop("id")
        cr_1.pop("identifier")
        cr_1.pop("data_catalog")  # causes an error
        response = self.client.post("/rest/v2/datasets", [cr_1, cr_1], format="json")
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)

        response = self.client.get("/rest/v2/apierrors")
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)

        error = ApiError.objects.last()

        self._assert_fields_presence(error.error)

        self.assertEqual("other" in error.error, True, response.data)
        self.assertEqual("bulk_request" in error.error["other"], True, response.data)
        self.assertEqual("data_row_count" in error.error["other"], True, response.data)

    def test_api_permitted_only_to_metax_user(self):
        # uses testuser by default
        self._use_http_authorization()
        response = self.client.get("/rest/v2/apierrors")
        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN)
        response = self.client.get("/rest/v2/apierrors/123")
        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN)
        response = self.client.delete("/rest/v2/apierrors/123")
        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN)
        response = self.client.post("/rest/v2/apierrors/flush_errors")
        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN)