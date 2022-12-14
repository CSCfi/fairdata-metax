# This file is part of the Metax API service
#
# Copyright 2017-2018 Ministry of Education and Culture, Finland
#
# :author: CSC - IT Center for Science Ltd., Espoo Finland <servicedesk@csc.fi>
# :license: MIT

from json import load as json_load
import uuid
import responses

from django.core.management import call_command

from rest_framework import status
from rest_framework.test import APITestCase

from metax_api.tests.utils import TestClassUtils, test_data_file_path


class EditorUserPermissionApiReadCommon(APITestCase, TestClassUtils):
    @classmethod
    def setUpClass(cls):
        """Loaded only once for test cases inside this class."""
        call_command("loaddata", test_data_file_path, verbosity=0)
        super(EditorUserPermissionApiReadCommon, cls).setUpClass()

    def setUp(self):
        self.cr_from_test_data = self._get_whole_object_from_test_data(
            "catalogrecord", requested_pk=1
        )
        self.crid = self.cr_from_test_data["pk"]
        self.identifier = "cr955e904-e3dd-4d7e-99f1-3fed446f96d1"
        self.permissionid = self.cr_from_test_data["fields"]["editor_permissions_id"]
        self.metadata_provider_user = self.cr_from_test_data["fields"]["metadata_provider_user"]
        self.editor_user_permission = self._get_whole_object_from_test_data(
            "editoruserpermission", requested_pk=str(uuid.UUID(int=1))
        )
        self.userid = self.editor_user_permission["fields"]["user_id"]
        self._use_http_authorization()

    def _get_whole_object_from_test_data(self, model_name, requested_pk=0):

        with open(test_data_file_path) as test_data_file:
            test_data = json_load(test_data_file)

        model = "metax_api.%s" % model_name

        for row in test_data:
            if row["model"] == model:
                if row["pk"] == requested_pk:
                    obj = {
                        "id": row["pk"],
                    }
                    obj.update(row)
                    return obj

        raise Exception(
            "Could not find model %s from test data with pk == %d. "
            "Are you certain you generated rows for model %s in generate_test_data.py?"
            % (model_name, requested_pk, model_name)
        )


class EditorUserPermissionApiReadBasicTests(EditorUserPermissionApiReadCommon):

    """Basic read operations."""

    def test_read_editor_permission_list_with_pk(self):
        response = self.client.get("/rest/datasets/%d/editor_permissions/users" % self.crid)
        self.assertEqual(response.status_code, status.HTTP_200_OK)

    def test_read_editor_permission_list_with_uuid(self):
        response = self.client.get("/rest/datasets/%s/editor_permissions/users" % self.identifier)
        self.assertEqual(response.status_code, status.HTTP_200_OK)

    def test_read_editor_permission_list_invalid(self):
        response = self.client.get("/rest/datasets/99999/editor_permissions/users")
        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND)

    def test_read_editor_permission_details_by_pk(self):
        response = self.client.get(
            "/rest/datasets/%d/editor_permissions/users/%s" % (self.crid, self.userid)
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(str(response.data["editor_permissions"]), self.permissionid)
        self.assertEqual(response.data["user_id"], self.userid)

    def test_read_editor_permission_details_by_pk_invalid(self):
        response = self.client.get(
            "/rest/datasets/%d/editor_permissions/users/%s" % (self.crid, "invalid")
        )
        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND)

    @responses.activate
    def test_read_editor_permission_list_by_wrong_user(self):
        self._mock_token_validation_succeeds()
        self._use_http_authorization(
            method="bearer", token={"group_names": [], "CSCUserName": "not_dataset_creator"}
        )
        response = self.client.get(f"/rest/datasets/{self.crid}/editor_permissions/users")
        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN)

    @responses.activate
    def test_read_editor_permission_list_by_provider_user(self):
        self._mock_token_validation_succeeds()
        self._use_http_authorization(
            method="bearer", token={"group_names": [], "CSCUserName": self.metadata_provider_user}
        )
        response = self.client.get(f"/rest/datasets/{self.crid}/editor_permissions/users")
        self.assertEqual(response.status_code, status.HTTP_200_OK)
