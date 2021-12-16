# This file is part of the Metax API service
#
# Copyright 2017-2018 Ministry of Education and Culture, Finland
#
# :author: CSC - IT Center for Science Ltd., Espoo Finland <servicedesk@csc.fi>
# :license: MIT


from json import load as json_load
import uuid

from django.core.management import call_command

from rest_framework import status
from rest_framework.test import APITestCase

from metax_api.models import EditorUserPermission
from metax_api.tests.utils import TestClassUtils, test_data_file_path


class EditorUserPermissionApiWriteCommon(APITestCase, TestClassUtils):
    @classmethod
    def setUpClass(cls):
        """
        Loaded only once for test cases inside this class.
        """
        call_command("loaddata", test_data_file_path, verbosity=0)
        super(EditorUserPermissionApiWriteCommon, cls).setUpClass()

    def setUp(self):
        self.cr_from_test_data = self._get_whole_object_from_test_data(
            "catalogrecord", requested_pk=1
        )
        self.crid = self.cr_from_test_data["pk"]
        self.permissionid = self.cr_from_test_data["fields"]["editor_permissions_id"]
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


class EditorUserPermissionApiWriteBasicTests(EditorUserPermissionApiWriteCommon):

    """
    Basic read operations
    """

    def test_write_editor_permission(self):
        self._set_http_authorization("service")
        data = {"role": "editor", "user_id": "test_editor"}
        response = self.client.get("/rest/datasets/%d/editor_permissions/users" % self.crid)
        user_count = len(response.data)
        response = self.client.post(
            "/rest/datasets/%d/editor_permissions/users" % self.crid, data, format="json"
        )
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)
        self.assertEqual(str(response.data["editor_permissions"]), self.permissionid)
        response = self.client.get("/rest/datasets/%d/editor_permissions/users" % self.crid)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data), user_count + 1)

    def test_write_editor_permission_invalid_data(self):
        self._set_http_authorization("service")
        data = {"role": "editor"}
        response = self.client.get("/rest/datasets/%d/editor_permissions/users" % self.crid)
        user_count = len(response.data)
        response = self.client.post(
            "/rest/datasets/%d/editor_permissions/users" % self.crid, data, format="json"
        )
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST, response.data)
        response = self.client.get("/rest/datasets/%d/editor_permissions/users" % self.crid)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data), user_count)

    def test_write_editor_permission_existing_userid(self):
        self._set_http_authorization("service")
        data = {"role": "editor", "user_id": "double_editor"}
        response = self.client.get("/rest/datasets/%d/editor_permissions/users" % self.crid)
        user_count = len(response.data)
        response = self.client.post(
            "/rest/datasets/%d/editor_permissions/users" % self.crid, data, format="json"
        )
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)
        response = self.client.post(
            "/rest/datasets/%d/editor_permissions/users" % self.crid, data, format="json"
        )
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST, response.data)
        response = self.client.get("/rest/datasets/%d/editor_permissions/users" % self.crid)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data), user_count + 1)

    def test_write_editor_permission_change_values(self):
        self._set_http_authorization("service")
        data = {"role": "creator", "user_id": "change_editor"}
        response = self.client.get("/rest/datasets/%d/editor_permissions/users" % self.crid)
        response = self.client.post(
            "/rest/datasets/%d/editor_permissions/users" % self.crid, data, format="json"
        )
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)
        new_data = {"role": "editor"}
        response = self.client.patch(
            "/rest/datasets/%d/editor_permissions/users/%s"
            % (self.crid, response.data.get("user_id")),
            new_data,
            format="json",
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        self.assertEqual(response.data.get("role"), "editor")

    def test_write_editor_permission_remove_users(self):
        self._set_http_authorization("service")
        data = {"role": "creator", "user_id": "new_creator"}
        response = self.client.post(
            "/rest/datasets/%d/editor_permissions/users" % self.crid, data, format="json"
        )
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)
        response = self.client.delete(
            "/rest/datasets/%d/editor_permissions/users/%s" % (self.crid, data.get("user_id"))
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)

        response = self.client.get("/rest/datasets/%d/editor_permissions/users" % self.crid)
        for user in response.data:
            if user.get("role") == "creator":
                response = self.client.delete(
                    "/rest/datasets/%d/editor_permissions/users/%s"
                    % (self.crid, user.get("user_id"))
                )
                self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST, response.data)
            else:
                response = self.client.delete(
                    "/rest/datasets/%d/editor_permissions/users/%s"
                    % (self.crid, user.get("user_id"))
                )
                self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)

    def test_write_editor_permission_add_removed_user(self):
        self._set_http_authorization("service")
        data = {"role": "editor", "user_id": "new_editor"}
        # add
        response = self.client.post(
            "/rest/datasets/%d/editor_permissions/users" % self.crid, data, format="json"
        )
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)

        # remove
        response = self.client.delete(
            "/rest/datasets/%d/editor_permissions/users/%s" % (self.crid, data.get("user_id"))
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        removed_user = EditorUserPermission.objects_unfiltered.get(
            user_id="new_editor", editor_permissions_id=self.permissionid
        )
        self.assertEqual(removed_user.removed, True)
        response = self.client.post(
            "/rest/datasets/%d/editor_permissions/users" % self.crid, data, format="json"
        )
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)
        self.assertEqual(response.data.get("removed"), False)
