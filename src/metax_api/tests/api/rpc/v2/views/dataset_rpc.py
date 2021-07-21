# This file is part of the Metax API service
#
# Copyright 2017-2018 Ministry of Education and Culture, Finland
#
# :author: CSC - IT Center for Science Ltd., Espoo Finland <servicedesk@csc.fi>
# :license: MIT

import responses
from django.conf import settings
from rest_framework import status

from metax_api.models import CatalogRecordV2
from metax_api.tests.api.rest.base.views.datasets.write import CatalogRecordApiWriteCommon
from metax_api.tests.utils import get_test_oidc_token

CR = CatalogRecordV2


class ChangeCumulativeStateRPC(CatalogRecordApiWriteCommon):

    """
    This class tests different cumulative state transitions. Different parent class is needed
    to get use of cr_test_data.
    """

    def _create_cumulative_dataset(self, state):
        self.cr_test_data["cumulative_state"] = state

        response = self.client.post("/rest/v2/datasets", self.cr_test_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)
        self.assertEqual(response.data["cumulative_state"], state, response.data)

        return response.data

    def _update_cr_cumulative_state(self, identifier, state, result=status.HTTP_204_NO_CONTENT):
        response = self.client.post(
            "/rpc/v2/datasets/change_cumulative_state?identifier=%s&cumulative_state=%d"
            % (identifier, state),
            format="json",
        )
        self.assertEqual(response.status_code, result, response.data)
        return response.data

    def _get_cr(self, identifier):
        response = self.client.get("/rest/v2/datasets/%s" % identifier, format="json")
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)

        return response.data

    def _assert_file_counts(self, new_version):
        new_count = CatalogRecordV2.objects.get(pk=new_version["id"]).files.count()
        old_count = CatalogRecordV2.objects.get(
            pk=new_version["previous_dataset_version"]["id"]
        ).files.count()
        self.assertEqual(new_count, old_count, "file count between versions should match")

    def test_transitions_from_NO(self):
        """
        If dataset is published, and it has files, state can't be changed from NO to anything.
        """
        cr_orig = self._create_cumulative_dataset(0)
        self._update_cr_cumulative_state(
            cr_orig["identifier"], CR.CUMULATIVE_STATE_YES, status.HTTP_400_BAD_REQUEST
        )
        self._update_cr_cumulative_state(
            cr_orig["identifier"],
            CR.CUMULATIVE_STATE_CLOSED,
            status.HTTP_400_BAD_REQUEST,
        )

    def test_transitions_from_YES(self):
        """
        From YES, a published cumulative dataset can only be set to CLOSED.
        """
        cr = self._create_cumulative_dataset(1)
        orig_record_count = CatalogRecordV2.objects.all().count()
        self._update_cr_cumulative_state(
            cr["identifier"], CR.CUMULATIVE_STATE_NO, status.HTTP_400_BAD_REQUEST
        )
        self.assertEqual(CatalogRecordV2.objects.all().count(), orig_record_count)

        # active to non-active cumulation is legal
        self._update_cr_cumulative_state(cr["identifier"], CR.CUMULATIVE_STATE_CLOSED)
        cr = self._get_cr(cr["identifier"])
        self.assertEqual(
            cr["cumulative_state"],
            CR.CUMULATIVE_STATE_CLOSED,
            "dataset should have changed status",
        )


class CatalogRecordVersionHandling(CatalogRecordApiWriteCommon):

    """
    New dataset versions are not created automatically when changing files of a dataset.
    New dataset versions can only be created by explicitly calling related RPC API.
    """

    def test_create_new_version(self):
        """
        A new dataset version can be created for datasets in data catalogs that support versioning.
        """
        response = self.client.post(
            "/rpc/v2/datasets/create_new_version?identifier=1", format="json"
        )
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)

        next_version_identifier = response.data.get("identifier")

        response = self.client.get("/rest/v2/datasets/1", format="json")
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        self.assertEqual(
            response.data.get("next_dataset_version", {}).get("identifier"),
            next_version_identifier,
        )

        response2 = self.client.get("/rest/v2/datasets/%s" % next_version_identifier, format="json")
        self.assertEqual(response2.status_code, status.HTTP_200_OK, response2.data)
        self.assertEqual(
            response2.data.get("previous_dataset_version", {}).get("identifier"),
            response.data["identifier"],
        )

    def test_delete_new_version_draft(self):
        """
        Ensure a new version that is created into draft state can be deleted, and is permanently deleted.
        """
        response = self.client.post(
            "/rpc/v2/datasets/create_new_version?identifier=1", format="json"
        )
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)

        next_version_identifier = response.data.get("identifier")

        response = self.client.delete(
            "/rest/v2/datasets/%s" % next_version_identifier, format="json"
        )
        self.assertEqual(response.status_code, status.HTTP_204_NO_CONTENT, response.data)

        num_found = CatalogRecordV2.objects_unfiltered.filter(
            identifier=next_version_identifier
        ).count()
        self.assertEqual(num_found, 0, "draft should have been permanently deleted")
        self.assertEqual(CatalogRecordV2.objects.get(pk=1).next_dataset_version, None)

    def test_version_already_exists(self):
        """
        If a dataset already has a next version, then a new version cannot be created.
        """
        response = self.client.post(
            "/rpc/v2/datasets/create_new_version?identifier=1", format="json"
        )
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)

        response = self.client.post(
            "/rpc/v2/datasets/create_new_version?identifier=1", format="json"
        )
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST, response.data)
        self.assertEqual(
            "already has a next version" in response.data["detail"][0],
            True,
            response.data,
        )

    def test_new_version_removes_deprecated_files(self):
        """
        If a new version is created from a deprecated dataset, then the new version should have deprecated=False
        status, and all files that are no longer available should be removed from the dataset.
        """
        original_cr = CatalogRecordV2.objects.get(pk=1)

        response = self.client.delete("/rest/v2/files/1", format="json")
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)

        response = self.client.post(
            "/rpc/v2/datasets/create_new_version?identifier=1", format="json"
        )
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)

        new_cr = CatalogRecordV2.objects.get(pk=response.data["id"])
        self.assertEqual(new_cr.deprecated, False)
        self.assertTrue(
            len(new_cr.research_dataset["files"]) < len(original_cr.research_dataset["files"])
        )
        self.assertTrue(
            new_cr.files.count() < original_cr.files(manager="objects_unfiltered").count()
        )

    def test_version_from_draft(self):
        """
        New versions cannot be created from drafts
        """
        response = self.client.post("/rest/v2/datasets?draft", self.cr_test_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)

        dft_id = response.data["identifier"]

        response = self.client.post(
            f"/rpc/v2/datasets/create_new_version?identifier={dft_id}", format="json"
        )
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST, response.data)
        self.assertTrue("draft" in response.data["detail"][0], response.data)

    def test_draft_blocks_version_creation(self):
        """
        Don't allow new versions if there are unmerged drafts for a dataset
        """
        response = self.client.post("/rpc/v2/datasets/create_draft?identifier=1")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)

        response = self.client.post("/rpc/v2/datasets/create_new_version?identifier=1")
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST, response.data)
        self.assertTrue("unmerged draft" in response.data["detail"][0], response.data)

    @responses.activate
    def test_authorization(self):
        """
        Creating a new dataset version should have the same authorization rules as when normally editing a dataset.
        """

        # service use should be OK
        response = self.client.post(
            "/rpc/v2/datasets/create_new_version?identifier=2", format="json"
        )
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)

        # test with end user, should fail
        self.token = get_test_oidc_token(new_proxy=True)
        self._mock_token_validation_succeeds()
        self._use_http_authorization(method="bearer", token=self.token)

        response = self.client.post(
            "/rpc/v2/datasets/create_new_version?identifier=1", format="json"
        )
        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN, response.data)

        # change owner, try again. should be OK
        cr = CatalogRecordV2.objects.get(pk=1)
        cr.metadata_provider_user = self.token["CSCUserName"]
        cr.editor = None
        cr.force_save()

        response = self.client.post(
            "/rpc/v2/datasets/create_new_version?identifier=1", format="json"
        )
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)


class CatalogRecordPublishing(CatalogRecordApiWriteCommon):
    def test_publish_new_dataset_draft(self):

        response = self.client.post("/rest/v2/datasets?draft", self.cr_test_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)

        cr_id = response.data["id"]

        response = self.client.post(
            "/rpc/v2/datasets/publish_dataset?identifier=%d" % cr_id, format="json"
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        self.assertTrue(response.data.get("preferred_identifier") is not None)

        response = self.client.get("/rest/v2/datasets/%d" % cr_id, format="json")
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        self.assertEqual(response.data["state"], "published")
        self.assertEqual(
            response.data["research_dataset"]["preferred_identifier"]
            == response.data["identifier"],
            False,
        )

    @responses.activate
    def test_authorization(self):
        """
        Test authorization for publishing draft datasets. Note: General visibility of draft datasets has
        been tested elsewhere,so authorization failure is not tested here since unauthorized people
        should not even see the records.
        """

        # test with service
        response = self.client.post("/rest/v2/datasets?draft", self.cr_test_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)

        response = self.client.post(
            "/rpc/v2/datasets/publish_dataset?identifier=%d" % response.data["id"],
            format="json",
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)

        # test with end user
        self.token = get_test_oidc_token(new_proxy=True)
        self._mock_token_validation_succeeds()
        self._use_http_authorization(method="bearer", token=self.token)
        self.create_end_user_data_catalogs()

        self.cr_test_data["data_catalog"] = settings.END_USER_ALLOWED_DATA_CATALOGS[0]
        self.cr_test_data["research_dataset"].pop("files", None)
        self.cr_test_data["research_dataset"].pop("directories", None)

        response = self.client.post("/rest/v2/datasets?draft", self.cr_test_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)

        response = self.client.post(
            "/rpc/v2/datasets/publish_dataset?identifier=%d" % response.data["id"],
            format="json",
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)


class CatalogRecordV1APIs(CatalogRecordApiWriteCommon):
    def test_ensure_v1_apis_dont_work(self):
        for endpoint in ("fix_deprecated", "refresh_directory_content"):
            response = self.client.post(f"/rpc/v2/datasets/{endpoint}", format="json")
            self.assertEqual(response.status_code, 501, response.data)
