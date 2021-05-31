# This file is part of the Metax API service
#
# Copyright 2017-2020 Ministry of Education and Culture, Finland
#
# :author: CSC - IT Center for Science Ltd., Espoo Finland <servicedesk@csc.fi>
# :license: MIT

from copy import deepcopy

from rest_framework import status

from metax_api.models import CatalogRecord, CatalogRecordV2

from .write import CatalogRecordApiWriteCommon

CR = CatalogRecordV2


class CatalogRecordApiLock(CatalogRecordApiWriteCommon):
    """
    Tests that no datasets created or edited by v2 api can be further
    modified by using v1 api beacuse v2 is not fully compatible with v1 api.
    """

    def setUp(self):
        super().setUp()

    def _create_v1_dataset(self, cumulative=False, cr=None, draft=False):
        if not cr:
            cr = self.cr_test_data

        if cumulative:
            # not to mess up with the original test dataset
            cr = deepcopy(cr)
            cr["cumulative_state"] = 1

        if draft:
            params = "draft"
        else:
            params = ""

        response = self.client.post(f"/rest/datasets?{params}", cr, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED)
        self.assertEqual(response.data["api_meta"]["version"], 1)

        cr = CatalogRecord.objects.get(pk=response.data["id"])
        self.assertEqual(cr.api_meta["version"], 1, "api_version should be 1")

        return response.data

    def _create_v2_dataset(self, cumulative=False, cr=None, draft=False):
        if not cr:
            cr = self.cr_test_data

        if cumulative:
            # not to mess up with the original test dataset
            cr = deepcopy(cr)
            cr["cumulative_state"] = 1

        if draft:
            params = "draft"
        else:
            params = ""

        response = self.client.post(f"/rest/v2/datasets?{params}", cr, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED)
        self.assertEqual(response.data["api_meta"]["version"], 2)

        cr = CatalogRecord.objects.get(pk=response.data["id"])
        self.assertEqual(cr.api_meta["version"], 2, "api_version should be 2")

        return response.data

    def _assert_api_version(self, identifier, version):
        response = self.client.get(f"/rest/datasets/{identifier}")
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        self.assertEqual(
            response.data["api_meta"]["version"],
            version,
            "api_version should have been changed",
        )

    def test_datasets_are_assigned_to_correct_api_version_on_create(self):
        """
        Test that api versions are correctly set on dataset creation
        """

        self._create_v1_dataset()
        self._create_v2_dataset()

    def test_version_v2_blocks_all_v1_apis(self):
        """
        Test that none of the v1 update apis are working when dataset version is 2
        """

        cr_v2 = self._create_v2_dataset()

        cr_v2["research_dataset"]["title"]["en"] = "totally unique changes to the english title"

        # test basic update operations

        response = self.client.put(f'/rest/datasets/{cr_v2["id"]}', cr_v2, format="json")
        self.assertEqual(
            response.status_code,
            status.HTTP_400_BAD_REQUEST,
            "v1 modifications should have been blocked",
        )
        self.assertTrue(
            "correct api version" in response.data["detail"][0],
            "msg should be about api version",
        )

        response = self.client.patch(f'/rest/datasets/{cr_v2["id"]}', cr_v2, format="json")
        self.assertEqual(
            response.status_code,
            status.HTTP_400_BAD_REQUEST,
            "v1 modifications should have been blocked",
        )
        self.assertTrue(
            "correct api version" in response.data["detail"][0],
            "msg should be about api version",
        )

        cr_v1 = self._create_v1_dataset()
        cr_v1["research_dataset"]["title"]["en"] = "this title update should be file"

        response = self.client.put("/rest/datasets", [cr_v1, cr_v2], format="json")
        self.assertEqual(
            response.status_code,
            status.HTTP_400_BAD_REQUEST,
            "v1 modifications should have been blocked",
        )
        self.assertTrue(
            "correct api version" in response.data["detail"][0],
            "msg should be about api version",
        )

        response = self.client.patch("/rest/datasets", [cr_v1, cr_v2], format="json")
        self.assertEqual(
            response.status_code,
            status.HTTP_400_BAD_REQUEST,
            "v1 modifications should have been blocked",
        )
        self.assertTrue(
            "correct api version" in response.data["detail"][0],
            "msg should be about api version",
        )

        # test change_cumulative_state

        params = f'identifier={cr_v2["identifier"]}&cumulative_state=1'
        response = self.client.post(
            f"/rpc/datasets/change_cumulative_state?{params}", format="json"
        )
        self.assertEqual(
            response.status_code,
            status.HTTP_400_BAD_REQUEST,
            "v1 modifications should have been blocked",
        )
        self.assertTrue(
            "correct api version" in response.data["detail"][0],
            "msg should be about api version",
        )

        # add directory with some files in it to test other rpc apis
        # dir contains first 5 files 'pid:urn:n'
        cr_dirs = deepcopy(self.cr_test_data)
        cr_dirs["research_dataset"]["directories"] = [
            {
                "title": "dir_name",
                "identifier": "pid:urn:dir:3",
                "description": "What is in this directory",
                "use_category": {
                    "in_scheme": "http://uri.suomi.fi/codelist/fairdata/use_category",
                    "identifier": "http://uri.suomi.fi/codelist/fairdata/use_category/code/method",
                },
            }
        ]

        cr_dirs = self._create_v2_dataset(cr=cr_dirs)

        # test refresh_directory_content

        # the directory does not have anything to add but it is not relevant here, since
        # api should return error about the api version before that
        params = f'cr_identifier={cr_dirs["identifier"]}&dir_identifier=pid:urn:dir:3'
        response = self.client.post(
            f"/rpc/datasets/refresh_directory_content?{params}", format="json"
        )
        self.assertEqual(
            response.status_code,
            status.HTTP_400_BAD_REQUEST,
            "v1 modifications should have been blocked",
        )
        self.assertTrue(
            "correct api version" in response.data["detail"][0],
            "msg should be about api version",
        )

        # test fix_deprecated

        response = self.client.delete("/rest/files/pid:urn:1")
        self.assertEqual(response.status_code, status.HTTP_200_OK)

        params = f'identifier={cr_dirs["identifier"]}'
        response = self.client.post(f"/rpc/datasets/fix_deprecated?{params}", format="json")
        self.assertEqual(
            response.status_code,
            status.HTTP_400_BAD_REQUEST,
            "v1 modifications should have been blocked",
        )
        self.assertTrue(
            "correct api version" in response.data["detail"][0],
            "msg should be about api version",
        )

    def test_v2_rest_api_modification_updates_api_version(self):
        """
        Tests that when v1 datasets are updated using any v2 rest api, their api version is changed to v2
        and thus further updates by v1 api should be prevented
        """

        # test basic single PUT/PATCH updates

        for http_verb in ["put", "patch"]:
            update_request = getattr(self.client, http_verb)
            cr_v1 = self._create_v1_dataset()

            cr_v1["research_dataset"]["title"]["en"] = "some new title"

            response = update_request(f'/rest/v2/datasets/{cr_v1["id"]}', cr_v1, format="json")

            self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)

            self._assert_api_version(cr_v1["identifier"], 2)

        # test basic bulk PUT/PATCH updates

        for http_verb in ["put", "patch"]:
            update_request = getattr(self.client, http_verb)
            cr_v1_0 = self._create_v1_dataset()
            cr_v1_1 = self._create_v1_dataset()

            cr_v1_0["research_dataset"]["title"]["en"] = "some new title"
            cr_v1_1["research_dataset"]["title"]["en"] = "some new title for another"

            response = update_request("/rest/v2/datasets", [cr_v1_0, cr_v1_1], format="json")

            self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)

            self._assert_api_version(cr_v1_0["identifier"], 2)
            self._assert_api_version(cr_v1_1["identifier"], 2)

        # test POST /rest/v2/datasets/{PID}/files updates api version

        # make dataset cumulative so that file additions are allowed for published datasets
        cr_v1 = self._create_v1_dataset(cumulative=True)

        file_changes = {
            "files": [
                {"identifier": "pid:urn:1"},  # adds file, but entry is otherwise not persisted
                {
                    "identifier": "pid:urn:2",
                    "title": "custom title",
                    "use_category": {"identifier": "source"},
                },
            ]
        }

        response = self.client.post(
            f'/rest/v2/datasets/{cr_v1["id"]}/files', file_changes, format="json"
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)

        self._assert_api_version(cr_v1["id"], 2)

        # test PUT/PATCH /rest/v2/datasets/{PID}/files/user_metadata

        for http_verb in ["put", "patch"]:
            update_request = getattr(self.client, http_verb)
            cr = self.cr_test_data
            cr["research_dataset"]["files"] = [
                {
                    "title": "customtitle",
                    "identifier": "pid:urn:1",
                    "use_category": {
                        "in_scheme": "http://uri.suomi.fi/codelist/fairdata/use_category",
                        "identifier": "http://uri.suomi.fi/codelist/fairdata/use_category/code/source",
                    },
                },
            ]

            cr_v1 = self._create_v1_dataset(cr)

            user_metadata = {
                "files": [
                    {
                        "identifier": "pid:urn:1",
                        "title": "custom title 1",
                        "use_category": {"identifier": "source"},
                    }
                ]
            }

            response = update_request(
                f'/rest/v2/datasets/{cr_v1["id"]}/files/user_metadata',
                user_metadata,
                format="json",
            )
            self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)

            self._assert_api_version(cr_v1["id"], 2)

    def test_v2_rpc_api_modification_updates_api_version(self):
        """
        Tests that when v1 datasets are updated using any v2 rpc api, their api version is changed to v2
        and thus further updates by v1 api should be prevented
        """

        # test change_cumulative_state
        cr_v1_cum = self._create_v1_dataset(cumulative=True)

        # create one non-cumulative dataset without files so it can be edited with this rpc
        cr_v1_non_cum = self.cr_test_data
        cr_v1_non_cum["cumulative_state"] = 0
        del cr_v1_non_cum["research_dataset"]["files"]
        cr_v1_non_cum = self._create_v1_dataset(cr=cr_v1_non_cum)

        params = f'identifier={cr_v1_non_cum["identifier"]}&cumulative_state=1'
        response = self.client.post(
            f"/rpc/v2/datasets/change_cumulative_state?{params}", format="json"
        )
        self.assertEqual(response.status_code, status.HTTP_204_NO_CONTENT, response.data)

        self._assert_api_version(cr_v1_non_cum["identifier"], 2)

        params = f'identifier={cr_v1_cum["identifier"]}&cumulative_state=2'
        response = self.client.post(
            f"/rpc/v2/datasets/change_cumulative_state?{params}", format="json"
        )
        self.assertEqual(response.status_code, status.HTTP_204_NO_CONTENT, response.data)

        self._assert_api_version(cr_v1_cum["identifier"], 2)

        # test create_draft
        cr_v1 = self._create_v1_dataset()

        params = f'identifier={cr_v1["identifier"]}'
        response = self.client.post(f"/rpc/v2/datasets/create_draft?{params}", format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)

        response = self.client.get(f'/rest/datasets/{cr_v1["identifier"]}')
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        cr_v1 = response.data

        new_id = response.data["identifier"]
        response = self.client.get(f"/rest/datasets/{new_id}")
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        new_dataset = response.data

        self._assert_api_version(cr_v1["identifier"], 2)
        self._assert_api_version(new_dataset["identifier"], 2)

        # test create_new_version
        cr_v1 = self._create_v1_dataset()

        params = f'identifier={cr_v1["identifier"]}'
        response = self.client.post(f"/rpc/v2/datasets/create_new_version?{params}", format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)

        response = self.client.get(f'/rest/datasets/{cr_v1["identifier"]}')
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        cr_v1 = response.data

        new_id = response.data["identifier"]
        response = self.client.get(f"/rest/datasets/{new_id}")
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)

        new_dataset = response.data

        self._assert_api_version(cr_v1["identifier"], 2)
        self._assert_api_version(new_dataset["identifier"], 2)

        # publish dataset does not need to be checked because v1 datasets cannot be drafts

        # merge draft does not need to be checked because v1 datasets cannot have "parent" dataset
