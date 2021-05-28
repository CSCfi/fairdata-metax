# This file is part of the Metax API service
#
# Copyright 2017-2018 Ministry of Education and Culture, Finland
#
# :author: CSC - IT Center for Science Ltd., Espoo Finland <servicedesk@csc.fi>
# :license: MIT

import responses
from django.conf import settings
from django.core.management import call_command
from rest_framework import status
from rest_framework.test import APITestCase

from metax_api.models import CatalogRecord, Directory, File
from metax_api.tests.api.rest.base.views.datasets.write import (
    CatalogRecordApiWriteAssignFilesCommon,
    CatalogRecordApiWriteCommon,
)
from metax_api.tests.utils import TestClassUtils, get_test_oidc_token, test_data_file_path


class DatasetRPCTests(APITestCase, TestClassUtils):
    @classmethod
    def setUpClass(cls):
        """
        Loaded only once for test cases inside this class.
        """
        super().setUpClass()
        call_command("loaddata", test_data_file_path, verbosity=0)

    def setUp(self):
        super().setUp()
        self.create_end_user_data_catalogs()

    @responses.activate
    def test_get_minimal_dataset_template(self):
        """
        Retrieve and use a minimal dataset template example from the api.
        """

        # query param type is missing, should return error and description what to do.
        response = self.client.get("/rpc/datasets/get_minimal_dataset_template")
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)

        # test preventing typos
        response = self.client.get("/rpc/datasets/get_minimal_dataset_template?type=wrong")
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)

        # test minimal dataset for service use
        response = self.client.get("/rpc/datasets/get_minimal_dataset_template?type=service")
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertTrue("metadata_provider_org" in response.data)
        self.assertTrue("metadata_provider_user" in response.data)
        self._use_http_authorization(username="testuser")
        response = self.client.post("/rest/datasets", response.data, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED)

        # test minimal dataset for end user use
        response = self.client.get("/rpc/datasets/get_minimal_dataset_template?type=enduser")
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertTrue("metadata_provider_org" not in response.data)
        self.assertTrue("metadata_provider_user" not in response.data)
        self._use_http_authorization(method="bearer", token=get_test_oidc_token())
        self._mock_token_validation_succeeds()
        response = self.client.post("/rest/datasets", response.data, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED)

    def test_set_preservation_identifier(self):
        self._set_http_authorization("service")

        # Parameter 'identifier' is required
        response = self.client.post("/rpc/datasets/set_preservation_identifier")
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)

        # Nonexisting identifier should return 404
        response = self.client.post(
            "/rpc/datasets/set_preservation_identifier?identifier=nonexisting"
        )
        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND)

        # Create ida data catalog
        dc = self._get_object_from_test_data("datacatalog", requested_index=0)
        dc_id = settings.IDA_DATA_CATALOG_IDENTIFIER
        dc["catalog_json"]["identifier"] = dc_id
        self.client.post("/rest/datacatalogs", dc, format="json")

        # Test OK ops

        # Create new ida cr without doi
        cr_json = self.client.get("/rest/datasets/1").data
        cr_json.pop("preservation_identifier", None)
        cr_json.pop("identifier")
        cr_json["research_dataset"].pop("preferred_identifier", None)
        cr_json["data_catalog"] = dc_id
        cr_json["research_dataset"]["issued"] = "2018-01-01"
        cr_json["research_dataset"]["publisher"] = {
            "@type": "Organization",
            "name": {"en": "publisher"},
        }

        response = self.client.post("/rest/datasets?pid_type=urn", cr_json, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)
        identifier = response.data["identifier"]

        # Verify rpc api returns the same doi as the one that is set to the datasets' preservation identifier
        response = self.client.post(
            f"/rpc/datasets/set_preservation_identifier?identifier={identifier}"
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)

        response2 = self.client.get(f"/rest/datasets/{identifier}")
        self.assertEqual(response2.status_code, status.HTTP_200_OK, response2.data)
        self.assertEqual(response.data, response2.data["preservation_identifier"], response2.data)

        # Return 400 if request is not correct datacite format
        response2.data["research_dataset"].pop("issued")
        response = self.client.put(f"/rest/datasets/{identifier}", response2.data, format="json")
        self.assertEqual(response2.status_code, status.HTTP_200_OK, response2.data)

        response = self.client.post(
            f"/rpc/datasets/set_preservation_identifier?identifier={identifier}"
        )
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST, response.data)


class ChangeCumulativeStateRPC(CatalogRecordApiWriteCommon):

    """
    This class tests different cumulative state transitions. Different parent class is needed
    to get use of cr_test_data.
    """

    def _create_cumulative_dataset(self, state):
        self.cr_test_data["cumulative_state"] = state

        response = self.client.post("/rest/datasets", self.cr_test_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)
        self.assertEqual(response.data["cumulative_state"], state, response.data)

        return response.data

    def _update_cr_cumulative_state(self, identifier, state, result=status.HTTP_204_NO_CONTENT):
        url = "/rpc/datasets/change_cumulative_state?identifier=%s&cumulative_state=%d"

        response = self.client.post(url % (identifier, state), format="json")
        self.assertEqual(response.status_code, result, response.data)

        return response.data

    def _get_cr(self, identifier):
        response = self.client.get("/rest/datasets/%s" % identifier, format="json")
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)

        return response.data

    def _assert_file_counts(self, new_version):
        new_count = CatalogRecord.objects.get(pk=new_version["id"]).files.count()
        old_count = CatalogRecord.objects.get(
            pk=new_version["previous_dataset_version"]["id"]
        ).files.count()
        self.assertEqual(new_count, old_count, "file count between versions should match")

    def test_transitions_from_NO(self):
        """
        Transition from non-cumulative to active is allowed but to closed it is not.
        New version is created if non-cumulative dataset is marked actively cumulative.
        """
        cr_orig = self._create_cumulative_dataset(0)
        orig_preferred_identifier = cr_orig["research_dataset"]["preferred_identifier"]
        orig_record_count = CatalogRecord.objects.all().count()
        self._update_cr_cumulative_state(cr_orig["identifier"], 2, status.HTTP_400_BAD_REQUEST)

        self._update_cr_cumulative_state(cr_orig["identifier"], 1, status.HTTP_200_OK)
        self.assertEqual(CatalogRecord.objects.all().count(), orig_record_count + 1)

        # get updated dataset
        old_version = self._get_cr(cr_orig["identifier"])
        self.assertEqual(old_version["cumulative_state"], 0, "original status should not changed")
        self.assertTrue("next_dataset_version" in old_version, "should have new dataset")

        # cannot change old dataset cumulative_status
        self._update_cr_cumulative_state(old_version["identifier"], 2, status.HTTP_400_BAD_REQUEST)

        # new version of the dataset should have new cumulative state
        new_version = self._get_cr(old_version["next_dataset_version"]["identifier"])
        self.assertTrue(
            new_version["research_dataset"]["preferred_identifier"] != orig_preferred_identifier
        )
        self.assertEqual(
            new_version["cumulative_state"], 1, "new version should have changed status"
        )
        self._assert_file_counts(new_version)

    def test_transitions_from_YES(self):
        cr = self._create_cumulative_dataset(1)
        orig_record_count = CatalogRecord.objects.all().count()
        self._update_cr_cumulative_state(cr["identifier"], 0, status.HTTP_400_BAD_REQUEST)
        self.assertEqual(CatalogRecord.objects.all().count(), orig_record_count)

        # active to non-active cumulation is legal
        self._update_cr_cumulative_state(cr["identifier"], 2)
        cr = self._get_cr(cr["identifier"])
        self.assertEqual(cr["cumulative_state"], 2, "dataset should have changed status")

    def test_correct_response_data(self):
        """
        Tests that correct information is set to response.
        """
        cr = self._create_cumulative_dataset(0)
        return_data = self._update_cr_cumulative_state(cr["identifier"], 1, status.HTTP_200_OK)
        self.assertTrue(
            "new_version_created" in return_data,
            "new_version_created should be returned",
        )
        new_version_identifier = return_data["new_version_created"]["identifier"]
        cr = self._get_cr(cr["identifier"])
        self.assertEqual(cr["next_dataset_version"]["identifier"], new_version_identifier)

        new_cr = self._get_cr(new_version_identifier)
        return_data = self._update_cr_cumulative_state(new_cr["identifier"], 2)
        self.assertEqual(
            return_data, None, "when new version is not created, return should be None"
        )


class RefreshDirectoryContent(CatalogRecordApiWriteAssignFilesCommon):

    url = "/rpc/datasets/refresh_directory_content?cr_identifier=%s&dir_identifier=%s"

    def _assert_rd_total_byte_size(self, file_size_before, file_size_after, expected_addition):
        self.assertEqual(file_size_after, file_size_before + expected_addition)

    def test_refresh_adds_new_files(self):
        self._add_directory(self.cr_test_data, "/TestExperiment")
        response = self.client.post("/rest/datasets", self.cr_test_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)
        cr_id = response.data["identifier"]
        dir_id = response.data["research_dataset"]["directories"][0]["identifier"]
        file_byte_size_before = response.data["research_dataset"]["total_files_byte_size"]

        # freeze two files to /TestExperiment/Directory_2
        self._freeze_files_to_root()
        response = self.client.post(self.url % (cr_id, dir_id), format="json")
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        self.assertEqual(response.data["number_of_files_added"], 2)

        new_version = CatalogRecord.objects.get(id=response.data["new_version_created"]["id"])
        file_size_after = new_version.research_dataset["total_files_byte_size"]
        self.assertEqual(
            new_version.files.count(),
            new_version.previous_dataset_version.files.count() + 2,
        )
        self._assert_rd_total_byte_size(
            file_byte_size_before, file_size_after, self._single_file_byte_size * 2
        )

        # freeze two files to /TestExperiment/Directory_2/Group_3
        self._freeze_new_files()
        response = self.client.post(self.url % (new_version.identifier, dir_id), format="json")
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        self.assertEqual(response.data["number_of_files_added"], 2)

        new_version = CatalogRecord.objects.get(id=response.data["new_version_created"]["id"])
        self.assertEqual(
            new_version.files.count(),
            new_version.previous_dataset_version.files.count() + 2,
        )

    def test_adding_parent_dir_allows_refreshes_to_child_dirs(self):
        """
        When parent directory is added to dataset, refreshes to child directories are also possible.
        """
        self._add_directory(self.cr_test_data, "/TestExperiment/Directory_2")
        response = self.client.post("/rest/datasets", self.cr_test_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)
        cr_id = response.data["identifier"]

        self._freeze_new_files()
        frozen_dir = Directory.objects.filter(
            directory_path="/TestExperiment/Directory_2/Group_3"
        ).first()

        response = self.client.post(self.url % (cr_id, frozen_dir.identifier), format="json")
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        self.assertEqual(response.data["number_of_files_added"], 2)

        new_version = CatalogRecord.objects.get(id=response.data["new_version_created"]["id"])
        self.assertEqual(
            new_version.files.count(),
            new_version.previous_dataset_version.files.count() + 2,
        )

    def test_refresh_adds_new_files_multiple_locations(self):
        self._add_directory(self.cr_test_data, "/TestExperiment/Directory_2")
        response = self.client.post("/rest/datasets", self.cr_test_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)
        cr_id = response.data["identifier"]
        dir_id = response.data["research_dataset"]["directories"][0]["identifier"]

        self._freeze_new_files()
        self._freeze_files_to_root()
        response = self.client.post(self.url % (cr_id, dir_id), format="json")
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        self.assertEqual(response.data["number_of_files_added"], 4)

        new_version = CatalogRecord.objects.get(id=response.data["new_version_created"]["id"])
        self.assertEqual(
            new_version.files.count(),
            new_version.previous_dataset_version.files.count() + 4,
        )

    def test_refresh_adds_no_new_files_from_upper_dirs(self):
        """
        Include parent/subdir and freeze files to parent. Should be no changes in the dataset.
        """
        self._add_directory(self.cr_test_data, "/TestExperiment/Directory_2/Group_2")
        response = self.client.post("/rest/datasets", self.cr_test_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)
        cr_id = response.data["identifier"]
        dir_id = response.data["research_dataset"]["directories"][0]["identifier"]
        file_count_before = CatalogRecord.objects.get(identifier=cr_id).files.count()

        self._freeze_files_to_root()
        response = self.client.post(self.url % (cr_id, dir_id), format="json")
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        self.assertEqual(response.data["number_of_files_added"], 0)

        cr_after = CatalogRecord.objects.get(identifier=cr_id)
        self.assertEqual(cr_after.next_dataset_version, None, "should not have new dataset version")
        self.assertEqual(cr_after.files.count(), file_count_before, "No new files should be added")

    def test_refresh_with_cumulative_state_yes(self):
        """
        When dataset has cumulation active, files are added to dataset but no new version is created.
        """
        self._add_directory(self.cr_test_data, "/TestExperiment/Directory_2")
        self.cr_test_data["cumulative_state"] = 1
        response = self.client.post("/rest/datasets", self.cr_test_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)
        cr_id = response.data["identifier"]
        dir_id = response.data["research_dataset"]["directories"][0]["identifier"]
        file_count_before = CatalogRecord.objects.get(identifier=cr_id).files.count()
        file_byte_size_before = response.data["research_dataset"]["total_files_byte_size"]

        self._freeze_new_files()
        self._freeze_files_to_root()
        response = self.client.post(self.url % (cr_id, dir_id), format="json")
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        self.assertEqual(response.data["number_of_files_added"], 4)

        cr_after = CatalogRecord.objects.get(identifier=cr_id)
        file_size_after = cr_after.research_dataset["total_files_byte_size"]
        self.assertEqual(cr_after.next_dataset_version, None, "should not have new dataset version")
        self.assertEqual(
            len(cr_after.get_metadata_version_listing()),
            2,
            "new metadata version should be created",
        )
        self.assertEqual(cr_after.files.count(), file_count_before + 4)
        self._assert_rd_total_byte_size(
            file_byte_size_before, file_size_after, self._single_file_byte_size * 4
        )

        # check that added sub dir is found in catalog records internal variables
        new_dir = Directory.objects.filter(
            directory_path__startswith="/TestExperiment/Directory_2/Group_3"
        ).first()

        self.assertTrue(
            str(new_dir.id) in cr_after._directory_data,
            "New dir id should be found in cr",
        )
        self.assertEqual(new_dir.byte_size, self._single_file_byte_size * 2)

    def test_refreshing_deprecated_dataset_is_not_allowed(self):
        self._add_directory(self.cr_test_data, "/TestExperiment/Directory_2")
        response = self.client.post("/rest/datasets", self.cr_test_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)
        cr_id = response.data["identifier"]
        dir_id = response.data["research_dataset"]["directories"][0]["identifier"]

        removed_file_id = CatalogRecord.objects.get(identifier=cr_id).files.all()[0].id
        response = self.client.delete(f"/rest/files/{removed_file_id}")
        self.assertEqual(response.status_code, status.HTTP_200_OK)

        response = self.client.get(f"/rest/datasets/{cr_id}")
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        depr_cr = response.data
        self._freeze_new_files()
        response = self.client.post(self.url % (depr_cr["identifier"], dir_id), format="json")
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST, response.data)

    def test_adding_files_from_non_assigned_dir_is_not_allowed(self):
        """
        Only allow adding files from directories which paths are included in the research dataset.
        """
        self._add_directory(self.cr_test_data, "/SecondExperiment/Data")
        response = self.client.post("/rest/datasets", self.cr_test_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)
        cr_id = response.data["identifier"]

        # create another dataset so that dir /SecondExperiment/Data_Config will be created
        self._add_directory(self.cr_test_data, "/SecondExperiment/Data_Config")
        response = self.client.post("/rest/datasets", self.cr_test_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)
        dir_id = response.data["research_dataset"]["directories"][1]["identifier"]

        response = self.client.post(self.url % (cr_id, dir_id), format="json")
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST, response.data)
        self.assertTrue("not included" in response.data["detail"][0], response.data)


class FixDeprecatedTests(CatalogRecordApiWriteAssignFilesCommon):
    """
    Tests for fix_deprecated api. Tests remove files/directories from database and then checks that
    fix_deprecated api removes those files/directories from the given dataset.
    """

    def _get_next_dataset_version(self, identifier):
        """
        Returns next dataset version for dataset <identifier>
        """
        response = self.client.get("/rest/datasets/%s" % identifier)
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        self.assertTrue("next_dataset_version" in response.data, "new dataset should be created")
        response = self.client.get(
            "/rest/datasets/%s" % response.data["next_dataset_version"]["identifier"]
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)

        return response.data

    def _check_new_dataset_version(self, identifier, file_count_before, deleted_file_ids):
        """
        Ensures that next dataset version for dataset <identifier> has correct deprecated state and right
        files included. Research_dataset must be checked separately.
        """
        new_cr_version = self._get_next_dataset_version(identifier)
        new_version_files = CatalogRecord.objects.get(pk=new_cr_version["id"]).files.all()
        self.assertEqual(new_cr_version["deprecated"], False, "deprecated flag should be fixed")
        self.assertEqual(
            new_version_files.count(),
            file_count_before - len(deleted_file_ids),
            "new file count should be on smaller than before",
        )
        self.assertTrue(
            all(d not in new_version_files.values_list("id", flat=True) for d in deleted_file_ids),
            "Deleted files should not be found in new version",
        )

        return new_cr_version

    def _delete_files_from_directory_path(self, path):
        """
        Deletes files from sub directories as well
        """
        deleted_file_ids = [
            id
            for id in File.objects.filter(file_path__startswith=path).values_list("id", flat=True)
        ]
        response = self.client.delete("/rest/files", deleted_file_ids, format="json")
        self.assertEqual(response.status_code, status.HTTP_200_OK)

        return deleted_file_ids

    def test_fix_deprecated_files(self):
        file_count_before = CatalogRecord.objects.get(pk=1).files.count()

        # delete file from dataset
        deleted_file = File.objects.get(pk=1)
        response = self.client.delete("/rest/files/%s" % deleted_file.identifier)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        response = self.client.get("/rest/datasets/1")
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertTrue(response.data["deprecated"], "dataset should be deprecated")
        identifier = response.data["identifier"]

        # fix deprecated dataset
        response = self.client.post("/rpc/datasets/fix_deprecated?identifier=%s" % identifier)
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)

        # ensure that new dataset version is not deprecated, dataset files contain only the non-removed file
        # and removed file is deleted from research_dataset
        new_cr_version = self._check_new_dataset_version(
            identifier, file_count_before, [deleted_file.id]
        )
        rd_filenames = [f["identifier"] for f in new_cr_version["research_dataset"]["files"]]
        self.assertTrue(
            deleted_file.identifier not in rd_filenames,
            "deleted file should not be in research_dataset",
        )

    def test_fix_deprecated_directories(self):
        """
        This test adds parent directory of two files to a dataset and deletes the files
        """
        # add/describe the parent directory of the newly added file to the dataset
        self._add_directory(self.cr_test_data, "/TestExperiment/Directory_2/Group_2/Group_2_deeper")
        response = self.client.post("/rest/datasets", self.cr_test_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)
        cr_with_dir = response.data
        file_count_before = CatalogRecord.objects.get(
            identifier=cr_with_dir["identifier"]
        ).files.count()

        # delete/unfreeze the files contained by described directory
        deleted_file_ids = self._delete_files_from_directory_path(
            "/TestExperiment/Directory_2/Group_2/Group_2_deeper"
        )

        # fix deprecated dataset
        response = self.client.post(
            "/rpc/datasets/fix_deprecated?identifier=%s" % cr_with_dir["identifier"]
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)

        # ensure old dataset is unchanged
        response = self.client.get("/rest/datasets/%s" % cr_with_dir["identifier"])
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(
            len(response.data["research_dataset"]["directories"]),
            1,
            "old dataset version directories should still contain the removed directory",
        )
        self.assertTrue(
            response.data["deprecated"],
            "old dataset version deprecated flag should not be changed",
        )

        # ensure the new dataset is correct
        new_cr_version = self._check_new_dataset_version(
            cr_with_dir["identifier"], file_count_before, deleted_file_ids
        )
        self.assertTrue("directories" not in new_cr_version["research_dataset"])

    def test_fix_deprecated_nested_directories_1(self):
        """
        This test adds parent directory to dataset and then deletes all files from sub directory.
        research_dataset should be unchanged and file count should be smaller for new version.
        """
        self._add_directory(self.cr_test_data, "/TestExperiment/Directory_2/Group_2")
        response = self.client.post("/rest/datasets", self.cr_test_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)

        cr_before = response.data
        file_count_before = CatalogRecord.objects.get(
            identifier=cr_before["identifier"]
        ).files.count()

        # delete/unfreeze the files contained by described directory
        deleted_file_ids = self._delete_files_from_directory_path(
            "/TestExperiment/Directory_2/Group_2/Group_2_deeper"
        )

        # fix deprecated dataset
        response = self.client.post(
            "/rpc/datasets/fix_deprecated?identifier=%s" % cr_before["identifier"]
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)

        # ensure the new dataset is correct
        new_cr_version = self._check_new_dataset_version(
            cr_before["identifier"], file_count_before, deleted_file_ids
        )
        self.assertEqual(
            cr_before["research_dataset"].get("files"),
            new_cr_version["research_dataset"].get("files"),
            "should be no difference in research_dataset.files",
        )
        self.assertEqual(
            cr_before["research_dataset"].get("directories"),
            new_cr_version["research_dataset"].get("directories"),
            "should be no difference in research_dataset.dirs",
        )

    def test_fix_deprecated_nested_directories_2(self):
        """
        This test adds parent and sub directory to dataset and then deletes all files from sub directory.
        research_dataset and file count should change for new version.
        """
        self._add_directory(self.cr_test_data, "/TestExperiment/Directory_2/Group_2")
        self._add_directory(self.cr_test_data, "/TestExperiment/Directory_2/Group_2/Group_2_deeper")
        response = self.client.post("/rest/datasets", self.cr_test_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)
        cr_before = response.data
        file_count_before = CatalogRecord.objects.get(
            identifier=cr_before["identifier"]
        ).files.count()

        deleted_file_ids = self._delete_files_from_directory_path(
            "/TestExperiment/Directory_2/Group_2/Group_2_deeper"
        )

        # fix deprecated dataset
        response = self.client.post(
            "/rpc/datasets/fix_deprecated?identifier=%s" % cr_before["identifier"]
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)

        # ensure the new dataset is correct
        new_cr_version = self._check_new_dataset_version(
            cr_before["identifier"], file_count_before, deleted_file_ids
        )
        # description field conveniently has the dir path for directories which are saved by _add_directory
        rd_dirpaths = [d["description"] for d in new_cr_version["research_dataset"]["directories"]]
        self.assertTrue("/TestExperiment/Directory_2/Group_2/Group_2_deeper" not in rd_dirpaths)

    def test_fix_deprecated_nested_directories_3(self):
        """
        This test adds parent and sub directory to dataset and then deletes all files from sub directory.
        research_dataset and file count should change for new version.
        """
        self._add_directory(self.cr_test_data, "/TestExperiment/Directory_2/Group_2")
        self._add_file(
            self.cr_test_data,
            "/TestExperiment/Directory_2/Group_2/Group_2_deeper/file_11.txt",
        )
        self._add_file(
            self.cr_test_data,
            "/TestExperiment/Directory_2/Group_2/Group_2_deeper/file_12.txt",
        )
        self._add_file(self.cr_test_data, "/TestExperiment/Directory_2/Group_2/file_09.txt")
        self._add_file(self.cr_test_data, "/TestExperiment/Directory_2/Group_2/file_10.txt")
        response = self.client.post("/rest/datasets", self.cr_test_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)
        cr_before = response.data
        file_count_before = CatalogRecord.objects.get(
            identifier=cr_before["identifier"]
        ).files.count()

        # delete/unfreeze the files contained by described directory
        deleted_file_ids = self._delete_files_from_directory_path(
            "/TestExperiment/Directory_2/Group_2/Group_2_deeper"
        )

        # fix deprecated dataset
        response = self.client.post(
            "/rpc/datasets/fix_deprecated?identifier=%s" % cr_before["identifier"]
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)

        # ensure the new dataset is correct
        new_cr_version = self._check_new_dataset_version(
            cr_before["identifier"], file_count_before, deleted_file_ids
        )
        rd_dirpaths = [d["description"] for d in new_cr_version["research_dataset"]["directories"]]
        rd_filepaths = [f["description"] for f in new_cr_version["research_dataset"]["files"]]
        self.assertTrue("/TestExperiment/Directory_2/Group_2/Group_2_deeper" not in rd_dirpaths)
        self.assertTrue(
            "/TestExperiment/Directory_2/Group_2/Group_2_deeper/file_11.txt" not in rd_filepaths
        )
        self.assertTrue(
            "/TestExperiment/Directory_2/Group_2/Group_2_deeper/file_12.txt" not in rd_filepaths
        )
