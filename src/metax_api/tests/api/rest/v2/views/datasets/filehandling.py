# This file is part of the Metax API service
#
# Copyright 2017-2020 Ministry of Education and Culture, Finland
#
# :author: CSC - IT Center for Science Ltd., Espoo Finland <servicedesk@csc.fi>
# :license: MIT

from copy import deepcopy

import responses
from rest_framework import status

from metax_api.models import CatalogRecordV2, Directory, File
from metax_api.tests.utils import TestClassUtils, get_test_oidc_token

from .write import CatalogRecordApiWriteCommon

CR = CatalogRecordV2


class CatalogRecordApiWriteAssignFilesCommon(CatalogRecordApiWriteCommon, TestClassUtils):
    """
    Helper class to test file assignment in Metax. Does not include any tests itself.
    """

    def _get_file_from_test_data(self):
        from_test_data = self._get_object_from_test_data("file", requested_index=0)
        from_test_data.update(
            {
                "checksum": {
                    "value": "checksumvalue",
                    "algorithm": "SHA-256",
                    "checked": "2017-05-23T10:07:22.559656Z",
                },
                "file_name": "must_replace",
                "file_path": "must_replace",
                "identifier": "must_replace",
                "project_identifier": "must_replace",
                "file_storage": self._get_object_from_test_data("filestorage", requested_index=0),
            }
        )
        return from_test_data

    def _form_test_file_hierarchy(self):
        """
        A file hierarchy that will be created prior to executing the tests.
        Files and dirs from these files will then be added to a dataset.
        """
        file_data_1 = [
            {
                "file_name": "file_01.txt",
                "file_path": "/TestExperiment/Directory_1/Group_1/file_01.txt",
                "project_identifier": "testproject",
            },
            {
                "file_name": "file_02.txt",
                "file_path": "/TestExperiment/Directory_1/Group_1/file_02.txt",
                "project_identifier": "testproject",
            },
            {
                "file_name": "file_03.txt",
                "file_path": "/TestExperiment/Directory_1/Group_2/file_03.txt",
                "project_identifier": "testproject",
            },
            {
                "file_name": "file_04.txt",
                "file_path": "/TestExperiment/Directory_1/Group_2/file_04.txt",
                "project_identifier": "testproject",
            },
            {
                "file_name": "file_05.txt",
                "file_path": "/TestExperiment/Directory_1/file_05.txt",
                "project_identifier": "testproject",
            },
            {
                "file_name": "file_06.txt",
                "file_path": "/TestExperiment/Directory_1/file_06.txt",
                "project_identifier": "testproject",
            },
            {
                "file_name": "file_07.txt",
                "file_path": "/TestExperiment/Directory_2/Group_1/file_07.txt",
                "project_identifier": "testproject",
            },
            {
                "file_name": "file_08.txt",
                "file_path": "/TestExperiment/Directory_2/Group_1/file_08.txt",
                "project_identifier": "testproject",
            },
            {
                "file_name": "file_09.txt",
                "file_path": "/TestExperiment/Directory_2/Group_2/file_09.txt",
                "project_identifier": "testproject",
            },
            {
                "file_name": "file_10.txt",
                "file_path": "/TestExperiment/Directory_2/Group_2/file_10.txt",
                "project_identifier": "testproject",
            },
            {
                "file_name": "file_11.txt",
                "file_path": "/TestExperiment/Directory_2/Group_2/Group_2_deeper/file_11.txt",
                "project_identifier": "testproject",
            },
            {
                "file_name": "file_12.txt",
                "file_path": "/TestExperiment/Directory_2/Group_2/Group_2_deeper/file_12.txt",
                "project_identifier": "testproject",
            },
            {
                "file_name": "file_13.txt",
                "file_path": "/TestExperiment/Directory_2/file_13.txt",
                "project_identifier": "testproject",
            },
            {
                "file_name": "file_14.txt",
                "file_path": "/TestExperiment/Directory_2/file_14.txt",
                "project_identifier": "testproject",
            },
        ]

        file_data_2 = [
            {
                "file_name": "file_15.txt",
                "file_path": "/SecondExperiment/Directory_1/Group_1/file_15.txt",
                "project_identifier": "testproject_2",
            },
            {
                "file_name": "file_16.txt",
                "file_path": "/SecondExperiment/Directory_1/Group_1/file_16.txt",
                "project_identifier": "testproject_2",
            },
            {
                "file_name": "file_17.txt",
                "file_path": "/SecondExperiment/Directory_1/Group_2/file_18.txt",
                "project_identifier": "testproject_2",
            },
            {
                "file_name": "file_18.txt",
                "file_path": "/SecondExperiment/Directory_1/Group_2/file_18.txt",
                "project_identifier": "testproject_2",
            },
            {
                "file_name": "file_19.txt",
                "file_path": "/SecondExperiment/Directory_1/file_19.txt",
                "project_identifier": "testproject_2",
            },
            {
                "file_name": "file_20.txt",
                "file_path": "/SecondExperiment/Directory_1/file_20.txt",
                "project_identifier": "testproject_2",
            },
            {
                "file_name": "file_21.txt",
                "file_path": "/SecondExperiment/Data/file_21.txt",
                "project_identifier": "testproject_2",
            },
            {
                "file_name": "file_22.txt",
                "file_path": "/SecondExperiment/Data_Config/file_22.txt",
                "project_identifier": "testproject_2",
            },
            {
                "file_name": "file_23.txt",
                "file_path": "/SecondExperiment/Data_Config/file_23.txt",
                "project_identifier": "testproject_2",
            },
            {
                "file_name": "file_24.txt",
                "file_path": "/SecondExperiment/Data/History/file_24.txt",
                "project_identifier": "testproject_2",
            },
        ]

        file_template = self._get_file_from_test_data()
        del file_template["id"]
        self._single_file_byte_size = file_template["byte_size"]

        files_1 = []
        for i, f in enumerate(file_data_1):
            file = deepcopy(file_template)
            file.update(f, identifier="test:file:%s" % f["file_name"][-6:-4])
            files_1.append(file)

        files_2 = []
        for i, f in enumerate(file_data_2):
            file = deepcopy(file_template)
            file.update(f, identifier="test:file:%s" % f["file_name"][-6:-4])
            files_2.append(file)

        return files_1, files_2

    def _research_dataset_or_file_changes(self, rd_or_file_changes):
        """
        File and dir entries can be added both to research_dataset object, and
        the more simple "file changes" object that is sent to /rest/v2/datasets/pid/files.
        Methods who call this helper can treat the returned object as either, and only
        operate with "files" and "directories" keys.
        """
        if "research_dataset" in rd_or_file_changes:
            files_and_dirs = rd_or_file_changes["research_dataset"]
        else:
            files_and_dirs = rd_or_file_changes

        return files_and_dirs

    def _add_directory(self, ds, path, project=None):
        """
        Add directory to research_dataset object or file_changes object.
        """
        params = {"directory_path": path}
        if project:
            params["project_identifier"] = project

        identifier = Directory.objects.get(**params).identifier

        files_and_dirs = self._research_dataset_or_file_changes(ds)

        if "directories" not in files_and_dirs:
            files_and_dirs["directories"] = []

        files_and_dirs["directories"].append(
            {
                "identifier": identifier,
                "title": "Directory Title",
                "description": "This is directory at %s" % path,
                "use_category": {"identifier": "method"},
            }
        )

    def _add_file(self, ds, path):
        """
        Add file to research_dataset object or file_changes object.
        """
        identifier = File.objects.filter(file_path__startswith=path).first().identifier

        files_and_dirs = self._research_dataset_or_file_changes(ds)

        if "files" not in files_and_dirs:
            files_and_dirs["files"] = []

        files_and_dirs["files"].append(
            {
                "identifier": identifier,
                "title": "File Title",
                "description": "This is file at %s" % path,
                "use_category": {"identifier": "method"},
            }
        )

    def _add_nonexisting_directory(self, ds):
        """
        Add non-existing directory to research_dataset object or file_changes object.
        """
        files_and_dirs = self._research_dataset_or_file_changes(ds)

        files_and_dirs["directories"] = [
            {
                "identifier": "doesnotexist",
                "title": "Directory Title",
                "description": "This is directory does not exist",
                "use_category": {"identifier": "method"},
            }
        ]

    def _add_nonexisting_file(self, ds):
        """
        Add non-existing file to research_dataset object or file_changes object.
        """
        files_and_dirs = self._research_dataset_or_file_changes(ds)

        files_and_dirs["files"] = [
            {
                "identifier": "doesnotexist",
                "title": "File Title",
                "description": "This is file does not exist",
                "use_category": {"identifier": "method"},
            }
        ]

    def _remove_directory(self, ds, path):
        """
        Remove directory from research_dataset object or file_changes object.
        """
        files_and_dirs = self._research_dataset_or_file_changes(ds)

        if "directories" not in files_and_dirs:
            raise Exception("ds has no dirs")

        identifier = Directory.objects.get(directory_path=path).identifier

        for i, dr in enumerate(files_and_dirs["directories"]):
            if dr["identifier"] == identifier:
                ds["research_dataset"]["directories"].pop(i)
                return
        raise Exception("path %s not found in directories" % path)

    def _remove_file(self, ds, path):
        """
        Remove file from research_dataset object or file_changes object.
        """
        files_and_dirs = self._research_dataset_or_file_changes(ds)

        if "files" not in files_and_dirs:
            raise Exception("ds has no files")

        identifier = File.objects.get(file_path=path).identifier

        for i, f in enumerate(files_and_dirs["files"]):
            if f["identifier"] == identifier:
                ds["research_dataset"]["files"].pop(i)
                return
        raise Exception("path %s not found in files" % path)

    def _freeze_new_files(self):
        file_data = [
            {
                "file_name": "file_90.txt",
                "file_path": "/TestExperiment/Directory_2/Group_3/file_90.txt",
                "project_identifier": "testproject",
            },
            {
                "file_name": "file_91.txt",
                "file_path": "/TestExperiment/Directory_2/Group_3/file_91.txt",
                "project_identifier": "testproject",
            },
        ]

        file_template = self._get_file_from_test_data()
        del file_template["id"]
        self._single_file_byte_size = file_template["byte_size"]
        files = []

        for i, f in enumerate(file_data):
            file = deepcopy(file_template)
            file.update(f, identifier="frozen:later:file:%s" % f["file_name"][-6:-4])
            files.append(file)
        response = self.client.post("/rest/v2/files", files, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED)

    def _freeze_files_to_root(self):
        file_data = [
            {
                "file_name": "file_56.txt",
                "file_path": "/TestExperiment/Directory_2/file_56.txt",
                "project_identifier": "testproject",
            },
            {
                "file_name": "file_57.txt",
                "file_path": "/TestExperiment/Directory_2/file_57.txt",
                "project_identifier": "testproject",
            },
        ]

        file_template = self._get_file_from_test_data()
        del file_template["id"]
        self._single_file_byte_size = file_template["byte_size"]
        files = []

        for i, f in enumerate(file_data):
            file = deepcopy(file_template)
            file.update(f, identifier="frozen:later:file:%s" % f["file_name"][-6:-4])
            files.append(file)
        response = self.client.post("/rest/v2/files", files, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED)

    def setUp(self):
        """
        For each test:
        - remove files and dirs from the metadata record that is being created
        - create 12 new files in a new project
        """
        super().setUp()
        self.cr_test_data["research_dataset"].pop("id", None)
        self.cr_test_data["research_dataset"].pop("preferred_identifier", None)
        self.cr_test_data["research_dataset"].pop("files", None)
        self.cr_test_data["research_dataset"].pop("directories", None)
        project_files = self._form_test_file_hierarchy()
        for p_files in project_files:
            response = self.client.post("/rest/v2/files", p_files, format="json")
            self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)

    def assert_preferred_identifier_changed(self, response, true_or_false):
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        self.assertEqual(
            "next_dataset_version" in response.data,
            true_or_false,
            "this field should only be present if preferred_identifier changed",
        )
        if true_or_false is True:
            self.assertEqual(
                response.data["research_dataset"]["preferred_identifier"]
                != response.data["next_dataset_version"]["preferred_identifier"],
                true_or_false,
            )

    def assert_file_count(self, cr, expected_file_count):
        self.assertEqual(
            CatalogRecordV2.objects.get(pk=cr if type(cr) is int else cr["id"]).files.count(),
            expected_file_count,
        )

    def assert_total_files_byte_size(self, cr, expected_size):
        self.assertEqual(cr["research_dataset"]["total_files_byte_size"], expected_size)


class CatalogRecordApiWriteAssignFilesCommonV2(CatalogRecordApiWriteAssignFilesCommon):

    """
    Note !!!

    These common helper methods inherit from V1 class CatalogRecordApiWriteAssignFilesCommon.
    """

    def _add_file(self, ds, path, with_metadata=False):

        super()._add_file(ds, path)

        if with_metadata:
            return

        files_and_dirs = self._research_dataset_or_file_changes(ds)

        # else: addition entry only, which will not be persisted. keep only identifier

        files_and_dirs["files"][-1] = {"identifier": files_and_dirs["files"][-1]["identifier"]}

    def _exclude_file(self, ds, path):
        self._add_file(ds, path)

        files_and_dirs = self._research_dataset_or_file_changes(ds)

        files_and_dirs["files"][-1]["exclude"] = True

        assert len(files_and_dirs["files"][-1]) == 2

    def _add_directory(self, ds, path, project=None, with_metadata=False):
        super()._add_directory(ds, path)

        if with_metadata:
            return

        files_and_dirs = self._research_dataset_or_file_changes(ds)

        # else: addition entry only, which will not be persisted. keep only identifier

        files_and_dirs["directories"][-1] = {
            "identifier": files_and_dirs["directories"][-1]["identifier"]
        }

    def _exclude_directory(self, ds, path):
        self._add_directory(ds, path)

        files_and_dirs = self._research_dataset_or_file_changes(ds)

        files_and_dirs["directories"][-1]["exclude"] = True

        assert len(files_and_dirs["directories"][-1]) == 2


class CatalogRecordFileHandling(CatalogRecordApiWriteAssignFilesCommonV2):
    def _set_token_authentication(self):
        self.create_end_user_data_catalogs()
        self.token = get_test_oidc_token()
        self.token["group_names"].append("IDA01:testproject")
        self._use_http_authorization(method="bearer", token=self.token)
        self._mock_token_validation_succeeds()

    def _create_draft(self):
        self.cr_test_data["research_dataset"].pop("files", None)
        self.cr_test_data["research_dataset"].pop("directories", None)
        response = self.client.post("/rest/v2/datasets?draft", self.cr_test_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)
        return response.data["id"]

    @responses.activate
    def test_authorization(self):
        """
        When adding files to a dataset, the user must have membership of the related files' project.
        If changing files of an existing dataset, the user must have membership of the related files' project,
        in addition to being the owner of the dataset.
        """
        self._set_token_authentication()

        for with_metadata in (False, True):
            self._add_file(self.cr_test_data, "/TestExperiment/Directory_1/file_05.txt")
            self._add_file(
                self.cr_test_data,
                "/TestExperiment/Directory_1/file_06.txt",
                with_metadata=with_metadata,
            )
            response = self.client.post("/rest/v2/datasets", self.cr_test_data, format="json")
            self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)

    @responses.activate
    def test_retrieve_dataset_file_projects(self):
        """
        A helper api for retrieving current file projects of a dataset. There should always be
        only 0 or 1 projects.
        """
        self._set_token_authentication()

        self._add_file(self.cr_test_data, "/TestExperiment/Directory_1/file_05.txt")
        response = self.client.post("/rest/v2/datasets", self.cr_test_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)

        cr_id = response.data["id"]

        # user owns dataset
        response = self.client.get("/rest/v2/datasets/%d/projects" % cr_id, format="json")
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        self.assertEqual(len(response.data), 1, response.data)

        # user is authenticated, but does not own dataset
        response = self.client.get("/rest/v2/datasets/1/projects", format="json")
        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN, response.data)

        # anonymous user
        self.client._credentials = {}
        response = self.client.get("/rest/v2/datasets/%d/projects" % cr_id, format="json")
        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN, response.data)

    def test_dataset_files_id_list(self):
        self._add_file(self.cr_test_data, "/TestExperiment/Directory_1/file_05.txt")
        self._add_file(
            self.cr_test_data,
            "/TestExperiment/Directory_1/file_06.txt",
        )
        response = self.client.post("/rest/v2/datasets", self.cr_test_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)

        file1 = File.objects.get(file_path="/TestExperiment/Directory_1/file_05.txt")
        file2 = File.objects.get(file_path="/TestExperiment/Directory_1/file_06.txt")
        file2.delete() # soft delete

        cr_id = response.data["id"]
        response = self.client.get(
            f"/rest/v2/datasets/{cr_id}/files?id_list=true", format="json"
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        self.assertEqual(response.data, [file1.id, file2.id])


    def test_include_user_metadata_parameter(self):
        """
        When retrieving datasets, by default the "user metadata", or "dataset-specific file metadata" stored
        in research_dataset.files and research_dataset.directories should not be returned.
        """
        response = self.client.get("/rest/v2/datasets/1", format="json")
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        self.assertEqual("files" in response.data["research_dataset"], False, response.data)

        response = self.client.get("/rest/v2/datasets/1?include_user_metadata", format="json")
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        self.assertEqual("files" in response.data["research_dataset"], True, response.data)

    def test_create_files_are_saved(self):
        """
        A very simple "add two individual files" test. Only entries with dataset-specific
        metadata should be persisted in research_dataset.files.
        """
        self._add_file(self.cr_test_data, "/TestExperiment/Directory_1/file_05.txt")
        self._add_file(
            self.cr_test_data,
            "/TestExperiment/Directory_1/file_06.txt",
            with_metadata=True,
        )
        response = self.client.post(
            "/rest/v2/datasets?include_user_metadata", self.cr_test_data, format="json"
        )
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)
        self.assertEqual(len(response.data["research_dataset"]["files"]), 1)
        self.assert_file_count(response.data, 2)
        self.assert_total_files_byte_size(response.data, self._single_file_byte_size * 2)

    def test_allowed_projects(self):
        """
        Test the ?allowed_projects=x,y parameter when adding files.
        """
        self._add_file(self.cr_test_data, "/TestExperiment/Directory_1/file_05.txt")

        response = self.client.post(
            "/rest/v2/datasets?allowed_projects=testproject",
            self.cr_test_data,
            format="json",
        )
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)

        response = self.client.post(
            "/rest/v2/datasets?allowed_projects=no,projects",
            self.cr_test_data,
            format="json",
        )
        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN, response.data)

    def test_create_directories_are_saved(self):
        """
        A very simple "add two individual directories" test. Only entries with dataset-specific
        metadata should be persisted in research_dataset.directories.
        """
        self._add_directory(self.cr_test_data, "/TestExperiment/Directory_1/Group_1")
        self._add_directory(
            self.cr_test_data, "/TestExperiment/Directory_1/Group_2", with_metadata=True
        )
        response = self.client.post(
            "/rest/v2/datasets?include_user_metadata", self.cr_test_data, format="json"
        )
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)
        self.assertEqual(len(response.data["research_dataset"]["directories"]), 1)
        self.assert_file_count(response.data, 4)
        self.assert_total_files_byte_size(response.data, self._single_file_byte_size * 4)

    def test_create_exclude_files(self):
        """
        Add directory of files, but exclude one file.
        """
        self._exclude_file(self.cr_test_data, "/TestExperiment/Directory_1/Group_1/file_01.txt")
        self._add_directory(self.cr_test_data, "/TestExperiment/Directory_1")
        response = self.client.post("/rest/v2/datasets", self.cr_test_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)
        self.assert_file_count(response.data, 5)
        self.assert_total_files_byte_size(response.data, self._single_file_byte_size * 5)

    def test_create_exclude_directories(self):
        """
        Add directory of files, but exclude one sub directory.
        """
        self._add_directory(self.cr_test_data, "/TestExperiment/Directory_1")
        self._exclude_directory(self.cr_test_data, "/TestExperiment/Directory_1/Group_1")
        response = self.client.post("/rest/v2/datasets", self.cr_test_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)
        self.assert_file_count(response.data, 4)
        self.assert_total_files_byte_size(response.data, self._single_file_byte_size * 4)

    def test_update_add_and_exclude_files(self):
        """
        Add and excldue files to an existing draft dataset.

        It should be possible to add or exclude files any number of times on a draft dataset.
        """
        cr_id = self._create_draft()

        file_changes = {}

        self._add_file(file_changes, "/TestExperiment/Directory_1/Group_1/file_01.txt")
        response = self.client.post(
            "/rest/v2/datasets/%d/files" % cr_id, file_changes, format="json"
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        self.assertEqual(response.data.get("files_added"), 1, response.data)
        self.assert_file_count(cr_id, 1)

        # executing the same request with same file entry should make no difference
        response = self.client.post(
            "/rest/v2/datasets/%d/files" % cr_id, file_changes, format="json"
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        self.assertEqual(response.data.get("files_added"), 0, response.data)
        self.assert_file_count(cr_id, 1)

        # adding a directory should add files that are not already added
        file_changes = {}

        self._add_directory(file_changes, "/TestExperiment/Directory_1/Group_1")
        response = self.client.post(
            "/rest/v2/datasets/%d/files" % cr_id, file_changes, format="json"
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        self.assertEqual(response.data.get("files_added"), 1, response.data)
        self.assert_file_count(cr_id, 2)

        # add even more files
        file_changes = {}

        self._add_directory(file_changes, "/TestExperiment/Directory_1")
        response = self.client.post(
            "/rest/v2/datasets/%d/files" % cr_id, file_changes, format="json"
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        self.assertEqual(response.data.get("files_added"), 4, response.data)
        self.assert_file_count(cr_id, 6)

        # exclude previously added files from one directroy
        file_changes = {}

        self._exclude_directory(file_changes, "/TestExperiment/Directory_1/Group_1")
        response = self.client.post(
            "/rest/v2/datasets/%d/files" % cr_id, file_changes, format="json"
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        self.assertEqual(response.data.get("files_removed"), 2, response.data)
        self.assert_file_count(cr_id, 4)

        # exclude all previously added files, but keep one file by adding an "add file" entry
        file_changes = {}

        self._add_file(file_changes, "/TestExperiment/Directory_1/Group_2/file_03.txt")
        self._exclude_directory(file_changes, "/TestExperiment")
        response = self.client.post(
            "/rest/v2/datasets/%d/files" % cr_id, file_changes, format="json"
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        self.assertEqual(response.data.get("files_added"), 0, response.data)
        self.assertEqual(response.data.get("files_removed"), 3, response.data)
        self.assert_file_count(cr_id, 1)

    def test_files_can_be_added_once_after_publishing(self):
        """
        First update from 0 to n files should be allowed even for a published dataset, and
        without needing to create new dataset versions, so this is permitted. Subsequent
        file changes will requiree creating a new draft version first.
        """
        response = self.client.post("/rest/v2/datasets", self.cr_test_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)

        cr_id = response.data["id"]

        self.assert_file_count(cr_id, 0)

        file_changes = {}

        self._add_file(file_changes, "/TestExperiment/Directory_1/Group_1/file_02.txt")
        response = self.client.post(
            "/rest/v2/datasets/%d/files" % cr_id, file_changes, format="json"
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        self.assertEqual(response.data.get("files_added"), 1, response.data)

        # try to add a second time. should give an error
        file_changes = {}

        self._add_file(file_changes, "/TestExperiment/Directory_1/Group_1/file_02.txt")
        response = self.client.post(
            "/rest/v2/datasets/%d/files" % cr_id, file_changes, format="json"
        )
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST, response.data)
        self.assertEqual(
            "Changing files of a published dataset" in response.data["detail"][0],
            True,
            response.data,
        )

    def test_prevent_changing_files_on_deprecated_datasets(self):
        cr = CR.objects.get(pk=1)
        cr.deprecated = True
        cr.force_save()

        file_changes = {"files": [{"identifier": "some file"}]}

        response = self.client.post("/rest/v2/datasets/1/files", file_changes, format="json")
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST, response.data)
        self.assertEqual(
            "Changing files of a deprecated" in response.data["detail"][0],
            True,
            response.data,
        )

    def test_prevent_adding_files_with_normal_update(self):
        cr_id = self._create_draft()

        cr = self.client.get(f"/rest/v2/datasets/{cr_id}", format="json").data

        for type in ["files", "directories"]:
            cr["research_dataset"][type] = [
                {"identifier": "pid:urn:%s1" % "" if type == "files" else "dir:"}
            ]

            response = self.client.put(
                f"/rest/v2/datasets/{cr_id}?include_user_metadata", cr, format="json"
            )
            self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
            self.assertEqual(type in response.data["research_dataset"], False, response.data)

            cr["research_dataset"].pop(type)

        # test for published dataset
        self.cr_test_data.pop("files", None)
        self.cr_test_data.pop("directories", None)

        response = self.client.post("/rest/v2/datasets/", self.cr_test_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)

        cr = response.data

        for type in ["files", "directories"]:
            cr["research_dataset"][type] = [
                {"identifier": "pid:urn:%s1" % "" if type == "files" else "dir:"}
            ]

            response = self.client.put(
                f'/rest/v2/datasets/{cr["id"]}?include_user_metadata', cr, format="json"
            )
            self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
            self.assertEqual(type in response.data["research_dataset"], False, response.data)

            cr["research_dataset"].pop(type)

    def test_directory_entries_are_processed_in_order(self):
        """
        Directory entries should executed in the order they are given in the request body.
        """
        # excluding should do nothing, since "add directory" entry is later
        self._exclude_directory(self.cr_test_data, "/TestExperiment/Directory_1/Group_1")
        self._add_directory(self.cr_test_data, "/TestExperiment/Directory_1")
        response = self.client.post("/rest/v2/datasets", self.cr_test_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)
        self.assert_file_count(response.data, 6)

        self.cr_test_data["research_dataset"].pop("directories")

        # exclusion should now have effect, since it is last
        self._add_directory(self.cr_test_data, "/TestExperiment/Directory_1")
        self._exclude_directory(self.cr_test_data, "/TestExperiment/Directory_1/Group_1")
        response = self.client.post("/rest/v2/datasets", self.cr_test_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)
        self.assert_file_count(response.data, 4)

    def test_allow_file_changes_only_on_drafts(self):
        self._add_file(self.cr_test_data, "/TestExperiment/Directory_1/Group_1/file_01.txt")
        response = self.client.post("/rest/v2/datasets", self.cr_test_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)

        file_changes = {}
        self._add_file(file_changes, "/TestExperiment/Directory_1/Group_1/file_02.txt")
        response = self.client.post(
            "/rest/v2/datasets/%d/files" % response.data["id"],
            file_changes,
            format="json",
        )
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST, response.data)
        self.assertEqual(
            "Changing files of a published dataset" in response.data["detail"][0],
            True,
            response.data,
        )

    def test_total_files_byte_size_is_updated_after_adding_files(self):
        # create a draft dataset with zero files
        cr_id = self._create_draft()
        response = self.client.get(f"/rest/v2/datasets/{cr_id}")
        cr_id = response.data["id"]
        self.assertEqual(response.data.get("research_dataset").get("total_files_byte_size"), None)

        # add file to dataset
        file_changes = {}

        self._add_file(file_changes, "/TestExperiment/Directory_1/Group_1/file_01.txt")
        response = self.client.post(
            f"/rest/v2/datasets/{cr_id}/files", file_changes, format="json"
        )
        self.assertEqual(response.data.get("files_added"), 1, response.data)
        self.assert_file_count(cr_id, 1)
        response = self.client.get(f"/rest/v2/datasets/{cr_id}")
        self.assert_total_files_byte_size(response.data, 100)

    def test_total_files_byte_size_field_is_dropped_from_drafts_with_no_files(self):
        # draft with no files/dirs does not have total_files_byte_size field
        cr_id = self._create_draft()
        response = self.client.get(f"/rest/v2/datasets/{cr_id}")
        self.assertEqual(response.data.get("research_dataset").get("total_files_byte_size"), None)

    def test_total_files_byte_size_field_is_dropped_from_datasets_with_no_files(self):
        # dataset with no files/dirs does not have total_files_byte_size field
        response = self.client.post("/rest/v2/datasets", self.cr_test_data, format="json")
        cr_id = response.data["id"]
        response = self.client.get(f"/rest/v2/datasets/{cr_id}")
        self.assertEqual(response.data.get("research_dataset").get("total_files_byte_size"), None)


class CatalogRecordUserMetadata(CatalogRecordApiWriteAssignFilesCommonV2):

    """
    Dataset-specific metadata aka User Metadata related tests.
    """

    def test_retrieve_file_metadata_only(self):

        cr_id = 11

        # retrieve all "user metadata" of adataset
        response = self.client.get(
            "/rest/v2/datasets/%d/files/user_metadata" % cr_id, format="json"
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)

        cr = CR.objects.only("research_dataset").get(pk=cr_id)

        for object_type in ("files", "directories"):
            self.assertEqual(
                len(cr.research_dataset[object_type]),
                len(response.data[object_type]),
                response.data,
            )

        files_and_dirs = response.data

        params = [
            {
                # a pid of single file
                "pid": "pid:urn:2",
                "directory": "false",
                "expect_to_find": True,
            },
            {
                # a pid of a directory
                "pid": "pid:urn:dir:2",
                "directory": "true",
                "expect_to_find": True,
            },
            {
                # a pid of a directory not part of this dataset
                "pid": "should not be found",
                "directory": "true",
                "expect_to_find": False,
            },
        ]

        for p in params:

            # retrieve a single metadata entry
            response = self.client.get(
                "/rest/v2/datasets/%d/files/%s/user_metadata?directory=%s"
                % (cr_id, p["pid"], p["directory"]),
                format="json",
            )
            if p["expect_to_find"] is True:
                self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
                self.assertEqual("identifier" in response.data, True, response.data)
            else:
                self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND, response.data)
                continue

            if p["directory"] == "true":
                object_type = "directories"
            else:
                object_type = "files"

            for obj in files_and_dirs[object_type]:
                if obj["identifier"] == response.data["identifier"]:
                    if p["expect_to_find"] is True:
                        self.assertEqual(obj, response.data, response.data)
                        break
                    else:
                        self.fail("pid %s should not have been found" % p["pid"])
            else:
                if p["expect_to_find"] is True:
                    self.fail(
                        "Retrieved object %s was not found in research_dataset file data?"
                        % p["pid"]
                    )

    def test_dataset_files_schema(self):
        """
        Ensure new schema file dataset_files_schema.json is used.
        """
        response = self.client.post("/rest/v2/datasets", self.cr_test_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)

        cr_id = response.data["id"]

        # use a non-schema field
        file_changes = {}
        self._add_file(
            file_changes,
            "/TestExperiment/Directory_1/Group_1/file_02.txt",
            with_metadata=True,
        )
        file_changes["files"][0]["some_unexpected_file"] = "should raise error"

        response = self.client.post(
            "/rest/v2/datasets/%d/files" % cr_id, file_changes, format="json"
        )
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST, response.data)
        self.assertEqual("is not valid" in str(response.data["detail"][0]), True, response.data)

        # various mandatory fields missing
        for mandatory_field in ("identifier", "use_category"):
            file_changes = {}
            self._add_file(
                file_changes,
                "/TestExperiment/Directory_1/Group_1/file_02.txt",
                with_metadata=True,
            )
            file_changes["files"][0].pop(mandatory_field)

            response = self.client.post(
                "/rest/v2/datasets/%d/files" % cr_id, file_changes, format="json"
            )
            self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST, response.data)
            self.assertEqual("is not valid" in str(response.data["detail"][0]), True, response.data)

    def test_update_metadata_only(self):
        self._add_file(self.cr_test_data, "/TestExperiment/Directory_1/Group_1/file_02.txt")
        response = self.client.post("/rest/v2/datasets", self.cr_test_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)
        self.assertEqual("files" in response.data["research_dataset"], False, response.data)

        cr_id = response.data["id"]

        # add metadata for one file

        file_changes = {}
        self._add_file(
            file_changes,
            "/TestExperiment/Directory_1/Group_1/file_02.txt",
            with_metadata=True,
        )
        file_changes["files"][0]["title"] = "New title"

        response = self.client.put(
            "/rest/v2/datasets/%d/files/user_metadata" % cr_id,
            file_changes,
            format="json",
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        self.assertEqual(
            CR.objects.only("research_dataset").get(pk=cr_id).research_dataset["files"][0]["title"],
            file_changes["files"][0]["title"],
        )

        # add metadata for two directories

        file_changes = {}
        self._add_directory(file_changes, "/TestExperiment", with_metadata=True)
        self._add_directory(file_changes, "/TestExperiment/Directory_1", with_metadata=True)
        file_changes["directories"][0]["title"] = "New dir title"
        file_changes["directories"][0]["description"] = "New dir description"
        file_changes["directories"][1]["title"] = "New dir title 2"
        file_changes["directories"][1]["description"] = "New dir description 2"

        file_count_before = CR.objects.get(pk=cr_id).files.count()

        response = self.client.put(
            "/rest/v2/datasets/%d/files/user_metadata" % cr_id,
            file_changes,
            format="json",
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)

        cr = CR.objects.only("research_dataset").get(pk=cr_id)

        self.assertEqual(
            cr.files.count(),
            file_count_before,
            "operation should only update metadata, but not add files",
        )

        for index in (0, 1):
            for field in ("title", "description"):
                self.assertEqual(
                    cr.research_dataset["directories"][index][field],
                    file_changes["directories"][index][field],
                )

        # update only one field using patch

        file_changes = {}
        self._add_directory(file_changes, "/TestExperiment", with_metadata=True)
        file_changes["directories"][0] = {
            "identifier": file_changes["directories"][0]["identifier"],
            "title": "Changed dir title",
        }

        response = self.client.patch(
            "/rest/v2/datasets/%d/files/user_metadata" % cr_id,
            file_changes,
            format="json",
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        self.assertEqual(
            CR.objects.only("research_dataset")
            .get(pk=cr_id)
            .research_dataset["directories"][0]["title"],
            file_changes["directories"][0]["title"],
        )

        # remove metadata entry. it should be ok that there are normal metadata-addition entries included
        # in the request body too.

        file_changes = {}
        self._add_directory(file_changes, "/TestExperiment/Directory_1/Group_1", with_metadata=True)
        self._add_directory(file_changes, "/TestExperiment")
        file_changes["directories"][-1]["delete"] = True

        entry_to_delete = file_changes["directories"][-1]["identifier"]

        response = self.client.put(
            "/rest/v2/datasets/%d/files/user_metadata" % cr_id,
            file_changes,
            format="json",
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)

        cr = CR.objects.only("research_dataset").get(pk=cr_id)

        self.assertEqual(
            entry_to_delete in [dr["identifier"] for dr in cr.research_dataset["directories"]],
            False,
        )

        # dont allow adding metadata entries for files that are not actually included in the dataset
        file_changes = {}
        self._add_file(
            file_changes,
            "/TestExperiment/Directory_1/Group_2/file_03.txt",
            with_metadata=True,
        )
        non_existing_file = file_changes["files"][-1]["identifier"]

        response = self.client.put(
            "/rest/v2/datasets/%d/files/user_metadata" % cr_id,
            file_changes,
            format="json",
        )
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST, response.data)
        self.assertEqual("are not included" in response.data["detail"][0], True, response.data)
        self.assertEqual(non_existing_file in response.data["data"], True, response.data)

    def test_delete_all_file_meta_data(self):
        # create dataset with file
        self._add_file(self.cr_test_data, "/TestExperiment/Directory_1/Group_1/file_02.txt")
        response = self.client.post("/rest/v2/datasets", self.cr_test_data, format="json")

        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)
        self.assertEqual("files" in response.data["research_dataset"], False, response.data)

        cr_id = response.data["id"]

        # add metadata for one file

        file_changes = {}
        self._add_file(
            file_changes,
            "/TestExperiment/Directory_1/Group_1/file_02.txt",
            with_metadata=True,
        )
        response = self.client.put(
            "/rest/v2/datasets/%d/files/user_metadata" % cr_id,
            file_changes,
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)

        file_data = CR.objects.only("research_dataset").get(pk=cr_id).research_dataset["files"][0]
        file_id = file_data["identifier"]

        # delete data of all files
        file_changes = {"files": []}
        file_changes["files"].append({"delete": True})
        file_changes["files"][0]["identifier"] = file_id

        response = self.client.put(
            "/rest/v2/datasets/%d/files/user_metadata" % cr_id,
            file_changes,
            format="json",
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)

        cr = CR.objects.only("research_dataset").get(pk=cr_id)

        self.assertFalse(cr.research_dataset.get("files", False), "file metadata must not be there")

        # add metadata for one directory

        file_changes = {}
        self._add_directory(file_changes, "/TestExperiment", with_metadata=True)
        response = self.client.put(
            "/rest/v2/datasets/%d/files/user_metadata" % cr_id,
            file_changes,
            format="json",
        )

        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)

        dir_data = (
            CR.objects.only("research_dataset").get(pk=cr_id).research_dataset["directories"][0]
        )

        dir_id = dir_data["identifier"]

        # delete data of all directories
        file_changes = {"directories": []}
        file_changes["directories"].append({"delete": True})
        file_changes["directories"][0]["identifier"] = dir_id

        response = self.client.put(
            "/rest/v2/datasets/%d/files/user_metadata" % cr_id,
            file_changes,
            format="json",
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)

        cr = CR.objects.only("research_dataset").get(pk=cr_id)

        self.assertFalse(
            cr.research_dataset.get("directories", False),
            "directory metadata must not be there",
        )
        self.assertFalse(cr.research_dataset.get("files", False), "file metadata must not be there")


class CatalogRecordFileHandlingCumulativeDatasets(CatalogRecordApiWriteAssignFilesCommonV2):

    """
    Cumulative datasets should allow adding new files to a published dataset,
    but prevent removing files.
    """

    def setUp(self):
        super().setUp()
        self.cr_test_data.pop("files", None)
        self.cr_test_data.pop("directories", None)
        self.cr_test_data["cumulative_state"] = CR.CUMULATIVE_STATE_YES
        self._add_file(self.cr_test_data, "/TestExperiment/Directory_1/file_05.txt")

        response = self.client.post("/rest/v2/datasets", self.cr_test_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)
        self.cr_id = response.data["id"]

    def test_add_files_to_cumulative_dataset(self):
        """
        Adding files to an existing cumulative dataset should be ok.
        """
        file_data = {}
        self._add_file(file_data, "/TestExperiment/Directory_1/file_06.txt")

        response = self.client.post(
            "/rest/v2/datasets/%d/files" % self.cr_id, file_data, format="json"
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        cr = CR.objects.get(pk=self.cr_id)
        self.assertEqual(cr.files.count(), 2)
        self.assertEqual(cr.date_last_cumulative_addition, cr.date_modified)

    def test_exclude_files_from_cumulative_dataset(self):
        """
        Excluding files from an existing cumulative dataset should be prevented.
        """
        file_data = {}
        self._exclude_file(file_data, "/TestExperiment/Directory_1/file_05.txt")

        response = self.client.post(
            "/rest/v2/datasets/%d/files" % self.cr_id, file_data, format="json"
        )
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST, response.data)
        self.assertEqual(
            "Excluding files from a cumulative" in response.data["detail"][0],
            True,
            response.data,
        )
        self.assertEqual(CR.objects.get(pk=self.cr_id).files.count(), 1)

    def test_change_cumulative_states_on_draft(self):
        """
        Ensure changing cumulative states on a new cr draft works as expected.
        """
        response = self.client.post(
            "/rest/v2/datasets?draft=true", self.cr_test_data, format="json"
        )
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)

        cr_id = response.data["id"]

        # set to NO -> should remove all trace of ever being cumulative
        response = self.client.post(
            f"/rpc/v2/datasets/change_cumulative_state?identifier={cr_id}&cumulative_state={CR.CUMULATIVE_STATE_NO}",
            format="json",
        )
        self.assertEqual(response.status_code, status.HTTP_204_NO_CONTENT, response.data)

        # ensure everything related to being cumulative is nuked
        cr = CR.objects.get(pk=cr_id)
        self.assertEqual(cr.cumulative_state, CR.CUMULATIVE_STATE_NO)
        self.assertEqual(cr.date_last_cumulative_addition, None)
        self.assertEqual(cr.date_cumulation_started, None)

        # set back to YES
        response = self.client.post(
            f"/rpc/v2/datasets/change_cumulative_state?identifier={cr_id}&cumulative_state={CR.CUMULATIVE_STATE_YES}",
            format="json",
        )
        self.assertEqual(response.status_code, status.HTTP_204_NO_CONTENT, response.data)

        # check dataset looks cumulative. dates should not be set, since this is still just a draft
        cr = CR.objects.get(pk=cr_id)
        self.assertEqual(cr.cumulative_state, CR.CUMULATIVE_STATE_YES)
        self.assertEqual(cr.date_last_cumulative_addition, None)
        self.assertEqual(cr.date_cumulation_started, None)

        # set back to CLOSED -> does not make sense on a draft
        # note: too long url... yes its ugly
        response = self.client.post(
            "/rpc/v2/datasets/change_cumulative_state"
            f"?identifier={cr_id}&cumulative_state={CR.CUMULATIVE_STATE_CLOSED}",
            format="json",
        )
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST, response.data)
        self.assertEqual(
            "For a new dataset, cumulative_state must be" in response.data["detail"][0],
            True,
            response.data,
        )

    def test_change_cumulative_states_on_published_draft(self):
        """
        Ensure changing cumulative states on a draft of a published record works as expected.
        Essentially, only closing should be possible.
        """

        # create draft of the cumulative dataset
        response = self.client.post(
            f"/rpc/v2/datasets/create_draft?identifier={self.cr_id}", format="json"
        )
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)

        draft_id = response.data["id"]

        # set to NO -> should be prevented
        response = self.client.post(
            "/rpc/v2/datasets/change_cumulative_state"
            f"?identifier={draft_id}&cumulative_state={CR.CUMULATIVE_STATE_NO}",
            format="json",
        )
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST, response.data)
        self.assertTrue(
            "Cumulative dataset cannot be set to non-cumulative" in response.data["detail"][0],
            response.data,
        )

        # set to YES -> should do nothing, since is already cumulative
        response = self.client.post(
            "/rpc/v2/datasets/change_cumulative_state"
            f"?identifier={draft_id}&cumulative_state={CR.CUMULATIVE_STATE_YES}",
            format="json",
        )
        self.assertEqual(response.status_code, status.HTTP_204_NO_CONTENT, response.data)

        # set to CLOSED -> should work
        response = self.client.post(
            "/rpc/v2/datasets/change_cumulative_state"
            f"?identifier={draft_id}&cumulative_state={CR.CUMULATIVE_STATE_CLOSED}",
            format="json",
        )
        self.assertEqual(response.status_code, status.HTTP_204_NO_CONTENT, response.data)

        # merge draft changes
        response = self.client.post(
            f"/rpc/v2/datasets/merge_draft?identifier={draft_id}", format="json"
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)

        # check updates were applied
        cr = CR.objects.get(pk=self.cr_id)
        self.assertEqual(cr.cumulative_state, CR.CUMULATIVE_STATE_CLOSED)

    def test_add_files_to_cumulative_published_draft_dataset(self):
        """
        Adding files to a draft of an existing cumulative dataset should be ok.
        """

        # dataset created in setUp() already has one file in it
        num_files_added = 1

        # create draft of the cumulative dataset
        response = self.client.post(
            "/rpc/v2/datasets/create_draft?identifier=%d" % self.cr_id, format="json"
        )
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)

        draft_id = response.data["id"]

        # add files to draft
        file_data = {}
        self._add_file(file_data, "/TestExperiment/Directory_1/Group_1/file_01.txt")
        self._add_file(file_data, "/TestExperiment/Directory_1/Group_1/file_02.txt")

        num_files_added += len(file_data["files"])

        response = self.client.post(
            "/rest/v2/datasets/%d/files" % draft_id, file_data, format="json"
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)

        # ensure excluding is prevented
        file_data = {}
        self._exclude_file(file_data, "/TestExperiment/Directory_1/Group_1/file_02.txt")
        response = self.client.post(
            "/rest/v2/datasets/%d/files" % draft_id, file_data, format="json"
        )
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST, response.data)

        # ensure files were added to draft
        cr = CR.objects.get(pk=draft_id)
        self.assertEqual(cr.files.count(), num_files_added)

        # ensure published dataset has only the originally added file in it
        cr = CR.objects.get(pk=self.cr_id)
        self.assertEqual(cr.files.count(), 1)

        # merge draft changes
        response = self.client.post(
            "/rpc/v2/datasets/merge_draft?identifier=%d" % draft_id, format="json"
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)

        # published dataset should now have files added
        cr = CR.objects.get(pk=self.cr_id)
        self.assertEqual(cr.files.count(), num_files_added)

        # create another draft
        response = self.client.post(
            "/rpc/v2/datasets/create_draft?identifier=%d" % self.cr_id, format="json"
        )
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)

        draft_id = response.data["id"]

        # close cumulative period on draft
        response = self.client.post(
            "/rpc/v2/datasets/change_cumulative_state"
            f"?identifier={draft_id}&cumulative_state={CR.CUMULATIVE_STATE_CLOSED}",
            format="json",
        )
        self.assertEqual(response.status_code, status.HTTP_204_NO_CONTENT, response.data)

        # merge draft changes again
        response = self.client.post(
            "/rpc/v2/datasets/merge_draft?identifier=%d" % draft_id, format="json"
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)

        # published dataset should now have cumulativity closed
        cr = CR.objects.get(pk=self.cr_id)
        self.assertEqual(cr.cumulative_state, CR.CUMULATIVE_STATE_CLOSED)

    def test_change_preservation_state(self):
        """
        PAS process should not be started while cumulative period is open.
        """
        cr = {"preservation_state": 10}
        response = self.client.patch("/rest/v2/datasets/%s" % self.cr_id, cr, format="json")
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST, response.data)
