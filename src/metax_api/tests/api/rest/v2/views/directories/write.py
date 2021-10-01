# This file is part of the Metax API service
#
# Copyright 2017-2018 Ministry of Education and Culture, Finland
#
# :author: CSC - IT Center for Science Ltd., Espoo Finland <servicedesk@csc.fi>
# :license: MIT

from django.core.management import call_command
from rest_framework import status
from rest_framework.test import APITestCase

from metax_api.tests.utils import TestClassUtils, test_data_file_path

d = print


class DirectoryApiWriteCommon(APITestCase, TestClassUtils):
    @classmethod
    def setUpClass(cls):
        """
        Loaded only once for test cases inside this class.
        """
        call_command("loaddata", test_data_file_path, verbosity=0)
        super(DirectoryApiWriteCommon, cls).setUpClass()

    def setUp(self):
        """
        Reloaded for every test case
        """
        call_command("loaddata", test_data_file_path, verbosity=0)
        dir_from_test_data = self._get_object_from_test_data("directory")
        self.identifier = dir_from_test_data["identifier"]
        self.directory_name = dir_from_test_data["directory_name"]

        """
        New data that is sent to the server for POST, PUT, PATCH requests. Modified
        slightly as approriate for different purposes
        """
        self.test_new_data = self._get_new_test_data()
        self.second_test_new_data = self._get_second_new_test_data()
        self._use_http_authorization()

    def _get_new_test_data(self):
        from_test_data = self._get_object_from_test_data("directory", requested_index=0)
        from_test_data.update(
            {
                "identifier": "urn:nbn:fi:csc-ida201401200000000001",
            }
        )
        return from_test_data

    def _get_second_new_test_data(self):
        from_test_data = self._get_new_test_data()
        from_test_data.update(
            {
                "identifier": "urn:nbn:fi:csc-ida201401200000000002",
            }
        )
        return from_test_data


class DirectoryApiWriteTests(DirectoryApiWriteCommon):
    def test_create_files_for_catalog_record(self):
        """
        Tests flow of creating files and assigning them to dataset.
        """

        project = "project-test-files"
        directory_path = "/dir/"
        files = []
        for n in range(1, 4):
            file_path = directory_path + "file" + str(n)
            f = self._get_new_file_data(
                str(n),
                project=project,
                file_path=file_path,
                directory_path=directory_path,
                open_access=True,
            )
            f.pop("parent_directory", None)
            files.append(f)

        cr = self._get_ida_dataset_without_files()

        fields = "file_fields=id,identifier,file_path&directory_fields=id,identifier,directory_path,file_count"

        # start test #
        self._set_http_authorization("service")

        # adding file1 to /dir/
        response = self.client.post("/rest/v2/files", files[0], format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)

        file1_id = response.data["identifier"]

        # adding file2 to /dir/
        response = self.client.post("/rest/v2/files", files[1], format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)

        file2_id = response.data["identifier"]

        # adding file3 to /dir/
        response = self.client.post("/rest/v2/files", files[2], format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)

        file3_id = response.data["identifier"]

        # creating dataset
        response = self.client.post("/rest/v2/datasets?draft=true", cr, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)

        cr_id = response.data["id"]

        # getting dataset root directory identifier (%2F=='/')
        root_dir = self.client.get(
            "/rest/v2/directories/files?project={}&path=%2F&include_parent".format(project)
        )
        root_id = root_dir.data["id"]

        # getting dataset files from /
        response = self.client.get(
            "/rest/v2/directories/files?cr_identifier={}&project={}&path=%2F&{}".format(
                cr_id, project, fields
            )
        )
        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND, "Directory must be empty")

        # adding file1 to dataset
        first_file = {"files": [{"identifier": file1_id}]}

        response = self.client.post(
            "/rest/v2/datasets/{}/files".format(cr_id), first_file, format="json"
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)

        # getting dataset files from /
        response = self.client.get(
            "/rest/v2/directories/{}/files?cr_identifier={}&fields".format(root_id, cr_id)
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        dirs = response.data["directories"]
        self.assertEqual(len(dirs), 1, "Expected 1 directory")
        self.assertEqual(
            dirs[0]["file_count"],
            1,
            "Expected 1 file in directory %s" % dirs[0]["directory_path"],
        )

        # getting dataset files from /dir/
        response = self.client.get(
            "/rest/v2/directories/{}/files?cr_identifier={}&include_parent&{}".format(
                dirs[0]["id"], cr_id, fields
            )
        )
        self.assertEqual(
            len(response.data["files"]),
            1,
            "Expected 1 file in directory {}".format(dirs[0]["directory_path"]),
        )
        self.assertEqual(
            response.data["file_count"],
            len(response.data["files"]),
            "Expected 1 file in parent file_count",
        )

        # getting non-dataset files from /dir/
        response = self.client.get(
            "/rest/v2/directories/{}/files?not_cr_identifier={}&include_parent&{}".format(
                dirs[0]["id"], cr_id, fields
            )
        )
        self.assertEqual(
            len(response.data["files"]),
            2,
            "Expected 2 files in directory {}".format(dirs[0]["directory_path"]),
        )
        self.assertEqual(
            response.data["file_count"],
            len(response.data["files"]),
            "Expected 2 file in parent file_count",
        )

        # adding file2 and file3 to dataset

        last_files = {"files": [{"identifier": file2_id}, {"identifier": file3_id}]}

        response = self.client.post(
            "/rest/v2/datasets/{}/files".format(cr_id), last_files, format="json"
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)

        # getting dataset files from /dir/
        response = self.client.get(
            "/rest/v2/directories/{}/files?cr_identifier={}&include_parent&{}".format(
                dirs[0]["id"], cr_id, fields
            )
        )
        self.assertEqual(
            len(response.data["files"]),
            3,
            "Expected 3 files in directory {}".format(dirs[0]["directory_path"]),
        )
        self.assertEqual(
            response.data["file_count"],
            len(response.data["files"]),
            "Expected 3 file in parent file_count",
        )

        # getting non-dataset files from /dir/
        response = self.client.get(
            "/rest/v2/directories/{}/files?not_cr_identifier={}&include_parent&{}".format(
                dirs[0]["id"], cr_id, fields
            )
        )
        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND, "Directory must be empty")

        # getting dataset files from /
        response = self.client.get(
            "/rest/v2/directories/{}/files?cr_identifier={}&fields".format(root_id, cr_id)
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK)

        dirs = response.data["directories"]
        self.assertEqual(len(dirs), 1, "Expected 1 directory")
        self.assertEqual(
            dirs[0]["file_count"],
            3,
            "Expected 3 files in directory %s" % dirs[0]["directory_path"],
        )

        # getting dataset files from /
        response = self.client.get(
            "/rest/v2/directories/{}/files?not_cr_identifier={}&fields".format(root_id, cr_id)
        )
        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND, "Directory must be empty")
