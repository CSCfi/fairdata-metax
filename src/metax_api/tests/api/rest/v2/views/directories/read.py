# This file is part of the Metax API service
#
# Copyright 2017-2018 Ministry of Education and Culture, Finland
#
# :author: CSC - IT Center for Science Ltd., Espoo Finland <servicedesk@csc.fi>
# :license: MIT
import copy

from django.core.management import call_command
from django.db.models import Count, Sum
from rest_framework import status
from rest_framework.test import APITestCase

from metax_api.models import CatalogRecord, Directory
from metax_api.tests.utils import TestClassUtils, test_data_file_path


class DirectoryApiReadCommon(APITestCase, TestClassUtils):
    @classmethod
    def setUpClass(cls):
        """
        Loaded only once for test cases inside this class.
        """
        call_command("loaddata", test_data_file_path, verbosity=0)
        super(DirectoryApiReadCommon, cls).setUpClass()

    def setUp(self):
        dir_from_test_data = self._get_object_from_test_data("directory")
        self.identifier = dir_from_test_data["identifier"]
        self.pk = dir_from_test_data["id"]
        self._use_http_authorization()


class DirectoryApiReadCatalogRecordFileBrowsingTests(DirectoryApiReadCommon):

    """
    Test browsing files in the context of a specific CatalogRecord. Should always
    only dispaly those files that were selected for that CR, and only those dirs,
    that contained suchs files, or would contain such files further down the tree.
    """

    def setUp(self):
        self._set_http_authorization("service")
        self.client.get("/rest/v2/directories/update_byte_sizes_and_file_counts")
        self.client.get("/rest/v2/datasets/update_cr_directory_browsing_data")

    def test_directory_byte_size_and_file_count(self):
        """
        Test byte size and file count are calculated correctly for directories when browsing files
        in the context of a single record.
        """

        def _assert_dir_calculations(cr, dr):
            """
            Assert directory numbers received from browsing-api matches what exists in the db when
            making a reasonably fool-proof query of files by directory path
            """
            self.assertEqual("byte_size" in dr, True)
            self.assertEqual("file_count" in dr, True)

            byte_size = cr.files.filter(
                file_path__startswith="%s/" % dr["directory_path"]
            ).aggregate(Sum("byte_size"))["byte_size__sum"]

            file_count = cr.files.filter(file_path__startswith="%s/" % dr["directory_path"]).count()

            self.assertEqual(dr["byte_size"], byte_size, "path: %s" % dr["directory_path"])
            self.assertEqual(dr["file_count"], file_count, "path: %s" % dr["directory_path"])

        # prepare a new test dataset which contains a directory from testdata, which contains a decent
        # qty of files and complexity
        dr = Directory.objects.get(directory_path="/prj_112_root")
        cr_data = self.client.get("/rest/v2/datasets/1?include_user_metadata").data
        cr_data.pop("id")
        cr_data.pop("identifier")
        cr_data["research_dataset"].pop("preferred_identifier")
        cr_data["research_dataset"].pop("files", None)
        cr_data["research_dataset"]["directories"] = [
            {
                "identifier": dr.identifier,
                "title": "test dir",
                "use_category": {"identifier": "outcome"},
            }
        ]
        self._use_http_authorization(username="metax")
        response = self.client.post("/rest/v2/datasets", cr_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)
        cr_data = response.data
        cr = CatalogRecord.objects.get(pk=cr_data["id"])

        # begin tests

        # test: browse the file api, and receive a list of sub-directories
        response = self.client.get(
            "/rest/v2/directories/%d/files?cr_identifier=%s" % (dr.id, cr.identifier)
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)

        for directory in response.data["directories"]:
            _assert_dir_calculations(cr, directory)

        # test: browse with ?include_parent=true to get the dir directly that was added to the dataset
        response = self.client.get(
            "/rest/v2/directories/%d/files?cr_identifier=%s&include_parent" % (dr.id, cr.identifier)
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        _assert_dir_calculations(cr, response.data)

    def test_directory_byte_size_and_file_count_in_parent_directories(self):
        cr_id = 13

        def _assertDirectoryData(id, parent_data):
            response = self.client.get(
                "/rest/v2/directories/%d/files?cr_identifier=%d&include_parent" % (id, cr_id)
            )

            self.assertEqual(response.data["byte_size"], parent_data[id][0], response.data["id"])
            self.assertEqual(response.data["file_count"], parent_data[id][1])

            if response.data.get("parent_directory"):
                return _assertDirectoryData(response.data["parent_directory"]["id"], parent_data)

        def _assertDirectoryData_not_cr_id(id, parent_data):
            total_dir_res = self.client.get("/rest/v2/directories/%d" % id)
            total_dir_size = (
                total_dir_res.data.get("byte_size", None),
                total_dir_res.data.get("file_count", None),
            )

            response = self.client.get(
                "/rest/v2/directories/%s/files?not_cr_identifier=%d&include_parent" % (id, cr_id)
            )
            if response.data.get("id"):
                self.assertEqual(response.data["byte_size"], total_dir_size[0] - parent_data[id][0])
                self.assertEqual(
                    response.data["file_count"], total_dir_size[1] - parent_data[id][1]
                )

            if response.data.get("parent_directory", None):
                return _assertDirectoryData_not_cr_id(
                    response.data["parent_directory"]["id"], parent_data
                )

        def _get_parent_dir_data_for_cr(id):
            def _get_parents(pk):
                pk = Directory.objects.get(pk=pk).parent_directory_id
                if pk:
                    pks.append(pk)
                    _get_parents(pk)

            cr_data = (
                CatalogRecord.objects.get(pk=id)
                .files.order_by("parent_directory_id")
                .values_list("parent_directory_id")
                .annotate(Sum("byte_size"), Count("id"))
            )
            grouped_cr_data = {
                parent_id: [byte_size, file_count] for parent_id, byte_size, file_count in cr_data
            }

            drs = {}
            for dir in grouped_cr_data.keys():
                pks = []
                _get_parents(dir)
                drs[dir] = pks

            for key, value in drs.items():
                for v in value:
                    if grouped_cr_data.get(v):
                        grouped_cr_data[v][0] += grouped_cr_data[key][0]
                        grouped_cr_data[v][1] += grouped_cr_data[key][1]
                    else:
                        grouped_cr_data[v] = copy.deepcopy(grouped_cr_data[key])

            return grouped_cr_data

        # begin tests

        cr = self.client.get(
            "/rest/v2/datasets/%d?include_user_metadata&file_details&"
            "directory_fields=id,byte_size,file_count,parent_directory&"
            "file_fields=id,byte_size,parent_directory" % cr_id
        )

        dirs = [d["details"]["id"] for d in cr.data["research_dataset"].get("directories", [])] + [
            f["details"]["parent_directory"]["id"]
            for f in cr.data["research_dataset"].get("files", [])
        ]

        parent_data = _get_parent_dir_data_for_cr(cr_id)

        for id in set(dirs):
            _assertDirectoryData(id, parent_data)
            _assertDirectoryData_not_cr_id(id, parent_data)