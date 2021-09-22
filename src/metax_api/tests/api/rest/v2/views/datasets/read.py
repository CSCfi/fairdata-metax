# This file is part of the Metax API service
#
# Copyright 2017-2018 Ministry of Education and Culture, Finland
#
# :author: CSC - IT Center for Science Ltd., Espoo Finland <servicedesk@csc.fi>
# :license: MIT

from copy import deepcopy

from django.conf import settings
from django.core.management import call_command
from rest_framework import status
from rest_framework.test import APITestCase

from metax_api.models import CatalogRecordV2, File
from metax_api.tests.utils import TestClassUtils, test_data_file_path


class CatalogRecordApiReadCommon(APITestCase, TestClassUtils):
    @classmethod
    def setUpClass(cls):
        """
        Loaded only once for test cases inside this class.
        """
        call_command("loaddata", test_data_file_path, verbosity=0)
        super(CatalogRecordApiReadCommon, cls).setUpClass()

    def setUp(self):
        self.cr_from_test_data = self._get_object_from_test_data("catalogrecord", requested_index=0)
        self.pk = self.cr_from_test_data["id"]
        self.metadata_version_identifier = self.cr_from_test_data["research_dataset"][
            "metadata_version_identifier"
        ]
        self.preferred_identifier = self.cr_from_test_data["research_dataset"][
            "preferred_identifier"
        ]
        self.identifier = self.cr_from_test_data["identifier"]
        self._use_http_authorization()

    def create_legacy_dataset(self):
        cr = deepcopy(self.cr_from_test_data)
        cr["data_catalog"] = settings.LEGACY_CATALOGS[0]
        cr.pop("identifier")
        cr["research_dataset"]["preferred_identifier"] = "ldhkrfdwam"
        cr["research_dataset"].pop("files")
        cr["research_dataset"].pop("total_files_byte_size")
        response = self.client.post("/rest/v2/datasets", cr, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)

        return response.data["id"]


class CatalogRecordApiReadBasicAuthorizationTests(CatalogRecordApiReadCommon):

    """
    Basic read operations from authorization perspective
    """

    api_version = "v2"

    # THE FORBIDDEN TESTS

    def test_no_file_dir_info_for_embargoed_catalog_record_if_available_not_reached_and_no_authorization(
        self,
    ):
        not_available_embargoed_cr_json = (
            self.get_embargoed_cr_with_files_and_dirs_from_api_with_file_details(False)
        )

        # Verify no file and dir info for embargoed cr /rest/v2/datasets/<pk> when embargo date has not
        # been reached without authorization
        self._assert_limited_or_no_file_dir_info(not_available_embargoed_cr_json, "no")

    def _assert_limited_or_no_file_dir_info(self, cr_json, credentials_type):
        self._set_http_authorization(credentials_type)

        file_amt = len(cr_json["research_dataset"]["files"])
        dir_amt = len(cr_json["research_dataset"]["directories"])
        pk = cr_json["id"]

        response = self.client.get(
            "/rest/v2/datasets/{0}?include_user_metadata&file_details".format(pk)
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)

        self.assertEqual(len(response.data["research_dataset"]["files"]), file_amt)
        self.assertEqual(len(response.data["research_dataset"]["directories"]), dir_amt)

        for f in response.data["research_dataset"]["files"]:
            self.assertTrue("details" in f)
            # The below assert is a bit arbitrary
            self.assertFalse("identifier" in f)
        for d in response.data["research_dataset"]["directories"]:
            self.assertTrue("details" in d)
            # The below assert is a bit arbitrary
            self.assertFalse("identifier" in d)


class CatalogRecordApiReadPopulateFileInfoTests(CatalogRecordApiReadCommon):

    """
    Test populating individual research_dataset.file and directory objects with their
    corresponding objects from their db tables.
    """

    def test_file_details_for_deprecated_datasets(self):
        """
        When a dataset is deprecated, it is possible that some of its directories no longer exist.
        Ensure populating file details takes that into account.
        """

        # id 11 is one of the example datasets with full details. they should have a couple
        # of directories attached.
        cr = CatalogRecordV2.objects.get(pk=11)

        file_identifiers = File.objects.filter(
            project_identifier=cr.files.all()[0].project_identifier
        ).values_list("identifier", flat=True)

        response = self.client.delete("/rest/v2/files", data=file_identifiers, format="json")
        self.assertEqual(response.status_code, status.HTTP_200_OK)

        response = self.client.get(
            "/rest/v2/datasets/11?include_user_metadata&file_details", format="json"
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK)


class CatalogRecordApiReadFiles(CatalogRecordApiReadCommon):

    """
    Test /datasets/pid/files api
    """

    def test_get_files_specified_fields_only(self):
        """
        Test use of query parameter ?file_fields=x,y,z
        """
        response = self.client.get("/rest/v2/datasets/1/files?file_fields=identifier,file_path")
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data[0].keys()), 2)
        self.assertEqual("identifier" in response.data[0], True)
        self.assertEqual("file_path" in response.data[0], True)

    def test_removed_query_param(self):
        """
        Test use of query parameter removed_files=bool in /datasets/pid/files, which should return
        only deleted files.
        """
        response = self.client.get("/rest/v2/datasets/1/files")
        file_ids_before = set([f["id"] for f in response.data])
        obj = File.objects.get(pk=1)
        obj.removed = True
        obj.force_save()
        obj2 = File.objects.get(pk=2)
        obj2.removed = True
        obj2.force_save()

        response = self.client.get("/rest/v2/datasets/1/files")
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.content)
        self.assertEqual(len(response.data), 0)

        response = self.client.get("/rest/v2/datasets/1/files?removed_files=true")
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        self.assertEqual(len(response.data), len(file_ids_before))
        self.assertEqual(file_ids_before, set([f["id"] for f in response.data]))


class CatalogRecordApiReadFilesAuthorization(CatalogRecordApiReadCommon):

    """
    Test /datasets/pid/files api from authorization perspective
    """

    # THE FORBIDDEN TESTS

    def test_returns_forbidden_for_embargoed_catalog_record_if_available_not_reached_and_no_authorization(
        self,
    ):
        not_available_embargoed_cr_json = (
            self.get_embargoed_cr_with_files_and_dirs_from_api_with_file_details(False)
        )

        # Verify restricted dataset /rest/v2/datasets/<pk>/files returns forbidden when embargo
        # date has not been reached
        self._assert_forbidden(not_available_embargoed_cr_json, "no")

    def _assert_forbidden(self, cr_json, credentials_type):
        pk = cr_json["id"]
        self._set_http_authorization(credentials_type)
        response = self.client.get("/rest/v2/datasets/{0}/files".format(pk))
        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN)