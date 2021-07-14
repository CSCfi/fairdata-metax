# This file is part of the Metax API service
#
# Copyright 2017-2018 Ministry of Education and Culture, Finland
#
# :author: CSC - IT Center for Science Ltd., Espoo Finland <servicedesk@csc.fi>
# :license: MIT

from copy import deepcopy

from django.conf import settings
from django.core.management import call_command
from django.db.models import Count, Sum
from django.db.models.functions import Coalesce
from rest_framework import status
from rest_framework.test import APITestCase

from metax_api.models import CatalogRecord, DataCatalog, File
from metax_api.models.catalog_record import ACCESS_TYPES
from metax_api.tests.api.rest.base.views.datasets.write import CatalogRecordApiWriteCommon
from metax_api.tests.utils import TestClassUtils, test_data_file_path
from metax_api.utils import parse_timestamp_string_to_tz_aware_datetime


class StatisticRPCCommon(APITestCase, TestClassUtils):
    @classmethod
    def setUpClass(cls):
        """
        Loaded only once for test cases inside this class.
        """
        super().setUpClass()
        call_command("loaddata", test_data_file_path, verbosity=0)

    def setUp(self):
        super().setUp()
        self._use_http_authorization(username="metax")
        self._setup_testdata()

    def _setup_testdata(self):
        test_orgs = ["org_1", "org_2", "org_3"]
        access_types = [v for k, v in ACCESS_TYPES.items()]

        # ensure testdata has something sensible to test against.
        # todo may be feasible to move these to generate_test_data without breaking all tests ?
        # or maybe even shove into fetch_and_update_datasets.py ...
        for cr in CatalogRecord.objects.filter():

            # distribute records between orgs
            if cr.id % 3 == 0:
                cr.metadata_owner_org = test_orgs[2]
            elif cr.id % 2 == 0:
                cr.metadata_owner_org = test_orgs[1]
            else:
                cr.metadata_owner_org = test_orgs[0]

            # distribute records between access types
            if cr.id % 5 == 0:
                cr.research_dataset["access_rights"]["access_type"]["identifier"] = access_types[4]
            elif cr.id % 4 == 0:
                cr.research_dataset["access_rights"]["access_type"]["identifier"] = access_types[3]
            elif cr.id % 3 == 0:
                cr.research_dataset["access_rights"]["access_type"]["identifier"] = access_types[2]
            elif cr.id % 2 == 0:
                cr.research_dataset["access_rights"]["access_type"]["identifier"] = access_types[1]
            else:
                cr.research_dataset["access_rights"]["access_type"]["identifier"] = access_types[0]

            # distribute records between some creation months
            date = "20%s-%s-13"
            if cr.id % 8 == 0:
                cr.date_created = parse_timestamp_string_to_tz_aware_datetime(date % ("18", "06"))
            elif cr.id % 7 == 0:
                cr.date_created = parse_timestamp_string_to_tz_aware_datetime(date % ("18", "07"))
            elif cr.id % 6 == 0:
                cr.date_created = parse_timestamp_string_to_tz_aware_datetime(date % ("18", "10"))
            elif cr.id % 5 == 0:
                cr.date_created = parse_timestamp_string_to_tz_aware_datetime(date % ("18", "11"))
            elif cr.id % 4 == 0:
                cr.date_created = parse_timestamp_string_to_tz_aware_datetime(date % ("18", "12"))
            elif cr.id % 3 == 0:
                cr.date_created = parse_timestamp_string_to_tz_aware_datetime(date % ("19", "01"))
            elif cr.id % 2 == 0:
                cr.date_created = parse_timestamp_string_to_tz_aware_datetime(date % ("19", "02"))
            else:
                cr.date_created = parse_timestamp_string_to_tz_aware_datetime(date % ("19", "03"))

            # set some records as "created through end user api"
            if cr.id % 10 == 0:
                cr.service_created = None
                cr.user_created = "abc%d@fairdataid" % cr.id

            cr.force_save()

        # create a few files which do not belong to any datasets
        response = self.client.get("/rest/v2/files/1", format="json")

        files = []

        for i in range(5):
            f = deepcopy(response.data)
            del f["id"]
            f["identifier"] += "unique" + str(i)
            f["file_name"] += str(i)
            f["file_path"] += str(i)
            f["project_identifier"] = "prj123"
            files.append(f)

        response = self.client.post("/rest/v2/files", files, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)
        response = self.client.get(
            "/rest/v2/directories/update_byte_sizes_and_file_counts", format="json"
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        response = self.client.get(
            "/rest/v2/datasets/update_cr_total_files_byte_sizes", format="json"
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)

        # create legacy datacatalog
        dc = DataCatalog.objects.filter(catalog_json__research_dataset_schema="ida").first()
        dc.catalog_json["identifier"] = settings.LEGACY_CATALOGS[0]
        dc_json = {
            "catalog_record_services_create": "testuser,api_auth_user,metax",
            "catalog_record_services_edit": "testuser,api_auth_user,metax",
            "catalog_json": dc.catalog_json,
        }
        response = self.client.post("/rest/v2/datacatalogs", dc_json, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)

    def _create_new_dataset_version(self, id=1):
        """
        Finds the latest version of given dataset, and creates a new version of it.
        """
        response = self.client.get(f"/rest/v2/datasets/{id}?include_legacy", format="json")
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        cr = response.data
        while cr.get("next_dataset_version", False):
            id = cr["next_dataset_version"]["id"]
            response = self.client.get(f"/rest/v2/datasets/{id}?include_legacy", format="json")
            self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
            cr = response.data

        response = self.client.post(
            f'/rpc/v2/datasets/create_new_version?identifier={cr["id"]}', format="json"
        )
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)
        new_version_id = response.data["id"]

        response = self.client.post(
            f"/rpc/v2/datasets/publish_dataset?identifier={new_version_id}",
            format="json",
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)

        return new_version_id

    def _get_dataset_count_after(self, date):
        """
        Return the total count after the date provided (inclusive). date is in format 'YYYY-MM-DD'
        """
        return CatalogRecord.objects_unfiltered.filter(
            date_created__gte=f"{date}T00:00:00+03:00"
        ).count()

    def _get_dataset_count_of_month(self, date):
        """
        Returns the count for given date. date is in format 'YYYY-MM'
        """
        return CatalogRecord.objects_unfiltered.filter(date_created__startswith=date).count()


class StatisticRPCCountDatasets(StatisticRPCCommon, CatalogRecordApiWriteCommon):
    """
    Test suite for count_datasets api. So far tests only parameters removed, legacy, latest and
    combinations.
    """

    def test_count_datasets_from_date(self):
        total_count = CatalogRecord.objects_unfiltered.count()

        june_count = self._get_dataset_count_of_month("2018-06")
        july_count = self._get_dataset_count_of_month("2018-07")
        aug_count = self._get_dataset_count_of_month("2018-08")

        res = self.client.get("/rpc/v2/statistics/count_datasets?from_date=2018-07-01").data
        self.assertEqual(res["count"], total_count - june_count)

        res = self.client.get("/rpc/v2/statistics/count_datasets?from_date=2018-09-02").data
        self.assertEqual(res["count"], total_count - june_count - july_count - aug_count)

    def test_count_datasets_to_date(self):
        total_count = CatalogRecord.objects_unfiltered.count()

        after_jan_count = self._get_dataset_count_after("2019-01-02")
        after_feb_count = self._get_dataset_count_after("2019-02-02")

        res = self.client.get("/rpc/v2/statistics/count_datasets?to_date=2019-01-01").data
        self.assertEqual(res["count"], total_count - after_jan_count)

        res = self.client.get("/rpc/v2/statistics/count_datasets?to_date=2019-02-01").data
        self.assertEqual(res["count"], total_count - after_feb_count)