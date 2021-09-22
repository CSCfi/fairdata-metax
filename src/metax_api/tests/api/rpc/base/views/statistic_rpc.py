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

from metax_api.models import CatalogRecord, DataCatalog
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
        response = self.client.get("/rest/files/1", format="json")

        files = []

        for i in range(5):
            f = deepcopy(response.data)
            del f["id"]
            f["identifier"] += "unique" + str(i)
            f["file_name"] += str(i)
            f["file_path"] += str(i)
            f["project_identifier"] = "prj123"
            files.append(f)

        response = self.client.post("/rest/files", files, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)
        response = self.client.get(
            "/rest/directories/update_byte_sizes_and_file_counts", format="json"
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        response = self.client.get("/rest/datasets/update_cr_total_files_byte_sizes", format="json")
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)

        # create legacy datacatalog
        dc = DataCatalog.objects.filter(catalog_json__research_dataset_schema="ida").first()
        dc.catalog_json["identifier"] = settings.LEGACY_CATALOGS[0]
        dc_json = {
            "catalog_record_services_create": "testuser,api_auth_user,metax",
            "catalog_record_services_edit": "testuser,api_auth_user,metax",
            "catalog_json": dc.catalog_json,
        }
        response = self.client.post("/rest/datacatalogs", dc_json, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)

    def _set_deprecated_dataset(self, id=1):
        cr = CatalogRecord.objects.get(id=id)
        cr.deprecated = True
        cr.force_save()

    def _set_removed_dataset(self, id=1):
        cr = CatalogRecord.objects.get(id=id)
        cr.removed = True
        cr.force_save()

    def _create_legacy_dataset(self):
        """
        Creates one new legacy dataset and returns its id
        """
        legacy_dataset = {
            "data_catalog": {"identifier": settings.LEGACY_CATALOGS[0]},
            "metadata_owner_org": "some_org_id",
            "metadata_provider_org": "some_org_id",
            "metadata_provider_user": "some_user_id",
            "research_dataset": {
                "title": {"en": "Test Dataset Title"},
                "description": {
                    "en": "A descriptive description describing the contents of this dataset. Must be descriptive."
                },
                "files": [
                    {
                        "identifier": "pid:urn:1",
                        "title": "File Title",
                        "description": "informative description",
                        "use_category": {"identifier": "method"},
                    },
                    {
                        "identifier": "pid:urn:3",
                        "title": "File Title",
                        "description": "informative description",
                        "use_category": {"identifier": "method"},
                    },
                ],
                "creator": [
                    {
                        "name": "Teppo Testaaja",
                        "@type": "Person",
                        "member_of": {
                            "name": {"fi": "Testiorganisaatio"},
                            "@type": "Organization",
                        },
                    }
                ],
                "access_rights": {
                    "access_type": {
                        "identifier": "http://uri.suomi.fi/codelist/fairdata/access_type/code/open",
                        "pref_label": {"fi": "Avoin", "en": "Open", "und": "Avoin"},
                        "in_scheme": "http://uri.suomi.fi/codelist/fairdata/access_type",
                    }
                },
                "preferred_identifier": "uniikkinen_aidentifaijeri",
            },
        }
        response = self.client.post("/rest/datasets", legacy_dataset, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)

        return response.data["id"]

    def _create_new_dataset(self, dataset_json):
        response = self.client.post("/rest/datasets", dataset_json, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)
        new_cr_id = response.data["id"]

        return new_cr_id

    def _create_new_dataset_version(self, id=1):
        """
        Finds the latest version of given dataset, deletes one file from it and updates.
        Does not check if there are files to be deleted.
        """
        cr = self.client.get(f"/rest/datasets/{id}?include_legacy", format="json").data
        while cr.get("next_dataset_version", False):
            id = cr["next_dataset_version"]["id"]
            cr = self.client.get(f"/rest/datasets/{id}?include_legacy", format="json").data

        cr["research_dataset"]["files"].pop()
        response = self.client.put(f"/rest/datasets/{id}?include_legacy", cr, format="json")
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)

        return response.data["next_dataset_version"]["id"]

    def _set_dataset_creation_date(self, cr_id, date):
        """
        Forcibly changes the creation date for easier testing. Date-parameter is in form 'YYYY-MM-DD'
        """

        cr = CatalogRecord.objects_unfiltered.get(id=cr_id)
        cr.date_created = parse_timestamp_string_to_tz_aware_datetime(date)
        cr.force_save()

    def _get_catalog_record_size(self, id):
        """
        Returns the size of given record id
        """
        size = CatalogRecord.objects_unfiltered.get(id=id).research_dataset.get(
            "total_files_byte_size", 0
        )

        return size

    def _get_byte_size_of_month(self, date):
        """
        Returns the byte size for given date. date is in format 'YYYY-MM'
        """
        query = CatalogRecord.objects_unfiltered.filter(date_created__startswith=date)
        list_of_sizes = [cr.research_dataset.get("total_files_byte_size", 0) for cr in query]

        return sum(list_of_sizes)

    def _get_total_byte_size(self):
        """
        Returns byte size of all datasets in database
        """
        query = CatalogRecord.objects_unfiltered.all()
        list_of_sizes = [cr.research_dataset.get("total_files_byte_size", 0) for cr in query]

        return sum(list_of_sizes)

    def _get_dataset_count_of_month(self, date):
        """
        Returns the count for given date. date is in format 'YYYY-MM'
        """
        return CatalogRecord.objects_unfiltered.filter(date_created__startswith=date).count()

    def _get_dataset_count_after(self, date):
        """
        Return the total count after the date provided (inclusive). date is in format 'YYYY-MM-DD'
        """
        return CatalogRecord.objects_unfiltered.filter(
            date_created__gte=f"{date}T00:00:00+03:00"
        ).count()

    def _get_total_dataset_count(self):
        """
        Returns dataset count of entire database
        """
        return CatalogRecord.objects_unfiltered.count()

    def _set_cr_datacatalog(self, cr_id, catalog_id):
        cr = CatalogRecord.objects.get(pk=cr_id)
        cr.data_catalog_id = DataCatalog.objects.get(catalog_json__identifier=catalog_id).id
        cr.force_save()

    def _set_dataset_as_draft(self, cr_id):
        cr = CatalogRecord.objects.get(pk=cr_id)
        cr.state = "draft"
        cr.force_save()

    def _set_cr_organization(self, cr_id, org):
        cr = CatalogRecord.objects.get(pk=cr_id)
        cr.metadata_owner_org = org
        cr.force_save()


class StatisticRPCCountDatasets(StatisticRPCCommon, CatalogRecordApiWriteCommon):
    """
    Test suite for count_datasets api. So far tests only parameters removed, legacy, latest and
    combinations.
    """

    def test_count_datasets_single(self):
        """
        Tests single parameters for api. Empty removed and legacy parameters returns true AND false matches
        """
        total_count = CatalogRecord.objects_unfiltered.count()
        response = self.client.get("/rpc/statistics/count_datasets").data
        self.assertEqual(total_count, response["count"], response)

        # test removed -parameter
        self._set_removed_dataset(id=2)
        response = self.client.get("/rpc/statistics/count_datasets?removed=true").data
        self.assertEqual(response["count"], 1, response)

        response = self.client.get("/rpc/statistics/count_datasets?removed=false").data
        self.assertEqual(response["count"], total_count - 1, response)

        # test legacy -parameter
        self._create_legacy_dataset()
        total_count = CatalogRecord.objects_unfiltered.count()
        response = self.client.get("/rpc/statistics/count_datasets?legacy=true").data
        self.assertEqual(response["count"], 1, response)

        response = self.client.get("/rpc/statistics/count_datasets?legacy=false").data
        self.assertEqual(response["count"], total_count - 1, response)

        # test latest -parameter
        self._create_new_dataset_version()
        self._create_new_dataset_version()
        total_count = CatalogRecord.objects_unfiltered.count()
        response = self.client.get(
            "/rpc/statistics/count_datasets?latest=false"
        ).data  # returns all
        self.assertEqual(response["count"], total_count, response)

        with_param = self.client.get("/rpc/statistics/count_datasets?latest=true").data
        without_param = self.client.get("/rpc/statistics/count_datasets").data  # default is true
        self.assertEqual(with_param["count"], total_count - 2, with_param)
        self.assertEqual(with_param["count"], without_param["count"], with_param)

    def test_count_datasets_removed_latest(self):
        second_ver = self._create_new_dataset_version()
        self._create_new_dataset_version(second_ver)
        self._set_removed_dataset()
        self._set_removed_dataset(id=second_ver)
        self._set_removed_dataset(id=2)

        rem_lat = self.client.get("/rpc/statistics/count_datasets?removed=true&latest=true").data
        rem_not_lat = self.client.get(
            "/rpc/statistics/count_datasets?removed=true&latest=false"
        ).data

        self.assertEqual(rem_lat["count"], 1, "Only latest versions should be checked")  # id=2
        self.assertEqual(rem_not_lat["count"], 3, "Only the prev versions should be removed")

        # create new dataset with 2 versions
        response = self.client.post("/rest/datasets", self.cr_test_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)
        self._create_new_dataset_version(response.data["id"])

        not_rem_lat = self.client.get(
            "/rpc/statistics/count_datasets?removed=false&latest=true"
        ).data
        not_rem_not_lat = self.client.get(
            "/rpc/statistics/count_datasets?removed=false&latest=false"
        ).data

        self.assertEqual(not_rem_lat["count"], not_rem_not_lat["count"] - 1)

    def test_count_datasets_removed_legacy(self):
        self._create_legacy_dataset()
        self._create_legacy_dataset()
        leg_cr = self._create_legacy_dataset()
        self._set_removed_dataset(leg_cr)
        total_count = CatalogRecord.objects_unfiltered.count()

        rem_leg = self.client.get("/rpc/statistics/count_datasets?removed=true&legacy=true").data
        rem_not_leg = self.client.get(
            "/rpc/statistics/count_datasets?removed=true&legacy=false"
        ).data
        not_rem_leg = self.client.get(
            "/rpc/statistics/count_datasets?removed=false&legacy=true"
        ).data
        not_rem_not_leg = self.client.get(
            "/rpc/statistics/count_datasets?removed=false&legacy=false"
        ).data

        self.assertEqual(rem_leg["count"], 1)
        self.assertEqual(rem_not_leg["count"], 0)
        self.assertEqual(not_rem_leg["count"], 2)
        self.assertEqual(not_rem_not_leg["count"], total_count - 3)

    def test_count_datasets_latest_legacy(self):
        leg_cr = self._create_legacy_dataset()
        self._create_new_dataset_version(leg_cr)
        self._create_new_dataset_version(leg_cr)
        total_count = CatalogRecord.objects_unfiltered.count()

        leg_lat = self.client.get("/rpc/statistics/count_datasets?legacy=true&latest=true").data
        leg_not_lat = self.client.get(
            "/rpc/statistics/count_datasets?legacy=true&latest=false"
        ).data
        not_leg_not_lat = self.client.get(
            "/rpc/statistics/count_datasets?legacy=false&latest=false"
        ).data

        self.assertEqual(leg_lat["count"], 1)
        self.assertEqual(leg_not_lat["count"], 3)
        self.assertEqual(not_leg_not_lat["count"], total_count - 3)

    def test_count_datasets_from_date(self):
        total_count = CatalogRecord.objects_unfiltered.count()

        june_count = self._get_dataset_count_of_month("2018-06")
        july_count = self._get_dataset_count_of_month("2018-07")

        res = self.client.get("/rpc/statistics/count_datasets?from_date=2018-07-01").data
        self.assertEqual(res["count"], total_count - june_count)

        # datasets are created on 13th so this should include august count
        res = self.client.get("/rpc/statistics/count_datasets?from_date=2018-08-13").data
        self.assertEqual(res["count"], total_count - june_count - july_count)

    def test_count_datasets_to_date(self):
        total_count = CatalogRecord.objects_unfiltered.count()

        after_jan_count = self._get_dataset_count_after("2019-01-01")
        after_feb_count = self._get_dataset_count_after("2019-02-14")

        res = self.client.get("/rpc/statistics/count_datasets?to_date=2019-01-01").data
        self.assertEqual(res["count"], total_count - after_jan_count)

        res = self.client.get("/rpc/statistics/count_datasets?to_date=2019-02-13").data
        self.assertEqual(res["count"], total_count - after_feb_count)


class StatisticRPCAllDatasetsCumulative(StatisticRPCCommon, CatalogRecordApiWriteCommon):
    """
    Test suite for all_datasets_cumulative. Test only optional parameters removed, legacy and latest for now.
    """

    url = "/rpc/statistics/all_datasets_cumulative"
    dateparam_all = "from_date=2018-06&to_date=2019-03"

    def test_all_datasets_cumulative(self):
        """
        Basic tests for all_datasets_cumulative including parameter checks and basic functionality.

        Return values for each interval:
        count: Number of datasets created in this month
        ida_byte_size: size of all files in datasets created this month
        count_cumulative: number of datasets from from_date to this month (including)
        ida_byte_size_cumulative: size of all files in datasets created from from_date to this month (including)
        """

        # test bad query parameters
        response = self.client.get(f"{self.url}")
        self.assertEqual(
            response.status_code,
            status.HTTP_400_BAD_REQUEST,
            "from_date and to_date are required",
        )

        response = self.client.get(f"{self.url}?from_date=2019-11&to_date=bad_parameter")
        self.assertEqual(
            response.status_code, status.HTTP_400_BAD_REQUEST, "date format is YYYY-MM"
        )

        response = self.client.get(f"{self.url}?from_date=2019-11&to_date=2019-11-15")
        self.assertEqual(
            response.status_code, status.HTTP_400_BAD_REQUEST, "date format is YYYY-MM"
        )

        # test the basic functionality
        june_size = self._get_byte_size_of_month("2018-06")
        june_count = self._get_dataset_count_of_month("2018-06")

        july_size = self._get_byte_size_of_month("2018-07")
        july_count = self._get_dataset_count_of_month("2018-07")

        march_size = self._get_byte_size_of_month("2019-03")
        march_count = self._get_dataset_count_of_month("2019-03")

        total_count = self._get_total_dataset_count()
        total_size = self._get_total_byte_size()

        response = self.client.get(f"{self.url}?{self.dateparam_all}").data
        # ensure the counts and byte sizes are calculated correctly
        self.assertEqual(response[0]["count"], june_count, response)
        self.assertEqual(response[0]["ida_byte_size"], june_size, response)
        self.assertEqual(response[0]["count_cumulative"], june_count, response)
        self.assertEqual(response[0]["ida_byte_size_cumulative"], june_size, response)

        self.assertEqual(response[1]["count"], july_count, response)
        self.assertEqual(response[1]["ida_byte_size"], july_size, response)
        self.assertEqual(response[1]["count_cumulative"], june_count + july_count, response)
        self.assertEqual(response[1]["ida_byte_size_cumulative"], june_size + july_size, response)

        self.assertEqual(response[-1]["count"], march_count, response)
        self.assertEqual(response[-1]["ida_byte_size"], march_size, response)
        self.assertEqual(response[-1]["count_cumulative"], total_count, response)
        self.assertEqual(response[-1]["ida_byte_size_cumulative"], total_size, response)

        # test that only datasets from beginning of from_date is counted
        response = self.client.get(f"{self.url}?from_date=2018-07&to_date=2019-03").data

        self.assertEqual(response[-1]["count_cumulative"], total_count - june_count, response)
        self.assertEqual(response[-1]["ida_byte_size_cumulative"], total_size - june_size, response)

    def test_all_datasets_cumulative_single(self):
        """
        Tests single parameters for api. Empty removed and legacy parameters returns true AND false matches
        """
        total_count = self._get_total_dataset_count()
        total_size = self._get_total_byte_size()

        # test removed -parameter
        june_size = self._get_byte_size_of_month("2018-06")
        june_count = self._get_dataset_count_of_month("2018-06")

        self._set_removed_dataset(id=8)  # belongs to 2018-06, i.e. the first interval
        removed_size = self._get_catalog_record_size(id=8)

        response = self.client.get(f"{self.url}?{self.dateparam_all}&removed=true").data
        # ensure that only the first month (2018-06) contains dataset and that cumulative is calculated correctly
        self.assertEqual(response[0]["count"], 1, response)
        self.assertEqual(response[0]["ida_byte_size"], removed_size, response)
        self.assertEqual(response[0]["count_cumulative"], 1, response)
        self.assertEqual(response[0]["ida_byte_size_cumulative"], removed_size, response)

        self.assertEqual(response[-1]["count_cumulative"], 1, response)
        self.assertEqual(response[-1]["ida_byte_size_cumulative"], removed_size, response)

        response = self.client.get(f"{self.url}?{self.dateparam_all}&removed=false").data
        # ensure that the correct dataset is missing from results
        self.assertEqual(response[0]["count"], june_count - 1, response)
        self.assertEqual(response[0]["ida_byte_size"], june_size - removed_size, response)
        self.assertEqual(response[-1]["count_cumulative"], total_count - 1, response)
        self.assertEqual(
            response[-1]["ida_byte_size_cumulative"],
            total_size - removed_size,
            response,
        )

        # test legacy -parameter
        leg_cr = (
            self._create_legacy_dataset()
        )  # legacy cr belongs to 2019-03, i.e. the last interval
        self._set_dataset_creation_date(leg_cr, "2019-03-13")

        legacy_size = self._get_catalog_record_size(id=leg_cr)

        total_count = self._get_total_dataset_count()
        total_size = self._get_total_byte_size()

        march_size = self._get_byte_size_of_month("2019-03")
        march_count = self._get_dataset_count_of_month("2019-03")

        response = self.client.get(f"{self.url}?{self.dateparam_all}&legacy=true").data

        self.assertEqual(response[-1]["count"], 1, response)
        self.assertEqual(response[-1]["ida_byte_size"], legacy_size, response)
        self.assertEqual(response[-1]["count_cumulative"], 1, response)
        self.assertEqual(response[-1]["ida_byte_size_cumulative"], legacy_size, response)

        response = self.client.get(f"{self.url}?{self.dateparam_all}&legacy=false").data

        self.assertEqual(response[-1]["count"], march_count - 1, response)
        self.assertEqual(response[-1]["ida_byte_size"], march_size - legacy_size, response)
        self.assertEqual(response[-1]["count_cumulative"], total_count - 1, response)
        self.assertEqual(
            response[-1]["ida_byte_size_cumulative"], total_size - legacy_size, response
        )

        # test latest -parameter
        # new versions will belong to 2019-03, i.e. the last interval
        second = self._create_new_dataset_version()
        self._set_dataset_creation_date(second, "2019-03-17")

        old_ver_size = self._get_catalog_record_size(id=1)

        total_count = self._get_total_dataset_count()
        total_size = self._get_total_byte_size()

        march_size = self._get_byte_size_of_month("2019-03")
        march_count = self._get_dataset_count_of_month("2019-03")

        response = self.client.get(
            f"{self.url}?{self.dateparam_all}&latest=false"
        ).data  # returns all
        self.assertEqual(response[-1]["count"], march_count, response)
        self.assertEqual(response[-1]["ida_byte_size"], march_size, response)
        self.assertEqual(response[-1]["count_cumulative"], total_count, response)
        self.assertEqual(response[-1]["ida_byte_size_cumulative"], total_size, response)

        with_param = self.client.get(f"{self.url}?{self.dateparam_all}&latest=true").data
        self.assertEqual(with_param[-1]["count"], march_count - 1, with_param)
        self.assertEqual(with_param[-1]["ida_byte_size"], march_size - old_ver_size, with_param)
        self.assertEqual(with_param[-1]["count_cumulative"], total_count - 1, with_param)
        self.assertEqual(
            with_param[-1]["ida_byte_size_cumulative"],
            total_size - old_ver_size,
            response,
        )

        # ensure that default value(true) is working as expected
        without_param = self.client.get(f"{self.url}?{self.dateparam_all}").data
        self.assertEqual(with_param[-1]["count"], without_param[-1]["count"], with_param)
        self.assertEqual(
            with_param[-1]["ida_byte_size"],
            without_param[-1]["ida_byte_size"],
            with_param,
        )
        self.assertEqual(
            with_param[-1]["count_cumulative"],
            without_param[-1]["count_cumulative"],
            with_param,
        )
        self.assertEqual(
            with_param[-1]["ida_byte_size_cumulative"],
            without_param[-1]["ida_byte_size_cumulative"],
            with_param,
        )

    def test_all_datasets_cumulative_removed_latest(self):
        second = self._create_new_dataset_version()
        self._set_dataset_creation_date(second, "2019-03-11")

        self._set_removed_dataset(id=1)
        self._set_removed_dataset(id=second)

        latest_size = self._get_catalog_record_size(id=second)
        removed_size = self._get_catalog_record_size(id=1) + latest_size
        removed_count = 2

        rem_lat = self.client.get(f"{self.url}?{self.dateparam_all}&removed=true&latest=true").data

        self.assertEqual(rem_lat[-1]["count"], 1, rem_lat)  # id=second
        self.assertEqual(rem_lat[-1]["ida_byte_size"], latest_size, rem_lat)
        self.assertEqual(rem_lat[-1]["count_cumulative"], 1, rem_lat)
        self.assertEqual(rem_lat[-1]["ida_byte_size_cumulative"], latest_size, rem_lat)

        rem_not_lat = self.client.get(
            f"{self.url}?{self.dateparam_all}&removed=true&latest=false"
        ).data

        self.assertEqual(rem_not_lat[-1]["count"], removed_count, rem_not_lat)  # id=second
        self.assertEqual(rem_not_lat[-1]["ida_byte_size"], removed_size, rem_not_lat)
        self.assertEqual(rem_not_lat[-1]["count_cumulative"], removed_count, rem_not_lat)
        self.assertEqual(rem_not_lat[-1]["ida_byte_size_cumulative"], removed_size, rem_not_lat)

        # create new dataset with two versions, which will not be deleted
        new_cr_id = self._create_new_dataset(self.cr_test_data)
        self._set_dataset_creation_date(new_cr_id, "2019-01-02")

        new_cr_ver = self._create_new_dataset_version(new_cr_id)
        self._set_dataset_creation_date(new_cr_ver, "2019-01-06")

        old_version_size = self._get_catalog_record_size(id=new_cr_id)

        jan_count = self._get_dataset_count_of_month("2019-01")
        jan_size = self._get_byte_size_of_month("2019-01")

        total_count = self._get_total_dataset_count()
        total_size = self._get_total_byte_size()

        not_rem_lat = self.client.get(
            f"{self.url}?{self.dateparam_all}&removed=false&latest=true"
        ).data

        # missing the removed dataset from before and dataset id='new_cr_id'
        self.assertEqual(not_rem_lat[-3]["count"], jan_count - 1, not_rem_lat)
        self.assertEqual(not_rem_lat[-3]["ida_byte_size"], jan_size - old_version_size, not_rem_lat)
        self.assertEqual(
            not_rem_lat[-1]["count_cumulative"],
            total_count - removed_count - 1,
            not_rem_lat,
        )
        self.assertEqual(
            not_rem_lat[-1]["ida_byte_size_cumulative"],
            total_size - removed_size - old_version_size,
            not_rem_lat,
        )

        not_rem_not_lat = self.client.get(
            f"{self.url}?{self.dateparam_all}&removed=false&latest=false"
        ).data

        self.assertEqual(not_rem_not_lat[-3]["count"], jan_count, not_rem_not_lat)
        self.assertEqual(not_rem_not_lat[-3]["ida_byte_size"], jan_size, not_rem_not_lat)
        self.assertEqual(
            not_rem_not_lat[-1]["count_cumulative"],
            total_count - removed_count,
            not_rem_not_lat,
        )
        self.assertEqual(
            not_rem_not_lat[-1]["ida_byte_size_cumulative"],
            total_size - removed_size,
            not_rem_not_lat,
        )

    def test_all_datasets_cumulative_removed_legacy(self):
        leg_cr_1 = self._create_legacy_dataset()
        self._set_dataset_creation_date(leg_cr_1, "2018-07-03")

        leg_cr_2 = self._create_legacy_dataset()
        self._set_dataset_creation_date(leg_cr_2, "2019-02-08")
        self._set_removed_dataset(leg_cr_2)

        self._set_removed_dataset(id=8)  # belongs to first interval, i.e. 2018-06

        leg_non_rem_size = self._get_catalog_record_size(leg_cr_1)
        leg_removed_size = self._get_catalog_record_size(leg_cr_2)
        removed_size = self._get_catalog_record_size(8)

        rem_leg_count = 3
        rem_leg_size = leg_non_rem_size + leg_removed_size + removed_size

        total_count = self._get_total_dataset_count()
        total_size = self._get_total_byte_size()

        june_count = self._get_dataset_count_of_month("2018-06")
        june_size = self._get_byte_size_of_month("2018-06")

        feb_count = self._get_dataset_count_of_month("2019-02")
        feb_size = self._get_byte_size_of_month("2019-02")

        rem_leg = self.client.get(f"{self.url}?{self.dateparam_all}&removed=true&legacy=true").data

        self.assertEqual(rem_leg[-2]["count"], 1, rem_leg)
        self.assertEqual(rem_leg[-2]["ida_byte_size"], leg_removed_size, rem_leg)
        self.assertEqual(rem_leg[-1]["count_cumulative"], 1, rem_leg)
        self.assertEqual(rem_leg[-1]["ida_byte_size_cumulative"], leg_removed_size, rem_leg)

        rem_not_leg = self.client.get(
            f"{self.url}?{self.dateparam_all}&removed=true&legacy=false"
        ).data

        self.assertEqual(rem_not_leg[0]["count"], 1, rem_not_leg)
        self.assertEqual(rem_not_leg[0]["ida_byte_size"], removed_size, rem_not_leg)
        self.assertEqual(rem_not_leg[-1]["count_cumulative"], 1, rem_not_leg)
        self.assertEqual(rem_not_leg[-1]["ida_byte_size_cumulative"], removed_size, rem_not_leg)

        not_rem_leg = self.client.get(
            f"{self.url}?{self.dateparam_all}&removed=false&legacy=true"
        ).data

        self.assertEqual(not_rem_leg[1]["count"], 1, not_rem_leg)
        self.assertEqual(not_rem_leg[1]["ida_byte_size"], leg_non_rem_size, not_rem_leg)
        self.assertEqual(not_rem_leg[-1]["count_cumulative"], 1, not_rem_leg)
        self.assertEqual(not_rem_leg[-1]["ida_byte_size_cumulative"], leg_non_rem_size, not_rem_leg)

        not_rem_not_leg = self.client.get(
            f"{self.url}?{self.dateparam_all}&removed=false&legacy=false"
        ).data

        self.assertEqual(not_rem_not_leg[0]["count"], june_count - 1, not_rem_not_leg)
        self.assertEqual(
            not_rem_not_leg[0]["ida_byte_size"],
            june_size - removed_size,
            not_rem_not_leg,
        )
        self.assertEqual(not_rem_not_leg[-2]["count"], feb_count - 1, not_rem_not_leg)
        self.assertEqual(
            not_rem_not_leg[-2]["ida_byte_size"],
            feb_size - leg_removed_size,
            not_rem_not_leg,
        )
        self.assertEqual(
            not_rem_not_leg[-1]["count_cumulative"],
            total_count - rem_leg_count,
            not_rem_not_leg,
        )
        self.assertEqual(
            not_rem_not_leg[-1]["ida_byte_size_cumulative"],
            total_size - rem_leg_size,
            not_rem_not_leg,
        )

    def test_all_datasets_cumulative_latest_legacy(self):
        leg_cr = self._create_legacy_dataset()
        self._set_dataset_creation_date(leg_cr, "2019-03-08")

        second = self._create_new_dataset_version(leg_cr)
        self._set_dataset_creation_date(second, "2019-03-12")

        leg_cr_size = self._get_catalog_record_size(id=leg_cr)
        second_size = self._get_catalog_record_size(id=second)

        legacy_count = 2
        legacy_size = leg_cr_size + second_size

        total_count = self._get_total_dataset_count()
        total_size = self._get_total_byte_size()

        march_count = self._get_dataset_count_of_month("2019-03")
        march_size = self._get_byte_size_of_month("2019-03")

        leg_lat = self.client.get(f"{self.url}?{self.dateparam_all}&legacy=true&latest=true").data

        self.assertEqual(leg_lat[-1]["count"], 1, leg_lat)
        self.assertEqual(leg_lat[-1]["ida_byte_size"], second_size, leg_lat)
        self.assertEqual(leg_lat[-1]["count_cumulative"], 1, leg_lat)
        self.assertEqual(leg_lat[-1]["ida_byte_size_cumulative"], second_size, leg_lat)

        leg_not_lat = self.client.get(
            f"{self.url}?{self.dateparam_all}&legacy=true&latest=false"
        ).data

        self.assertEqual(leg_not_lat[-1]["count"], legacy_count, leg_not_lat)
        self.assertEqual(leg_not_lat[-1]["ida_byte_size"], legacy_size, leg_not_lat)
        self.assertEqual(leg_not_lat[-1]["count_cumulative"], legacy_count, leg_not_lat)
        self.assertEqual(leg_not_lat[-1]["ida_byte_size_cumulative"], legacy_size, leg_not_lat)

        not_leg_not_lat = self.client.get(
            f"{self.url}?{self.dateparam_all}&legacy=false&latest=false"
        ).data

        self.assertEqual(not_leg_not_lat[-1]["count"], march_count - legacy_count, not_leg_not_lat)
        self.assertEqual(
            not_leg_not_lat[-1]["ida_byte_size"],
            march_size - legacy_size,
            not_leg_not_lat,
        )
        self.assertEqual(
            not_leg_not_lat[-1]["count_cumulative"],
            total_count - legacy_count,
            not_leg_not_lat,
        )
        self.assertEqual(
            not_leg_not_lat[-1]["ida_byte_size_cumulative"],
            total_size - legacy_size,
            not_leg_not_lat,
        )


class StatisticRPCforDrafts(StatisticRPCCommon, CatalogRecordApiWriteCommon):
    """
    Tests that drafts are not taken into account when calculating statistics
    """

    def test_count_datasets_api_for_drafts(self):
        """
        Tests that rpc/statistics/count_datasets returns only count of published datasets
        """
        response_1 = self.client.get("/rpc/statistics/count_datasets").data

        self._set_dataset_as_draft(1)
        self.assertEqual(
            CatalogRecord.objects.get(pk=1).state,
            "draft",
            "Dataset with id=1 should have changed state to draft",
        )

        response_2 = self.client.get("/rpc/statistics/count_datasets").data
        self.assertNotEqual(
            response_1["count"],
            response_2["count"],
            "Drafts should not be returned in count_datasets api",
        )

    def test_all_datasets_cumulative_for_drafts(self):
        """
        Tests that /rpc/statistics/all_datasets_cumulative returns only published datasets
        """
        url = "/rpc/statistics/all_datasets_cumulative?from_date=2019-06&to_date=2019-06"

        self._set_dataset_creation_date(1, "2019-06-15")
        response_1 = self.client.get(url).data

        self._set_dataset_as_draft(1)
        response_2 = self.client.get(url).data

        # ensure the counts and byte sizes are calculated without drafts
        self.assertNotEqual(
            response_1[0]["count"],
            response_2[0]["count"],
            "Count for June should reduce by one as dataset id=1 was set as draft",
        )
        self.assertNotEqual(
            response_1[0]["ida_byte_size"],
            response_2[0]["ida_byte_size"],
            "Byte size for June should reduce by one as dataset id=1 was set as draft",
        )

    def test_catalog_datasets_cumulative_for_drafts(self):
        """
        Tests that /rpc/statistics/catalog_datasets_cumulative returns only published datasets
        """

        url = "/rpc/statistics/catalog_datasets_cumulative?from_date=2019-06-01&to_date=2019-06-30"
        catalog = "urn:nbn:fi:att:2955e904-e3dd-4d7e-99f1-3fed446f96d3"

        self._set_dataset_creation_date(1, "2019-06-15")
        self._set_cr_datacatalog(1, catalog)  # Adds id=1 to catalog

        count_1 = self.client.get(url).data[catalog]["open"][0]["count"]
        total_1 = self.client.get(url).data[catalog]["total"]

        self._set_dataset_as_draft(1)

        count_2 = self.client.get(url).data[catalog]["open"][0]["count"]
        total_2 = self.client.get(url).data[catalog]["total"]

        # ensure the count and total are calculated without drafts
        self.assertNotEqual(
            count_1,
            count_2,
            "Count should reduce by one as dataset id=1 was set as draft",
        )
        self.assertNotEqual(
            total_1,
            total_2,
            "Total should reduce by one as dataset id=1 was set as draft",
        )

    def test_end_user_datasets_cumulative_for_drafts(self):
        """ End user api should return only published data """
        url = "/rpc/statistics/end_user_datasets_cumulative?from_date=2019-06-01&to_date=2019-06-30"

        self._set_dataset_creation_date(10, "2019-06-15")
        count_1 = self.client.get(url).data[0]["count"]

        self._set_dataset_as_draft(10)
        count_2 = self.client.get(url).data[0]["count"]

        # ensure the count are calculated without drafts
        self.assertNotEqual(
            count_1,
            count_2,
            "Count should be reduced by one after setting id=10 as draft",
        )

    def test_organization_datasets_cumulative_for_drafts(self):
        """ Organization api should return only published data """
        url = "/rpc/statistics/organization_datasets_cumulative?from_date=2019-06-01&to_date=2019-06-30"

        self._set_dataset_creation_date(1, "2019-06-15")
        self._set_cr_organization(1, "org_2")
        total_1 = self.client.get(url).data["org_2"]["total"]

        self._set_dataset_as_draft(1)
        total_2 = self.client.get(url).data["org_2"]["total"]

        # ensure the totals are calculated without drafts
        self.assertNotEqual(total_1, total_2, "Count be reduced by one after setting id=1 as draft")
