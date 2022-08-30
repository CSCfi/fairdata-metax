# This file is part of the Metax API service
#
# Copyright 2017-2020 Ministry of Education and Culture, Finland
#
# :author: CSC - IT Center for Science Ltd., Espoo Finland <servicedesk@csc.fi>
# :license: MIT

from django.conf import settings as django_settings
from rest_framework import status

from metax_api.models import CatalogRecordV2, DataCatalog
from metax_api.tests.utils import TestClassUtils
from metax_api.utils import get_tz_aware_now_without_micros

from .write import CatalogRecordApiWriteCommon


class CatalogRecordApiWritePreservationStateTests(CatalogRecordApiWriteCommon, TestClassUtils):

    """
    Field preservation_state related tests.
    """

    def setUp(self):
        super().setUp()
        dc = DataCatalog.objects.get(pk=1)
        catalog_json = dc.catalog_json
        catalog_json["identifier"] = django_settings.PAS_DATA_CATALOG_IDENTIFIER
        catalog_json["dataset_versioning"] = False
        dc = DataCatalog.objects.create(
            catalog_json=catalog_json,
            date_created=get_tz_aware_now_without_micros(),
            catalog_record_services_create="testuser,api_auth_user,metax",
            catalog_record_services_edit="testuser,api_auth_user,metax",
        )

    def test_update_pas_state_to_needs_revalidation(self):
        """
        When dataset metadata is updated, and preservation_state in (40, 50, 70), metax should
        automatically update preservation_state value to 60 ("validated metadata updated").
        """
        cr = CatalogRecordV2.objects.get(pk=1)

        for i, preservation_state_value in enumerate((40, 50, 70)):
            # set testing initial condition...
            cr.preservation_state = preservation_state_value
            cr.save()

            # retrieve record and ensure testing state was set correctly...
            cr_data = self.client.get("/rest/v2/datasets/1", format="json").data
            self.assertEqual(cr_data["preservation_state"], preservation_state_value)

            # strike and verify
            cr_data["research_dataset"]["title"]["en"] = "Metadata has been updated on loop %d" % i
            response = self.client.put("/rest/v2/datasets/1", cr_data, format="json")
            self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
            self.assertEqual(response.data["preservation_state"], 60)

    def test_prevent_file_changes_when_record_in_pas_process(self):
        """
        When preservation_state > 0, changing associated files of a dataset should not be allowed.
        """
        cr = CatalogRecordV2.objects.get(pk=1)
        cr.preservation_state = 10
        cr.save()

        file_changes = {"files": [{"identifier": "pid:urn:3"}]}

        response = self.client.post("/rest/v2/datasets/1/files", file_changes, format="json")
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST, response.data)
        self.assertEqual(
            "Changing files of a published" in response.data["detail"][0],
            True,
            response.data,
        )

    def test_pas_version_is_created_on_preservation_state_80(self):
        """
        When preservation_state is updated to 'accepted to pas', a copy should be created into
        designated PAS catalog.
        """
        cr_data = self.client.get("/rest/v2/datasets/1", format="json").data
        self.assertEqual(cr_data["preservation_state"], 0)

        origin_dataset = self._create_pas_dataset_from_id(1)
        self.assertEqual(origin_dataset["preservation_state"], 0)
        self.assertEqual("new_version_created" in origin_dataset, True)
        self.assertEqual(origin_dataset["new_version_created"]["version_type"], "pas")
        self.assertEqual("preservation_dataset_version" in origin_dataset, True)
        self.assertEqual("other_identifier" in origin_dataset["research_dataset"], True)
        self.assertEqual(
            origin_dataset["research_dataset"]["other_identifier"][0]["notation"].startswith("doi"),
            True,
        )

        # get pas version and verify links and other signature values are there
        pas_dataset = self.client.get(
            "/rest/v2/datasets/%d" % origin_dataset["preservation_dataset_version"]["id"],
            format="json",
        ).data
        self.assertEqual(
            pas_dataset["data_catalog"]["identifier"],
            django_settings.PAS_DATA_CATALOG_IDENTIFIER,
        )
        self.assertEqual(pas_dataset["preservation_state"], 80)
        self.assertEqual(
            pas_dataset["preservation_dataset_origin_version"]["id"],
            origin_dataset["id"],
        )
        self.assertEqual(
            pas_dataset["preservation_dataset_origin_version"]["preferred_identifier"],
            origin_dataset["research_dataset"]["preferred_identifier"],
        )
        self.assertEqual("deprecated" in pas_dataset["preservation_dataset_origin_version"], True)
        self.assertEqual("other_identifier" in pas_dataset["research_dataset"], True)
        self.assertEqual(
            pas_dataset["research_dataset"]["other_identifier"][0]["notation"].startswith("urn"),
            True,
        )

        # when pas copy is created, origin_dataset preservation_state should have been set back to 0
        cr_data = self.client.get("/rest/v2/datasets/1", format="json").data
        self.assertEqual(cr_data["preservation_state"], 0)

        # ensure files match between original and pas cr
        cr = CatalogRecordV2.objects.get(pk=1)
        cr_files = cr.files.filter().order_by("id").values_list("id", flat=True)
        cr_pas_files = (
            cr.preservation_dataset_version.files.filter()
            .order_by("id")
            .values_list("id", flat=True)
        )

        # note: trying to assert querysets will result in failure. must evaluate the querysets first by iterating them
        self.assertEqual([f for f in cr_files], [f for f in cr_pas_files])

    def test_file_relations_remain_in_pas(self):
        cr_data = self.client.get("/rest/v2/datasets/1?include_user_metadata", format="json").data
        self.assertEqual(cr_data["preservation_state"], 0)

        files = [{"identifier": "pid:urn:1"}, {"identifier": "pid:urn:2"}]
        file_relations = [{"pas_compatible_file": "pid:urn:1", "non_pas_compatible_file": "pid:urn:2"}]
        cr_data["research_dataset"]["files"] = files
        cr_data["research_dataset"]["file_relations"] = file_relations
        del(cr_data["research_dataset"]["preferred_identifier"])
        del(cr_data["identifier"])

        response = self.client.post("/rest/v2/datasets", cr_data, format="json")
        self.assertEqual(response.status_code, 201)
        cr_id = response.data["id"]

        response = self.client.get(f"/rest/v2/datasets/{cr_id}?include_user_metadata", format="json")
        cr_data = response.data
        cr_fr = cr_data["research_dataset"]["file_relations"]
        self.assertEqual(response.status_code, 200)
        self.assertTrue(cr_fr)
        self.assertEqual(cr_fr, file_relations)

        origin_dataset = self._create_pas_dataset_from_id(cr_id)
        pas_id = origin_dataset["preservation_dataset_version"]["id"]

        response = self.client.get(f"/rest/v2/datasets/{pas_id}?include_user_metadata", format="json")
        cr_data = response.data
        pas_fr = cr_data["research_dataset"]["file_relations"]

        self.assertEqual(response.status_code, 200)
        self.assertTrue(pas_fr)
        self.assertEqual(pas_fr, cr_fr)
