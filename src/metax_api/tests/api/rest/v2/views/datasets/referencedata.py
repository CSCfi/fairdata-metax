# This file is part of the Metax API service
#
# Copyright 2017-2020 Ministry of Education and Culture, Finland
#
# :author: CSC - IT Center for Science Ltd., Espoo Finland <servicedesk@csc.fi>
# :license: MIT

from copy import deepcopy

from rest_framework import status

from metax_api.services import ReferenceDataMixin as RDM
from metax_api.services.redis_cache_service import RedisClient

from .write import CatalogRecordApiWriteCommon


class CatalogRecordApiWriteReferenceDataTests(CatalogRecordApiWriteCommon):
    """
    Tests related to reference_data validation and dataset fields population
    from reference_data, according to given uri or code as the value.
    """

    def test_organization_name_is_required(self):
        """
        Organization 'name' field is not madatory in the schema, but that is only because it does
        not make sense to end users when using an identifier from reference data, which will overwrite
        the name anyway.

        If an organization identifier is used, which is not found in the reference data, and therefore
        does not populate the name automatically, then the user is required to provide the name.
        """

        # simple case
        cr = deepcopy(self.cr_full_ida_test_data)
        cr["research_dataset"]["curator"] = [
            {
                "@type": "Organization",
                "identifier": "not found!",
                # no name!
            }
        ]
        response = self.client.post("/rest/v2/datasets", cr, format="json")
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)

        # a more complex case. ensure organizations are found from deep structures
        cr = deepcopy(self.cr_full_ida_test_data)
        org = cr["research_dataset"]["provenance"][0]["was_associated_with"][0]
        del org["name"]  # should cause the error
        org["@type"] = "Organization"
        org["identifier"] = "not found!"
        response = self.client.post("/rest/v2/datasets", cr, format="json")
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)

        # try again. should be ok
        org["identifier"] = "http://uri.suomi.fi/codelist/fairdata/organization/code/10076"
        response = self.client.post("/rest/v2/datasets", cr, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)

    def test_catalog_record_reference_data_missing_ok(self):
        """
        The API should attempt to reload the reference data if it is missing from
        cache for whatever reason, and successfully finish the request
        """
        cache = RedisClient()
        cache.delete("reference_data")
        self.assertEqual(
            cache.get("reference_data", master=True),
            None,
            "cache ref data should be missing after cache.delete()",
        )

        response = self.client.post("/rest/v2/datasets", self.cr_test_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)

    def test_missing_license_identifier_ok(self):
        """
        Missing license identifier is ok if url is provided.
        Works on att and ida datasets
        """
        rd_ida = self.cr_full_ida_test_data["research_dataset"]
        rd_ida["access_rights"]["license"] = [{"license": "http://a.very.nice.custom/url"}]
        response = self.client.post("/rest/v2/datasets", self.cr_full_ida_test_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)
        self.assertEqual(
            len(response.data["research_dataset"]["access_rights"]["license"][0]),
            1,
            response.data,
        )

        rd_att = self.cr_full_att_test_data["research_dataset"]
        rd_att["access_rights"]["license"] = [
            {
                "license": "http://also.fine.custom/uri",
                "description": {
                    "en": "This is very informative description of this custom license."
                },
            }
        ]
        rd_att["remote_resources"][0]["license"] = [
            {
                "license": "http://cool.remote.uri",
                "description": {
                    "en": "Proof that also remote licenses can be used with custom urls."
                },
            }
        ]
        response = self.client.post("/rest/v2/datasets", self.cr_full_att_test_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)
        self.assertEqual(
            len(response.data["research_dataset"]["access_rights"]["license"][0]),
            2,
            response.data,
        )
        self.assertEqual(
            len(response.data["research_dataset"]["remote_resources"][0]["license"][0]),
            2,
            response.data,
        )

    def test_create_catalog_record_with_invalid_reference_data(self):
        rd_ida = self.cr_full_ida_test_data["research_dataset"]
        rd_ida["theme"][0]["identifier"] = "nonexisting"
        rd_ida["field_of_science"][0]["identifier"] = "nonexisting"
        rd_ida["language"][0]["identifier"] = "nonexisting"
        rd_ida["access_rights"]["access_type"]["identifier"] = "nonexisting"
        rd_ida["access_rights"]["license"][0]["identifier"] = "nonexisting"
        rd_ida["other_identifier"][0]["type"]["identifier"] = "nonexisting"
        rd_ida["spatial"][0]["place_uri"]["identifier"] = "nonexisting"
        rd_ida["files"][0]["file_type"]["identifier"] = "nonexisting"
        rd_ida["files"][0]["use_category"]["identifier"] = "nonexisting"
        rd_ida["infrastructure"][0]["identifier"] = "nonexisting"
        rd_ida["creator"][0]["contributor_role"][0]["identifier"] = "nonexisting"
        rd_ida["curator"][0]["contributor_type"][0]["identifier"] = "nonexisting"
        rd_ida["is_output_of"][0]["funder_type"]["identifier"] = "nonexisting"
        rd_ida["directories"][0]["use_category"]["identifier"] = "nonexisting"
        rd_ida["relation"][0]["relation_type"]["identifier"] = "nonexisting"
        rd_ida["relation"][0]["entity"]["type"]["identifier"] = "nonexisting"
        rd_ida["provenance"][0]["lifecycle_event"]["identifier"] = "nonexisting"
        rd_ida["provenance"][1]["preservation_event"]["identifier"] = "nonexisting"
        rd_ida["provenance"][0]["event_outcome"]["identifier"] = "nonexisting"
        response = self.client.post("/rest/v2/datasets", self.cr_full_ida_test_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual("research_dataset" in response.data.keys(), True)
        self.assertEqual(len(response.data["research_dataset"]), 19)

        rd_att = self.cr_full_att_test_data["research_dataset"]
        rd_att["remote_resources"][0]["license"][0]["identifier"] = "nonexisting"
        rd_att["remote_resources"][1]["resource_type"]["identifier"] = "nonexisting"
        rd_att["remote_resources"][0]["use_category"]["identifier"] = "nonexisting"
        response = self.client.post("/rest/v2/datasets", self.cr_full_att_test_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual("research_dataset" in response.data.keys(), True)
        self.assertEqual(len(response.data["research_dataset"]), 3)

    def test_create_catalog_record_populate_fields_from_reference_data(self):
        """
        1) Insert codes from cached reference data to dataset identifier fields
           that will be validated, and then populated
        2) Check that that the values in dataset identifier fields are changed from
           codes to uris after a successful create
        3) Check that labels have also been copied to datasets to their approriate fields
        """
        cache = RedisClient()
        rf = RDM.get_reference_data(cache)
        refdata = rf["reference_data"]
        orgdata = rf["organization_data"]
        refs = {}

        data_types = [
            "access_type",
            "restriction_grounds",
            "field_of_science",
            "identifier_type",
            "keyword",
            "language",
            "license",
            "location",
            "resource_type",
            "file_type",
            "use_category",
            "research_infra",
            "contributor_role",
            "contributor_type",
            "funder_type",
            "relation_type",
            "lifecycle_event",
            "preservation_event",
            "event_outcome",
        ]

        # the values in these selected entries will be used throghout the rest of the test case
        for dtype in data_types:
            if dtype == "location":
                entry = next((obj for obj in refdata[dtype] if obj.get("wkt", False)), None)
                self.assertTrue(entry is not None)
            else:
                entry = refdata[dtype][1]
            refs[dtype] = {
                "code": entry["code"],
                "uri": entry["uri"],
                "label": entry.get("label", None),
                "wkt": entry.get("wkt", None),
                "scheme": entry.get("scheme", None),
            }

        refs["organization"] = {
            "uri": orgdata["organization"][0]["uri"],
            "code": orgdata["organization"][0]["code"],
            "label": orgdata["organization"][0]["label"],
        }

        # replace the relations with objects that have only the identifier set with code as value,
        # to easily check that label was populated (= that it appeared in the dataset after create)
        # without knowing its original value from the generated test data
        rd_ida = self.cr_full_ida_test_data["research_dataset"]
        rd_ida["theme"][0] = {"identifier": refs["keyword"]["code"]}
        rd_ida["field_of_science"][0] = {"identifier": refs["field_of_science"]["code"]}
        rd_ida["language"][0] = {"identifier": refs["language"]["code"]}
        rd_ida["access_rights"]["access_type"] = {"identifier": refs["access_type"]["code"]}
        rd_ida["access_rights"]["restriction_grounds"][0] = {
            "identifier": refs["restriction_grounds"]["code"]
        }
        rd_ida["access_rights"]["license"][0] = {"identifier": refs["license"]["code"]}
        rd_ida["other_identifier"][0]["type"] = {"identifier": refs["identifier_type"]["code"]}
        rd_ida["spatial"][0]["place_uri"] = {"identifier": refs["location"]["code"]}
        rd_ida["files"][0]["file_type"] = {"identifier": refs["file_type"]["code"]}
        rd_ida["files"][0]["use_category"] = {"identifier": refs["use_category"]["code"]}
        rd_ida["directories"][0]["use_category"] = {"identifier": refs["use_category"]["code"]}
        rd_ida["infrastructure"][0] = {"identifier": refs["research_infra"]["code"]}
        rd_ida["creator"][0]["contributor_role"][0] = {
            "identifier": refs["contributor_role"]["code"]
        }
        rd_ida["curator"][0]["contributor_type"][0] = {
            "identifier": refs["contributor_type"]["code"]
        }
        rd_ida["is_output_of"][0]["funder_type"] = {"identifier": refs["funder_type"]["code"]}
        rd_ida["relation"][0]["relation_type"] = {"identifier": refs["relation_type"]["code"]}
        rd_ida["relation"][0]["entity"]["type"] = {"identifier": refs["resource_type"]["code"]}
        rd_ida["provenance"][0]["lifecycle_event"] = {"identifier": refs["lifecycle_event"]["code"]}
        rd_ida["provenance"][1]["preservation_event"] = {
            "identifier": refs["preservation_event"]["code"]
        }
        rd_ida["provenance"][0]["event_outcome"] = {"identifier": refs["event_outcome"]["code"]}

        # these have other required fields, so only update the identifier with code
        rd_ida["is_output_of"][0]["source_organization"][0]["identifier"] = refs["organization"][
            "code"
        ]
        rd_ida["is_output_of"][0]["has_funding_agency"][0]["identifier"] = refs["organization"][
            "code"
        ]
        rd_ida["other_identifier"][0]["provider"]["identifier"] = refs["organization"]["code"]
        rd_ida["contributor"][0]["member_of"]["identifier"] = refs["organization"]["code"]
        rd_ida["creator"][0]["member_of"]["identifier"] = refs["organization"]["code"]
        rd_ida["curator"][0]["is_part_of"]["identifier"] = refs["organization"]["code"]
        rd_ida["publisher"]["is_part_of"]["identifier"] = refs["organization"]["code"]
        rd_ida["rights_holder"][0]["is_part_of"]["identifier"] = refs["organization"]["code"]

        # Other type of reference data populations
        orig_wkt_value = rd_ida["spatial"][0]["as_wkt"][0]
        rd_ida["spatial"][0]["place_uri"]["identifier"] = refs["location"]["code"]
        rd_ida["spatial"][1]["as_wkt"] = []
        rd_ida["spatial"][1]["place_uri"]["identifier"] = refs["location"]["code"]

        response = self.client.post(
            "/rest/v2/datasets?include_user_metadata",
            self.cr_full_ida_test_data,
            format="json",
        )
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)
        self.assertEqual("research_dataset" in response.data.keys(), True)

        new_rd_ida = response.data["research_dataset"]
        self._assert_uri_copied_to_identifier(refs, new_rd_ida)
        self._assert_label_copied_to_pref_label(refs, new_rd_ida)
        self._assert_label_copied_to_title(refs, new_rd_ida)
        self._assert_label_copied_to_name(refs, new_rd_ida)

        # Assert if spatial as_wkt field has been populated with a value from ref data which has wkt value having
        # condition that the user has not given own coordinates in the as_wkt field
        self.assertEqual(orig_wkt_value, new_rd_ida["spatial"][0]["as_wkt"][0])
        self.assertEqual(refs["location"]["wkt"], new_rd_ida["spatial"][1]["as_wkt"][0])

        # rd from att data catalog
        rd_att = self.cr_full_att_test_data["research_dataset"]
        rd_att["remote_resources"][1]["resource_type"] = {
            "identifier": refs["resource_type"]["code"]
        }
        rd_att["remote_resources"][0]["use_category"] = {"identifier": refs["use_category"]["code"]}
        rd_att["remote_resources"][0]["license"][0] = {"identifier": refs["license"]["code"]}

        # Assert remote resources related reference datas
        response = self.client.post("/rest/v2/datasets", self.cr_full_att_test_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)
        self.assertEqual("research_dataset" in response.data.keys(), True)
        new_rd_att = response.data["research_dataset"]
        self._assert_att_remote_resource_items(refs, new_rd_att)

    def _assert_att_remote_resource_items(self, refs, new_rd):
        self.assertEqual(
            refs["resource_type"]["uri"],
            new_rd["remote_resources"][1]["resource_type"]["identifier"],
        )
        self.assertEqual(
            refs["use_category"]["uri"],
            new_rd["remote_resources"][0]["use_category"]["identifier"],
        )
        self.assertEqual(
            refs["license"]["uri"],
            new_rd["remote_resources"][0]["license"][0]["identifier"],
        )
        self.assertEqual(
            refs["resource_type"]["label"],
            new_rd["remote_resources"][1]["resource_type"].get("pref_label", None),
        )
        self.assertEqual(
            refs["use_category"]["label"],
            new_rd["remote_resources"][0]["use_category"].get("pref_label", None),
        )
        self.assertEqual(
            refs["license"]["label"],
            new_rd["remote_resources"][0]["license"][0].get("title", None),
        )

    def _assert_uri_copied_to_identifier(self, refs, new_rd):
        self.assertEqual(refs["keyword"]["uri"], new_rd["theme"][0]["identifier"])
        self.assertEqual(
            refs["field_of_science"]["uri"], new_rd["field_of_science"][0]["identifier"]
        )
        self.assertEqual(refs["language"]["uri"], new_rd["language"][0]["identifier"])
        self.assertEqual(
            refs["access_type"]["uri"],
            new_rd["access_rights"]["access_type"]["identifier"],
        )
        self.assertEqual(
            refs["restriction_grounds"]["uri"],
            new_rd["access_rights"]["restriction_grounds"][0]["identifier"],
        )
        self.assertEqual(
            refs["license"]["uri"], new_rd["access_rights"]["license"][0]["identifier"]
        )
        self.assertEqual(
            refs["identifier_type"]["uri"],
            new_rd["other_identifier"][0]["type"]["identifier"],
        )
        self.assertEqual(refs["location"]["uri"], new_rd["spatial"][0]["place_uri"]["identifier"])
        self.assertEqual(refs["file_type"]["uri"], new_rd["files"][0]["file_type"]["identifier"])
        self.assertEqual(
            refs["use_category"]["uri"],
            new_rd["files"][0]["use_category"]["identifier"],
        )

        self.assertEqual(
            refs["use_category"]["uri"],
            new_rd["directories"][0]["use_category"]["identifier"],
        )
        self.assertEqual(
            refs["organization"]["uri"],
            new_rd["is_output_of"][0]["source_organization"][0]["identifier"],
        )
        self.assertEqual(
            refs["organization"]["uri"],
            new_rd["is_output_of"][0]["has_funding_agency"][0]["identifier"],
        )
        self.assertEqual(
            refs["organization"]["uri"],
            new_rd["other_identifier"][0]["provider"]["identifier"],
        )
        self.assertEqual(
            refs["organization"]["uri"],
            new_rd["contributor"][0]["member_of"]["identifier"],
        )
        self.assertEqual(
            refs["organization"]["uri"], new_rd["creator"][0]["member_of"]["identifier"]
        )
        self.assertEqual(
            refs["organization"]["uri"],
            new_rd["curator"][0]["is_part_of"]["identifier"],
        )
        self.assertEqual(
            refs["organization"]["uri"], new_rd["publisher"]["is_part_of"]["identifier"]
        )
        self.assertEqual(
            refs["organization"]["uri"],
            new_rd["rights_holder"][0]["is_part_of"]["identifier"],
        )
        self.assertEqual(refs["research_infra"]["uri"], new_rd["infrastructure"][0]["identifier"])
        self.assertEqual(
            refs["contributor_role"]["uri"],
            new_rd["creator"][0]["contributor_role"][0]["identifier"],
        )
        self.assertEqual(
            refs["contributor_type"]["uri"],
            new_rd["curator"][0]["contributor_type"][0]["identifier"],
        )
        self.assertEqual(
            refs["funder_type"]["uri"],
            new_rd["is_output_of"][0]["funder_type"]["identifier"],
        )
        self.assertEqual(
            refs["relation_type"]["uri"],
            new_rd["relation"][0]["relation_type"]["identifier"],
        )
        self.assertEqual(
            refs["resource_type"]["uri"],
            new_rd["relation"][0]["entity"]["type"]["identifier"],
        )
        self.assertEqual(
            refs["lifecycle_event"]["uri"],
            new_rd["provenance"][0]["lifecycle_event"]["identifier"],
        )
        self.assertEqual(
            refs["preservation_event"]["uri"],
            new_rd["provenance"][1]["preservation_event"]["identifier"],
        )
        self.assertEqual(
            refs["event_outcome"]["uri"],
            new_rd["provenance"][0]["event_outcome"]["identifier"],
        )

    def _assert_scheme_copied_to_in_scheme(self, refs, new_rd):
        self.assertEqual(refs["keyword"]["scheme"], new_rd["theme"][0]["in_scheme"])
        self.assertEqual(
            refs["field_of_science"]["scheme"],
            new_rd["field_of_science"][0]["in_scheme"],
        )
        self.assertEqual(refs["language"]["scheme"], new_rd["language"][0]["in_scheme"])
        self.assertEqual(
            refs["access_type"]["scheme"],
            new_rd["access_rights"]["access_type"]["in_scheme"],
        )
        self.assertEqual(
            refs["restriction_grounds"]["scheme"],
            new_rd["access_rights"]["restriction_grounds"][0]["in_scheme"],
        )
        self.assertEqual(
            refs["license"]["scheme"],
            new_rd["access_rights"]["license"][0]["in_scheme"],
        )
        self.assertEqual(
            refs["identifier_type"]["scheme"],
            new_rd["other_identifier"][0]["type"]["in_scheme"],
        )
        self.assertEqual(refs["location"]["scheme"], new_rd["spatial"][0]["place_uri"]["in_scheme"])
        self.assertEqual(refs["file_type"]["scheme"], new_rd["files"][0]["file_type"]["in_scheme"])
        self.assertEqual(
            refs["use_category"]["scheme"],
            new_rd["files"][0]["use_category"]["in_scheme"],
        )

        self.assertEqual(
            refs["use_category"]["scheme"],
            new_rd["directories"][0]["use_category"]["in_scheme"],
        )
        self.assertEqual(refs["research_infra"]["scheme"], new_rd["infrastructure"][0]["in_scheme"])
        self.assertEqual(
            refs["contributor_role"]["scheme"],
            new_rd["creator"][0]["contributor_role"]["in_scheme"],
        )
        self.assertEqual(
            refs["contributor_type"]["scheme"],
            new_rd["curator"][0]["contributor_type"]["in_scheme"],
        )
        self.assertEqual(
            refs["funder_type"]["scheme"],
            new_rd["is_output_of"][0]["funder_type"]["in_scheme"],
        )
        self.assertEqual(
            refs["relation_type"]["scheme"],
            new_rd["relation"][0]["relation_type"]["in_scheme"],
        )
        self.assertEqual(
            refs["resource_type"]["scheme"],
            new_rd["relation"][0]["entity"]["type"]["in_scheme"],
        )
        self.assertEqual(
            refs["lifecycle_event"]["scheme"],
            new_rd["provenance"][0]["lifecycle_event"]["in_scheme"],
        )
        self.assertEqual(
            refs["preservation_event"]["scheme"],
            new_rd["provenance"][1]["preservation_event"]["in_scheme"],
        )
        self.assertEqual(
            refs["event_outcome"]["scheme"],
            new_rd["provenance"][0]["event_outcome"]["in_scheme"],
        )

    def _assert_label_copied_to_pref_label(self, refs, new_rd):
        self.assertEqual(refs["keyword"]["label"], new_rd["theme"][0].get("pref_label", None))
        self.assertEqual(
            refs["field_of_science"]["label"],
            new_rd["field_of_science"][0].get("pref_label", None),
        )
        self.assertEqual(
            refs["access_type"]["label"],
            new_rd["access_rights"]["access_type"].get("pref_label", None),
        )
        self.assertEqual(
            refs["restriction_grounds"]["label"],
            new_rd["access_rights"]["restriction_grounds"][0].get("pref_label", None),
        )
        self.assertEqual(
            refs["identifier_type"]["label"],
            new_rd["other_identifier"][0]["type"].get("pref_label", None),
        )
        self.assertEqual(
            refs["location"]["label"],
            new_rd["spatial"][0]["place_uri"].get("pref_label", None),
        )
        self.assertEqual(
            refs["file_type"]["label"],
            new_rd["files"][0]["file_type"].get("pref_label", None),
        )
        self.assertEqual(
            refs["use_category"]["label"],
            new_rd["files"][0]["use_category"].get("pref_label", None),
        )
        self.assertEqual(
            refs["use_category"]["label"],
            new_rd["directories"][0]["use_category"].get("pref_label", None),
        )

        self.assertEqual(
            refs["research_infra"]["label"],
            new_rd["infrastructure"][0].get("pref_label", None),
        )
        self.assertEqual(
            refs["contributor_role"]["label"],
            new_rd["creator"][0]["contributor_role"][0].get("pref_label", None),
        )
        self.assertEqual(
            refs["contributor_type"]["label"],
            new_rd["curator"][0]["contributor_type"][0].get("pref_label", None),
        )
        self.assertEqual(
            refs["funder_type"]["label"],
            new_rd["is_output_of"][0]["funder_type"].get("pref_label", None),
        )
        self.assertEqual(
            refs["relation_type"]["label"],
            new_rd["relation"][0]["relation_type"].get("pref_label", None),
        )
        self.assertEqual(
            refs["resource_type"]["label"],
            new_rd["relation"][0]["entity"]["type"].get("pref_label", None),
        )
        self.assertEqual(
            refs["lifecycle_event"]["label"],
            new_rd["provenance"][0]["lifecycle_event"].get("pref_label", None),
        )
        self.assertEqual(
            refs["preservation_event"]["label"],
            new_rd["provenance"][1]["preservation_event"].get("pref_label", None),
        )
        self.assertEqual(
            refs["event_outcome"]["label"],
            new_rd["provenance"][0]["event_outcome"].get("pref_label", None),
        )

    def _assert_label_copied_to_title(self, refs, new_rd):
        required_langs = dict(
            (lang, val)
            for lang, val in refs["language"]["label"].items()
            if lang in ["fi", "sv", "en", "und"]
        )
        self.assertEqual(required_langs, new_rd["language"][0].get("title", None))
        self.assertEqual(
            refs["license"]["label"],
            new_rd["access_rights"]["license"][0].get("title", None),
        )

    def _assert_label_copied_to_name(self, refs, new_rd):
        self.assertEqual(
            refs["organization"]["label"],
            new_rd["is_output_of"][0]["source_organization"][0].get("name", None),
        )
        self.assertEqual(
            refs["organization"]["label"],
            new_rd["is_output_of"][0]["has_funding_agency"][0].get("name", None),
        )
        self.assertEqual(
            refs["organization"]["label"],
            new_rd["other_identifier"][0]["provider"].get("name", None),
        )
        self.assertEqual(
            refs["organization"]["label"],
            new_rd["contributor"][0]["member_of"].get("name", None),
        )
        self.assertEqual(
            refs["organization"]["label"],
            new_rd["creator"][0]["member_of"].get("name", None),
        )
        self.assertEqual(
            refs["organization"]["label"],
            new_rd["curator"][0]["is_part_of"].get("name", None),
        )
        self.assertEqual(
            refs["organization"]["label"],
            new_rd["publisher"]["is_part_of"].get("name", None),
        )
        self.assertEqual(
            refs["organization"]["label"],
            new_rd["rights_holder"][0]["is_part_of"].get("name", None),
        )

    def test_refdata_sub_org_main_org_population(self):
        # Test parent org gets populated when sub org is from ref data and user has not provided is_part_of relation
        self.cr_test_data["research_dataset"]["publisher"] = {
            "@type": "Organization",
            "identifier": "10076-A800",
        }
        response = self.client.post("/rest/v2/datasets", self.cr_test_data, format="json")

        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)
        self.assertEqual("is_part_of" in response.data["research_dataset"]["publisher"], True)
        self.assertEqual(
            "http://uri.suomi.fi/codelist/fairdata/organization/code/10076",
            response.data["research_dataset"]["publisher"]["is_part_of"]["identifier"],
        )
        self.assertTrue(
            response.data["research_dataset"]["publisher"]["is_part_of"].get("name", False)
        )

        # Test parent org does not get populated when sub org is from ref data and user has provided is_part_of relation
        self.cr_test_data["research_dataset"]["publisher"] = {
            "@type": "Organization",
            "identifier": "10076-A800",
            "is_part_of": {
                "@type": "Organization",
                "identifier": "test_id",
                "name": {"und": "test_name"},
            },
        }
        response = self.client.post("/rest/v2/datasets", self.cr_test_data, format="json")

        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)
        self.assertEqual("is_part_of" in response.data["research_dataset"]["publisher"], True)
        self.assertEqual(
            "test_id",
            response.data["research_dataset"]["publisher"]["is_part_of"]["identifier"],
        )
        self.assertEqual(
            "test_name",
            response.data["research_dataset"]["publisher"]["is_part_of"]["name"]["und"],
        )

        # Test nothing happens when org is a parent org
        self.cr_test_data["research_dataset"]["publisher"] = {
            "@type": "Organization",
            "identifier": "10076",
        }
        response = self.client.post("/rest/v2/datasets", self.cr_test_data, format="json")

        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)
        self.assertEqual("is_part_of" not in response.data["research_dataset"]["publisher"], True)
