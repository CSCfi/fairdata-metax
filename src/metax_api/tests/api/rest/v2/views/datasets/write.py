# This file is part of the Metax API service
#
# Copyright 2017-2020 Ministry of Education and Culture, Finland
#
# :author: CSC - IT Center for Science Ltd., Espoo Finland <servicedesk@csc.fi>
# :license: MIT

from copy import deepcopy

from django.conf import settings as django_settings
from django.core.management import call_command
from rest_framework import status
from rest_framework.test import APITestCase

from metax_api.models import CatalogRecordV2, DataCatalog
from metax_api.services import RabbitMQService
from metax_api.tests.utils import TestClassUtils, test_data_file_path

CR = CatalogRecordV2

VALIDATE_TOKEN_URL = django_settings.VALIDATE_TOKEN_URL
END_USER_ALLOWED_DATA_CATALOGS = django_settings.END_USER_ALLOWED_DATA_CATALOGS
LEGACY_CATALOGS = django_settings.LEGACY_CATALOGS
IDA_CATALOG = django_settings.IDA_DATA_CATALOG_IDENTIFIER
EXT_CATALOG = django_settings.EXT_DATA_CATALOG_IDENTIFIER
DFT_CATALOG = django_settings.DFT_DATA_CATALOG_IDENTIFIER


class CatalogRecordApiWriteCommon(APITestCase, TestClassUtils):
    """
    Common class for write tests, inherited by other write test classes
    """

    def setUp(self):
        """
        Reloaded for every test case
        """
        call_command("loaddata", test_data_file_path, verbosity=0)
        catalog_record_from_test_data = self._get_object_from_test_data("catalogrecord")
        self.preferred_identifier = catalog_record_from_test_data["research_dataset"][
            "preferred_identifier"
        ]
        self.identifier = catalog_record_from_test_data["identifier"]
        self.pk = catalog_record_from_test_data["id"]

        """
        New data that is sent to the server for POST, PUT, PATCH requests. Modified
        slightly as approriate for different purposes
        """
        self.cr_test_data = self._get_new_test_cr_data()
        self.cr_test_data["research_dataset"]["publisher"] = {
            "@type": "Organization",
            "name": {"und": "Testaaja"},
        }
        self.cr_test_data["research_dataset"]["issued"] = "2010-01-01"

        self.cr_att_test_data = self._get_new_test_cr_data(cr_index=14, dc_index=5)
        self.cr_test_data_new_identifier = self._get_new_test_cr_data_with_updated_identifier()
        self.cr_full_ida_test_data = self._get_new_full_test_ida_cr_data()
        self.cr_full_att_test_data = self._get_new_full_test_att_cr_data()

        self._use_http_authorization()

    #
    #
    #
    # internal helper methods
    #
    #
    #

    def _get_new_test_cr_data(self, cr_index=0, dc_index=0, c_index=0):
        dc = self._get_object_from_test_data("datacatalog", requested_index=dc_index)
        catalog_record_from_test_data = self._get_object_from_test_data(
            "catalogrecord", requested_index=cr_index
        )

        if (
            dc["catalog_json"]["research_dataset_schema"] == "ida"
            and "remote_resources" in catalog_record_from_test_data["research_dataset"]
        ):
            self.fail(
                "Cannot generate the requested test catalog record since requested data catalog is indicates ida "
                "schema and the requested catalog record is having remote resources, which is not allowed"
            )

        if dc["catalog_json"]["research_dataset_schema"] == "att" and (
            "files" in catalog_record_from_test_data["research_dataset"]
            or "directories" in catalog_record_from_test_data["research_dataset"]
        ):
            self.fail(
                "Cannot generate the requested test catalog record since requested data catalog is indicates att "
                "schema and the requested catalog record is having files or directories, which is not allowed"
            )

        catalog_record_from_test_data.update(
            {
                "contract": self._get_object_from_test_data("contract", requested_index=c_index),
                "data_catalog": dc,
            }
        )
        catalog_record_from_test_data["research_dataset"].update(
            {
                "creator": [
                    {
                        "@type": "Person",
                        "name": "Teppo Testaaja",
                        "member_of": {
                            "@type": "Organization",
                            "name": {"fi": "Mysterious Organization"},
                        },
                    }
                ],
                "curator": [
                    {
                        "@type": "Person",
                        "name": "Default Owner",
                        "member_of": {
                            "@type": "Organization",
                            "name": {"fi": "Mysterious Organization"},
                        },
                    }
                ],
            }
        )
        catalog_record_from_test_data["research_dataset"].pop("preferred_identifier", None)
        catalog_record_from_test_data["research_dataset"].pop("metadata_version_identifier", None)
        catalog_record_from_test_data.pop("identifier", None)
        return catalog_record_from_test_data

    def _get_new_test_cr_data_with_updated_identifier(self):
        catalog_record_from_test_data = self._get_new_test_cr_data()
        # catalog_record_from_test_data['research_dataset'].update({
        #     "metadata_version_identifier": "urn:nbn:fi:att:5cd4d4f9-9583-422e-9946-990c8ea96781"
        # })
        return catalog_record_from_test_data

    def _get_new_full_test_ida_cr_data(self):
        """
        Returns one of the fuller generated test datasets
        """
        catalog_record_from_test_data = self._get_object_from_test_data(
            "catalogrecord", requested_index=11
        )
        data_catalog_from_test_data = self._get_object_from_test_data(
            "datacatalog", requested_index=0
        )
        return self._get_new_full_test_cr_data(
            catalog_record_from_test_data, data_catalog_from_test_data
        )

    def _get_new_full_test_att_cr_data(self):
        """
        Returns one of the fuller generated test datasets
        """
        catalog_record_from_test_data = self._get_object_from_test_data(
            "catalogrecord", requested_index=23
        )
        data_catalog_from_test_data = self._get_object_from_test_data(
            "datacatalog", requested_index=5
        )
        return self._get_new_full_test_cr_data(
            catalog_record_from_test_data, data_catalog_from_test_data
        )

    def _get_new_full_test_cr_data(self, cr_from_test_data, dc_from_test_data):
        cr_from_test_data.update(
            {
                "contract": self._get_object_from_test_data("contract", requested_index=0),
                "data_catalog": dc_from_test_data,
            }
        )
        cr_from_test_data["research_dataset"].pop("metadata_version_identifier")
        cr_from_test_data["research_dataset"].pop("preferred_identifier")
        cr_from_test_data.pop("identifier")
        return cr_from_test_data


class CatalogRecordApiWriteCreateTests(CatalogRecordApiWriteCommon):
    #
    #
    #
    # create apis
    #
    #
    #
    def setUp(self):
        super().setUp()

    def test_create_catalog_record_using_pid_type(self):
        # Test with pid_type = urn
        self.cr_test_data["research_dataset"]["preferred_identifier"] = ""
        response = self.client.post(
            "/rest/v2/datasets?pid_type=urn", self.cr_test_data, format="json"
        )
        self.assertTrue(
            response.data["research_dataset"]["preferred_identifier"].startswith("urn:")
        )

        # Test with pid_type = doi AND not ida catalog
        self.cr_test_data["research_dataset"]["preferred_identifier"] = ""
        response = self.client.post(
            "/rest/v2/datasets?pid_type=doi", self.cr_test_data, format="json"
        )
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST, response.data)

        # Create ida data catalog
        dc = self._get_object_from_test_data("datacatalog", requested_index=0)
        dc_id = IDA_CATALOG
        dc["catalog_json"]["identifier"] = dc_id
        self.client.post("/rest/v2/datacatalogs", dc, format="json")
        # Test with pid_type = doi AND ida catalog
        self.cr_test_data["research_dataset"]["preferred_identifier"] = ""
        self.cr_test_data["data_catalog"] = IDA_CATALOG
        response = self.client.post(
            "/rest/v2/datasets?pid_type=doi", self.cr_test_data, format="json"
        )
        self.assertTrue(
            response.data["research_dataset"]["preferred_identifier"].startswith("doi:10.")
        )

        # Test with pid_type = not_known
        self.cr_test_data["research_dataset"]["preferred_identifier"] = ""
        response = self.client.post(
            "/rest/v2/datasets?pid_type=not_known", self.cr_test_data, format="json"
        )
        self.assertTrue(
            response.data["research_dataset"]["preferred_identifier"].startswith("urn:")
        )

        # Test without pid_type
        self.cr_test_data["research_dataset"]["preferred_identifier"] = ""
        response = self.client.post("/rest/v2/datasets", self.cr_test_data, format="json")
        self.assertTrue(
            response.data["research_dataset"]["preferred_identifier"].startswith("urn:")
        )


class CatalogRecordApiWriteIdentifierUniqueness(CatalogRecordApiWriteCommon):
    """
    Tests related to checking preferred_identifier uniqueness. Topics of interest:
    - when saving to ATT catalog, preferred_identifier already existing in the ATT
      catalog is fine (other versions of the same record).
    - when saving to ATT catalog, preferred_identifier already existing in OTHER
      catalogs is an error (ATT catalog should only have "new" records).
    - when saving to OTHER catalogs than ATT, preferred_identifier already existing
      in any other catalog is fine (the same record can be harvested from multiple
      sources).
    """

    #
    # create operations
    #

    def test_create_catalog_record_preferred_identifier_exists_in_another_catalog(self):
        """
        preferred_identifier existing in another data catalog is not an error.
        """
        unique_identifier = self._set_preferred_identifier_to_record(pk=1, catalog_id=1)
        self.cr_test_data["research_dataset"]["preferred_identifier"] = unique_identifier

        # different catalog, should be OK (not ATT catalog, so preferred_identifier being saved
        # can exist in other catalogs)
        self.cr_test_data["data_catalog"] = 3

        response = self.client.post("/rest/v2/datasets", self.cr_test_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)

    # #
    # # update operations
    # #

    def test_update_catalog_record_preferred_identifier_exists_in_another_catalog_1(
        self,
    ):
        """
        preferred_identifier existing in another data catalog is not an error.

        Test PATCH, when data_catalog of the record being updated is already
        different than another record's which has the same identifier.
        """
        unique_identifier = self._set_preferred_identifier_to_record(pk=1, catalog_id=1)

        cr = CatalogRecordV2.objects.get(pk=3)
        cr.data_catalog_id = 3
        cr.save()

        data = self.client.get("/rest/v2/datasets/3").data
        data["research_dataset"]["preferred_identifier"] = unique_identifier

        response = self.client.patch("/rest/v2/datasets/3", data, format="json")
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)

    #
    # helpers
    #

    def _set_preferred_identifier_to_record(self, pk=None, catalog_id=None):
        """
        Set preferred_identifier to an existing record to a value, and return that value,
        which will then be used by the test to create or update another record.
        """
        unique_identifier = "im unique yo"
        cr = CatalogRecordV2.objects.get(pk=pk)
        cr.research_dataset["preferred_identifier"] = unique_identifier
        cr.data_catalog_id = catalog_id
        cr.force_save()
        cr._handle_preferred_identifier_changed()
        return unique_identifier


class CatalogRecordApiWriteDatasetSchemaSelection(CatalogRecordApiWriteCommon):
    #
    #
    #
    # dataset schema selection related
    #
    #
    #

    def _set_data_catalog_schema_to_harvester(self):
        dc = DataCatalog.objects.get(pk=1)
        dc.catalog_json["research_dataset_schema"] = "harvester"
        dc.save()

    def setUp(self):
        super().setUp()
        self._set_data_catalog_schema_to_harvester()
        self.cr_test_data["research_dataset"]["preferred_identifier"] = "unique_pid"

    def test_catalog_record_draft_is_validated_with_draft_schema(self):
        """
        Ensure that non-published datasets are always validated with draft schema regardless
        of the chosen datacatalog.
        """
        cr = deepcopy(self.cr_test_data)
        cr["data_catalog"] = 2  # ida catalog
        response = self.client.post("/rest/v2/datasets?draft", cr, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)

        cr = response.data
        cr["research_dataset"]["remote_resources"] = [
            {"title": "title", "use_category": {"identifier": "source"}}
        ]

        response = self.client.put(f'/rest/v2/datasets/{cr["id"]}', cr, format="json")
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)

        # ensure that dataset is validated against correct schema when publishing
        response = self.client.post(
            f'/rpc/v2/datasets/publish_dataset?identifier={cr["id"]}', format="json"
        )
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST, response.data)


class CatalogRecordApiWriteUpdateTests(CatalogRecordApiWriteCommon):
    #
    #
    # update apis PUT
    #
    #

    def test_change_datacatalog_ATT_to_IDA(self):
        cr = self._get_new_full_test_att_cr_data()

        # create ATT data catalog
        dc_att = self._get_object_from_test_data("datacatalog", 4)
        dc_att["catalog_json"]["identifier"] = "urn:nbn:fi:att:data-catalog-att"
        dc_att = self.client.post("/rest/v2/datacatalogs", dc_att, format="json").data

        # create IDA data catalog
        dc_ida = self._get_object_from_test_data("datacatalog")
        dc_ida["catalog_json"]["identifier"] = "urn:nbn:fi:att:data-catalog-ida"
        dc_ida = self.client.post("/rest/v2/datacatalogs", dc_ida, format="json").data

        # create ATT catalog record
        cr["data_catalog"] = dc_att
        cr_att = self.client.post("/rest/v2/datasets", cr, format="json").data

        # change data catalog to IDA
        cr_id = cr_att["id"]
        cr_att["data_catalog"]["id"] = dc_ida["id"]
        cr_att["data_catalog"]["identifier"] = dc_ida["catalog_json"]["identifier"]
        cr_ida = self.client.put("/rest/v2/datasets/%d" % cr_id, cr_att, format="json")

        self.assertEqual(cr_ida.status_code, status.HTTP_200_OK, cr_ida)
        self.assertTrue(
            not all(
                item in cr_ida.data["research_dataset"].keys()
                for item in ["remote_resources", "total_remote_resources_byte_size"]
            )
        )
        self.assertTrue("metadata_version_identifier" in cr_ida.data["research_dataset"].keys())

        files = {
            "files": [
                {
                    "title": "File metadata title 1",
                    "file_type": {
                        "in_scheme": "http://uri.suomi.fi/codelist/fairdata/file_type",
                        "identifier": "http://uri.suomi.fi/codelist/fairdata/file_type/code/text",
                        "pref_label": {"en": "Text", "fi": "Teksti", "und": "Teksti"},
                    },
                    "identifier": "pid:urn:1",
                    "use_category": {
                        "in_scheme": "http://uri.suomi.fi/codelist/fairdata/use_category",
                        "identifier": "http://uri.suomi.fi/codelist/fairdata/use_category/code/source",
                        "pref_label": {
                            "en": "Source material",
                            "fi": "Lähdeaineisto",
                            "und": "Lähdeaineisto",
                        },
                    },
                }
            ]
        }
        cr_ida = self.client.post("/rest/v2/datasets/%d/files" % cr_id, files, format="json")

        self.assertEqual(cr_ida.status_code, status.HTTP_200_OK, cr_ida.data)
        self.assertTrue(cr_ida.data["files_added"] == 1, "Must indicate of one file addition")

        response = self.client.get("/rest/v2/datasets/%d?include_user_metadata" % cr_id).data
        self.assertTrue(
            len(response["research_dataset"]["files"]) == 1,
            "Dataset must contain one file",
        )


class CatalogRecordApiWriteAlternateRecords(CatalogRecordApiWriteCommon):

    """
    Tests related to handling alternate records: Records which have the same
    preferred_identifier, but are in different data catalogs.

    The tricky part here is that, in catalogs which support versioning, changing preferred_identifier
    will leave the old record in its existing alternate_record_set, and the new version will have
    the changed preferred_identifier, which may or may not be placed into a different
    alternate_record_set.
    """

    def setUp(self):
        super(CatalogRecordApiWriteAlternateRecords, self).setUp()
        self.preferred_identifier = self._set_preferred_identifier_to_record(pk=1, data_catalog=1)
        self.cr_test_data["research_dataset"]["preferred_identifier"] = self.preferred_identifier
        self.cr_test_data["data_catalog"] = None

    def _set_preferred_identifier_to_record(self, pk=1, data_catalog=1):
        """
        Set preferred_identifier to an existing record to a value, and return that value,
        which will then be used by the test to create or update a record.

        Note that if calling this method several times, this will also create an
        alternate_record_set (by calling _handle_preferred_identifier_changed()).
        """
        unique_identifier = "im unique yo"
        cr = CatalogRecordV2.objects.get(pk=pk)
        cr.research_dataset["preferred_identifier"] = unique_identifier
        cr.data_catalog_id = data_catalog
        cr.force_save()
        cr._handle_preferred_identifier_changed()
        return unique_identifier

    def test_dataset_version_lists_removed_records(self):

        # create version2 of a record
        response = self.client.post(
            "/rpc/v2/datasets/create_new_version?identifier=1", format="json"
        )
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)

        new_version_id = response.data["id"]

        # publish the new version
        response = self.client.post(
            f"/rpc/v2/datasets/publish_dataset?identifier={new_version_id}",
            format="json",
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)

        # delete version2
        response = self.client.delete(f"/rest/v2/datasets/{new_version_id}", format="json")
        self.assertEqual(response.status_code, status.HTTP_204_NO_CONTENT, response.data)

        # check date_removed is listed and not None in deleted version
        response = self.client.get("/rest/v2/datasets/1", format="json")
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)

        self.assertTrue(response.data["dataset_version_set"][0].get("date_removed"))
        self.assertTrue(response.data["dataset_version_set"][0].get("date_removed") is not None)
        self.assertFalse(response.data["dataset_version_set"][1].get("date_removed"))

    def test_allow_metadata_changes_after_deprecation(self):
        """
        For deprecated datasets metadata changes are still allowed. Changing user metadata for files that
        are marked as removed (caused the deprecation) is not possible.
        """
        response = self.client.get("/rest/v2/datasets/1?include_user_metadata")
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        cr = response.data

        response = self.client.delete("/rest/v2/files/1")
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)

        # after the dataset is deprecated, metadata updates should still be ok
        cr["research_dataset"]["description"] = {
            "en": "Updating new description for deprecated dataset should not create any problems"
        }

        response = self.client.put("/rest/v2/datasets/%s" % cr["id"], cr, format="json")
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        self.assertTrue(
            "new description" in response.data["research_dataset"]["description"]["en"],
            "description field should be updated",
        )

        file_changes = {"files": [cr["research_dataset"]["files"][0]]}

        file_changes["files"][0]["title"] = "Brand new title 1"

        response = self.client.put(
            "/rest/v2/datasets/%s/files/user_metadata" % cr["id"],
            file_changes,
            format="json",
        )
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST, response.data)
        self.assertEqual(
            "The following files are not" in response.data["detail"][0],
            True,
            response.data,
        )


class CatalogRecordApiWriteLegacyDataCatalogs(CatalogRecordApiWriteCommon):

    """
    Tests related to legacy data catalogs.
    """

    def setUp(self):
        """
        Create a test-datacatalog that plays the role of a legacy catalog.
        """
        super().setUp()
        dc = DataCatalog.objects.filter(catalog_json__research_dataset_schema="att").first()
        dc.catalog_json["identifier"] = LEGACY_CATALOGS[0]
        dc.force_save()
        del self.cr_test_data["research_dataset"]["files"]
        del self.cr_test_data["research_dataset"]["total_files_byte_size"]

    def test_legacy_catalog_pid_must_be_provided(self):
        # pid cant be empty string
        self.cr_test_data["data_catalog"] = LEGACY_CATALOGS[0]
        self.cr_test_data["research_dataset"]["preferred_identifier"] = ""
        response = self.client.post("/rest/v2/datasets", self.cr_test_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST, response.data)

        # pid cant be omitted
        del self.cr_test_data["research_dataset"]["preferred_identifier"]
        response = self.client.post("/rest/v2/datasets", self.cr_test_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST, response.data)


class CatalogRecordApiWriteOwnerFields(CatalogRecordApiWriteCommon):

    """
    Owner-fields related tests:
    metadata_owner_org
    metadata_provider_org
    metadata_provider_user
    """

    def test_metadata_owner_org_is_copied_from_metadata_provider_org(self):
        """
        If field metadata_owner_org is omitted when creating or updating a ds, its value should be copied
        from field metadata_provider_org.
        """

        # create
        cr = self.client.get("/rest/v2/datasets/1", format="json").data
        cr.pop("id")
        cr.pop("identifier")
        cr.pop("metadata_owner_org")
        cr["research_dataset"].pop("preferred_identifier")
        response = self.client.post("/rest/v2/datasets", cr, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)
        self.assertEqual(
            response.data["metadata_owner_org"], response.data["metadata_provider_org"]
        )

        # update to null - update is prevented
        cr = self.client.get("/rest/v2/datasets/1", format="json").data
        original = cr["metadata_owner_org"]
        cr["metadata_owner_org"] = None
        response = self.client.put("/rest/v2/datasets/1", cr, format="json")
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        self.assertEqual(response.data["metadata_owner_org"], original)

        # update with patch, where metadata_owner_org field is absent - value is not reverted back
        # to metadata_provider_org
        response = self.client.patch(
            "/rest/v2/datasets/1", {"metadata_owner_org": "abc"}, format="json"
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        response = self.client.patch("/rest/v2/datasets/1", {"contract": 1}, format="json")
        self.assertEqual(response.data["metadata_owner_org"], "abc")

    def test_metadata_provider_org_is_readonly_after_creating(self):
        cr = self.client.get("/rest/v2/datasets/1", format="json").data
        original = cr["metadata_provider_org"]
        cr["metadata_provider_org"] = "changed"
        response = self.client.put("/rest/v2/datasets/1", cr, format="json")
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        self.assertEqual(response.data["metadata_provider_org"], original)

    def test_metadata_provider_user_is_readonly_after_creating(self):
        cr = self.client.get("/rest/v2/datasets/1", format="json").data
        original = cr["metadata_provider_user"]
        cr["metadata_provider_user"] = "changed"
        response = self.client.put("/rest/v2/datasets/1", cr, format="json")
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        self.assertEqual(response.data["metadata_provider_user"], original)


class CatalogRecordExternalServicesAccess(CatalogRecordApiWriteCommon):

    """
    Testing access of services to external catalogs with harvested flag and vice versa.
    """

    def setUp(self):
        """
        Create a test-datacatalog that plays the role as a external catalog.
        """
        super().setUp()

        self.dc = DataCatalog.objects.filter(catalog_json__research_dataset_schema="att").first()
        self.dc.catalog_json["identifier"] = EXT_CATALOG
        self.dc.catalog_json["harvested"] = True
        self.dc.catalog_record_services_create = "external"
        self.dc.catalog_record_services_edit = "external"
        self.dc.force_save()

        self.cr_test_data["data_catalog"] = self.dc.catalog_json["identifier"]
        del self.cr_test_data["research_dataset"]["files"]
        del self.cr_test_data["research_dataset"]["total_files_byte_size"]

        self._use_http_authorization(
            username=django_settings.API_EXT_USER["username"],
            password=django_settings.API_EXT_USER["password"],
        )

    def test_external_service_can_not_add_catalog_record_to_other_catalog(self):
        dc = self._get_object_from_test_data("datacatalog", requested_index=1)
        self.cr_test_data["data_catalog"] = dc["catalog_json"]["identifier"]
        self.cr_test_data["research_dataset"]["preferred_identifier"] = "temp-pid"
        response = self.client.post("/rest/v2/datasets", self.cr_test_data, format="json")

        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN, response.data)

class CatalogRecordRabbitMQPublish(CatalogRecordApiWriteCommon):
    """
    Testing if RabbitMQ messages are published to correct exchanges with correct routing keys 
    when creating, updating, or deleting a catalog record.
    Uses a dummy RabbitMQService.
    """

    def setUp(self):
        super().setUp()

    def _check_rabbitmq_queue(self, routing_key, publish_to_etsin, publish_to_ttv):
        """
        Checks if a message with a given routing key exists in the correct exchange
        in the dummy RabbitMQService queue
        """
        messages_str = ''.join(str(message) for message in RabbitMQService.messages)
        assert_str_etsin = f"'routing_key': '{routing_key}', 'exchange': 'datasets'"
        assert_str_ttv = f"'routing_key': '{routing_key}', 'exchange': 'ttv-datasets'"

        self.assertEqual(assert_str_etsin in messages_str, publish_to_etsin)
        self.assertEqual(assert_str_ttv in messages_str, publish_to_ttv)



    def test_rabbitmq_publish(self):
        """
        Creates four different data catalogs and creates, updates, and deletes
        catalog records in them. Checks that RabbitMQ messages are published
        in correct exchanges with correct routing keys.
        """
        param_list = [(True, True), (True, False), (False, True), (False, False)]
        for publish_to_etsin, publish_to_ttv in param_list:
            with self.subTest():
                
                # Create the data catalog
                dc = self._get_object_from_test_data("datacatalog", 4)
                dc_id = f"urn:nbn:fi:att:data-catalog-att-{publish_to_etsin}-{publish_to_ttv}"
                dc["catalog_json"]["identifier"] = dc_id
                dc["publish_to_etsin"] = publish_to_etsin
                dc["publish_to_ttv"] = publish_to_ttv
                dc = self.client.post("/rest/v2/datacatalogs", dc, format="json").data

                # Create the catalog record
                cr = self._get_new_full_test_att_cr_data()
                cr["data_catalog"] = dc

                cr = self.client.post("/rest/v2/datasets", cr, format="json").data
                self._check_rabbitmq_queue("create", publish_to_etsin, publish_to_ttv)

                # Empty the queue
                RabbitMQService.messages = []

                # Update the catalog record
                cr["research_dataset"]["description"] = {
                    "en": "Updating the description"
                }

                response = self.client.put(f"/rest/v2/datasets/{cr['id']}", cr, format="json")
                self._check_rabbitmq_queue("update", publish_to_etsin, publish_to_ttv)

                RabbitMQService.messages = []

                # Delete the catalog record
                response = self.client.delete(f"/rest/v2/datasets/{cr['id']}")
                self._check_rabbitmq_queue("delete", publish_to_etsin, publish_to_ttv)

                RabbitMQService.messages = []

