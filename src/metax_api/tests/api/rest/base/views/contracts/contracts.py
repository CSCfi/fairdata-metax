# This file is part of the Metax API service
#
# Copyright 2017-2018 Ministry of Education and Culture, Finland
#
# :author: CSC - IT Center for Science Ltd., Espoo Finland <servicedesk@csc.fi>
# :license: MIT
import json
import responses

from django.core.management import call_command
from django.test import override_settings
from rest_framework import status
from rest_framework.test import APITestCase

from metax_api.models import CatalogRecord, Contract
from metax_api.tests.utils import TestClassUtils, test_data_file_path


class ContractApiReadTestV1(APITestCase, TestClassUtils):
    @classmethod
    def setUpClass(cls):
        """
        Loaded only once for test cases inside this class.
        """
        call_command("loaddata", test_data_file_path, verbosity=0)
        super(ContractApiReadTestV1, cls).setUpClass()

    def setUp(self):
        contract_from_test_data = self._get_object_from_test_data("contract", requested_index=0)
        self.pk = contract_from_test_data["id"]
        self.identifier = contract_from_test_data["contract_json"]["identifier"]
        self._use_http_authorization()

    def test_read_contract_list(self):
        response = self.client.get("/rest/datasets")
        self.assertEqual(response.status_code, status.HTTP_200_OK)

    def test_read_contract_details_by_pk(self):
        response = self.client.get("/rest/contracts/%s" % self.pk)
        self.assertEqual(response.status_code, status.HTTP_200_OK)

    def test_read_contract_details_by_identifier(self):
        response = self.client.get("/rest/contracts/%s" % self.identifier)
        self.assertEqual(response.status_code, status.HTTP_200_OK)

    def test_read_contract_details_not_found(self):
        response = self.client.get("/rest/contracts/shouldnotexist")
        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND)


class ContractApiWriteTestV1(APITestCase, TestClassUtils):
    def setUp(self):
        """
        Reloaded for every test case
        """
        call_command("loaddata", test_data_file_path, verbosity=0)
        self._use_http_authorization()
        contract_from_test_data = self._get_object_from_test_data("contract")
        self.pk = contract_from_test_data["id"]

        """
        New data that is sent to the server for POST, PUT, PATCH requests. Modified
        slightly as approriate for different purposes
        """
        self.test_new_data = self._get_new_test_data()
        self.second_test_new_data = self._get_second_new_test_data()
        self._use_http_authorization()

    def test_create_contract_with_existing_identifier(self):
        self.test_new_data["pk"] = self.pk
        response = self.client.post("/rest/contracts/", self.test_new_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)

        response = self.client.post("/rest/contracts/", self.test_new_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST, response.data)
        self.assertTrue(
            "already exists" in response.data["contract_json"][0],
            "Error regarding dublicated identifier",
        )

    def test_update_contract(self):
        self.test_new_data["pk"] = self.pk
        response = self.client.put(
            "/rest/contracts/%s" % self.pk, self.test_new_data, format="json"
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)

    def test_update_contract_not_found(self):
        response = self.client.put(
            "/rest/contracts/doesnotexist", self.test_new_data, format="json"
        )
        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND)

    def test_add_catalog_record_to_contract(self):
        new_catalog_record = self.client.get("/rest/datasets/1", format="json").data
        new_catalog_record.pop("id")
        new_catalog_record.pop("identifier")
        new_catalog_record["research_dataset"].pop("preferred_identifier")
        new_catalog_record["contract"] = self.pk

        response = self.client.post("/rest/datasets", new_catalog_record, format="json")
        created_catalog_record = response.data

        try:
            self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)
        except Exception:
            print(response.data)
            raise
        self.assertEqual("research_dataset" in created_catalog_record.keys(), True)
        self.assertEqual(created_catalog_record["contract"]["id"], self.pk)

        contract = Contract.objects.get(pk=self.pk)
        try:
            contract.records.get(pk=response.data["id"])
        except CatalogRecord.DoesNotExist:
            raise Exception(
                "The added CatalogRecord should appear in the relation contract.records"
            )

        response = self.client.get("/rest/contracts/%d/datasets" % self.pk)
        self.assertIn(
            created_catalog_record["id"],
            [cr["id"] for cr in response.data],
            "The added CatalogRecord should appear in the results of /contracts/id/datasets",
        )

    def test_delete_contract(self):
        url = "/rest/contracts/%s" % self.pk
        response = self.client.delete(url)
        self.assertEqual(response.status_code, status.HTTP_204_NO_CONTENT)
        response = self.client.get(url)
        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND)

        deleted_contract = None

        try:
            deleted_contract = Contract.objects.get(pk=self.pk)
        except Contract.DoesNotExist:
            pass

        if deleted_contract:
            raise Exception(
                "Deleted Contract should not be retrievable from the default objects table"
            )

        try:
            deleted_contract = Contract.objects_unfiltered.get(pk=self.pk)
        except Contract.DoesNotExist:
            raise Exception("Deleted contract should not be deleted from the db")

        self.assertEqual(
            deleted_contract.removed,
            True,
            "Deleted contract should be marked removed in the db",
        )
        self.assertEqual(
            deleted_contract.date_modified,
            deleted_contract.date_removed,
            "date_modified should be updated",
        )

    def test_delete_contract_catalog_records_are_marked_removed(self):
        # add two new records to contract
        new_catalog_record = self._get_new_catalog_record_test_data()
        new_catalog_record["contract"] = self.pk
        self.client.post("/rest/datasets", new_catalog_record, format="json")
        self.client.post("/rest/datasets", new_catalog_record, format="json")

        self.client.delete("/rest/contracts/%s" % self.pk)
        contract = Contract.objects_unfiltered.get(pk=self.pk)
        related_crs = contract.records(manager="objects_unfiltered").all()
        response_get_1 = self.client.get("/rest/datasets/%d" % related_crs[0].id)
        self.assertEqual(
            response_get_1.status_code,
            status.HTTP_404_NOT_FOUND,
            "CatalogRecords of deleted contracts should not be retrievable through the api",
        )
        response_get_2 = self.client.get("/rest/datasets/%d" % related_crs[1].id)
        self.assertEqual(
            response_get_2.status_code,
            status.HTTP_404_NOT_FOUND,
            "CatalogRecords of deleted contracts should not be retrievable through the api",
        )

        for cr in related_crs:
            self.assertEqual(
                cr.removed,
                True,
                "Related CatalogRecord objects should be marked as removed",
            )

    def test_deleted_catalog_record_is_not_listed_in_contract_datasets_api(self):
        deleted_id = 1
        self.client.delete("/rest/datasets/%d" % deleted_id)
        response = self.client.get("/rest/contracts/%d/datasets" % self.pk)
        self.assertNotIn(
            deleted_id,
            [cr["id"] for cr in response.data],
            "The deleted CatalogRecord should not appear in the results of /contracts/id/datasets",
        )

    def _get_new_test_data(self):
        return {
            "contract_json": {
                "title": "Title of new contract",
                "identifier": "optional-identifier-new",
                "quota": 111204,
                "created": "2014-01-17T08:19:58Z",
                "modified": "2014-01-17T08:19:58Z",
                "description": "Description of unknown length",
                "contact": [
                    {
                        "name": "Contact Name",
                        "phone": "+358501231234",
                        "email": "contact.email@csc.fi",
                    }
                ],
                "organization": {
                    "organization_identifier": "1234567abc",
                    "name": "Mysterious organization",
                },
                "related_service": [{"identifier": "local:service:id", "name": "Name of Service"}],
                "validity": {"start_date": "2014-01-17"},
            }
        }

    def _get_second_new_test_data(self):
        return {
            "contract_json": {
                "title": "Title of second contract",
                "identifier": "optional-identifier-for-second",
                "quota": 111204,
                "created": "2014-01-17T08:19:58Z",
                "modified": "2014-01-17T08:19:58Z",
                "description": "Description of unknown length",
                "contact": [
                    {
                        "name": "Contact Name",
                        "phone": "+358501231234",
                        "email": "contact.email@csc.fi",
                    }
                ],
                "organization": {
                    "organization_identifier": "1234567abc",
                    "name": "Mysterious organization",
                },
                "related_service": [{"identifier": "local:service:id", "name": "Name of Service"}],
                "validity": {"start_date": "2014-01-17"},
            }
        }

    def _get_new_catalog_record_test_data(self):
        catalog_record_from_test_data = self._get_object_from_test_data(
            "catalogrecord", requested_index=0
        )
        return {
            "identifier": "http://urn.fi/urn:nbn:fi:iiidentifier",
            "data_catalog": self._get_object_from_test_data("datacatalog", requested_index=0),
            "research_dataset": {
                "modified": "2014-01-17T08:19:58Z",
                "version_notes": ["This version contains changes to x and y."],
                "title": {"en": "Wonderful Title"},
                "description": [
                    {
                        "en": "A descriptive description describing the contents of this dataset. Must be descriptive."
                    }
                ],
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
                        "@type": "Organization",
                        "name": {"en": "Curator org", "fi": "Organisaatio"},
                    }
                ],
                "language": [{"identifier": "http://lexvo.org/id/iso639-3/aar"}],
                "total_files_byte_size": 1024,
                "files": catalog_record_from_test_data["research_dataset"]["files"],
            },
        }


class ContractApiSyncFromV3(APITestCase, TestClassUtils):
    def setUp(self):
        """
        Reloaded for every test case
        """
        call_command("loaddata", test_data_file_path, verbosity=0)
        self._use_http_authorization()
        contract_from_test_data = self._get_object_from_test_data("contract")
        self.pk = contract_from_test_data["id"]
        self.identifier = contract_from_test_data["contract_json"]["identifier"]

        """
        New data that is sent to the server for POST, PUT, PATCH requests. Modified
        slightly as approriate for different purposes
        """
        self._use_http_authorization(username="metax_service")

    def test_sync_v3_contracts_create(self):
        old_count = Contract.objects.count()
        data = self._get_v3_sync_payload()

        # No id, and identifier does not match existing contract, so new contract is created
        data[0]["id"] = None
        res = self.client.post("/rest/v2/contracts/sync_from_v3", data, format="json")
        self.assertEqual(res.status_code, status.HTTP_200_OK)
        self.assertIsNotNone(res.json()[0]["id"])

        identifier = data[0]["contract_json"]["identifier"]
        res = self.client.get(f"/rest/v2/contracts/{identifier}", format="json")
        self.assertEqual(res.status_code, status.HTTP_200_OK)

        expected_data = {**data[0]}
        expected_data.pop("id")
        expected_data.pop("date_created")
        expected_data.pop("date_modified")
        expected_data.pop("date_removed")
        self.assertDictContainsSubset(expected_data, res.json())

        new_count = Contract.objects.count()
        self.assertEqual(new_count, old_count + 1)

    def test_sync_v3_contracts_update_by_pk(self):
        old_count = Contract.objects.count()
        data = self._get_v3_sync_payload()

        # Update existing contract based on id
        data[0]["id"] = self.pk
        res = self.client.post("/rest/v2/contracts/sync_from_v3", data, format="json")
        self.assertEqual(res.status_code, status.HTTP_200_OK)

        identifier = data[0]["contract_json"]["identifier"]
        res = self.client.get(f"/rest/v2/contracts/{identifier}", format="json")
        self.assertEqual(res.status_code, status.HTTP_200_OK)

        expected_data = {**data[0]}
        expected_data.pop("date_created")
        expected_data.pop("date_modified")
        expected_data.pop("date_removed")
        self.assertDictContainsSubset(expected_data, res.json())

        new_count = Contract.objects.count()
        self.assertEqual(new_count, old_count)

    def test_sync_v3_contracts_update_by_identifier(self):
        old_count = Contract.objects.count()
        data = self._get_v3_sync_payload()

        # No id, but identifier matches existing contract
        data[0]["id"] = None
        data[0]["contract_json"]["identifier"] = self.identifier
        res = self.client.post("/rest/v2/contracts/sync_from_v3", data, format="json")
        self.assertEqual(res.status_code, status.HTTP_200_OK)

        identifier = data[0]["contract_json"]["identifier"]
        res = self.client.get(f"/rest/v2/contracts/{identifier}", format="json")
        self.assertEqual(res.status_code, status.HTTP_200_OK)

        expected_data = {**data[0], "id": self.pk}
        expected_data.pop("date_created")
        expected_data.pop("date_modified")
        expected_data.pop("date_removed")
        self.assertDictContainsSubset(expected_data, res.json())

        new_count = Contract.objects.count()
        self.assertEqual(new_count, old_count)

    def test_sync_v3_contracts_deleted(self):
        data = self._get_v3_sync_payload()
        data[0]["removed"] = True
        data[0]["date_removed"] = "2024-12-19T20:00:00Z"
        res = self.client.post("/rest/v2/contracts/sync_from_v3", data, format="json")
        self.assertEqual(res.status_code, status.HTTP_200_OK)

        identifier = data[0]["contract_json"]["identifier"]
        res = self.client.get(f"/rest/v2/contracts/{identifier}?removed=true", format="json")
        self.assertEqual(res.status_code, status.HTTP_200_OK)

        expected_data = {**data[0]}
        expected_data.pop("date_created")
        expected_data.pop("date_modified")
        expected_data.pop("date_removed")
        self.assertDictContainsSubset(expected_data, res.json())
        self.assertIsNotNone(res.data["date_removed"])

    def _get_v3_sync_payload(self):
        return [
            {
                "id": 1001,
                "date_created": "2022-09-19T11:42:58Z",
                "date_modified": "2022-09-20T09:55:06Z",
                "contract_json": {
                    "title": "Testisopimus",
                    "description": "Description of the contract",
                    "quota": 1234567890,
                    "created": "2022-09-19T00:00:00Z",
                    "modified": "2022-09-20T09:55:05Z",
                    "organization": {
                        "name": "Testiopisto",
                        "organization_identifier": "https://www.example.com/testi",
                    },
                    "validity": {"start_date": "2022-09-19"},
                    "contact": [
                        {
                            "name": "Testiyhteys",
                            "email": "testi@example.com",
                            "phone": "+358-10-12345678",
                        },
                        {"name": "Toinen", "email": "toinen@example.com", "phone": ""},
                    ],
                    "related_service": [
                        {"identifier": "urn:nbn:fi:att:file-storage-pas", "name": "Fairdata-PAS"}
                    ],
                    "identifier": "urn:uuid:0d32393b-1be2-454e-98ff-ffb1da56343c",
                },
                "date_removed": None,
            }
        ]


@override_settings(
    METAX_V3={
        "HOST": "metax-test",
        "TOKEN": "test-token",
        "INTEGRATION_ENABLED": True,
        "PROTOCOL": "https",
    }
)
class ContractApiWriteV3IntegrationTests(APITestCase, TestClassUtils):
    def setUp(self):
        """
        Reloaded for every test case
        """
        call_command("loaddata", test_data_file_path, verbosity=0)
        self._use_http_authorization()
        contract_from_test_data = self._get_object_from_test_data("contract")
        self.pk = contract_from_test_data["id"]
        self.test_data = contract_from_test_data
        self.test_new_data = self._get_new_test_data()

    def mock_responses(self):
        responses.add(
            responses.POST,
            url="https://metax-test/v3/contracts/from-legacy",
            json={},
        )

    def _get_new_test_data(self):
        return {
            "contract_json": {
                "title": "Title of new contract",
                "identifier": "optional-identifier-new",
                "quota": 111204,
                "created": "2014-01-17T08:19:58Z",
                "modified": "2014-01-17T08:19:58Z",
                "description": "Description of unknown length",
                "contact": [
                    {
                        "name": "Contact Name",
                        "phone": "+358501231234",
                        "email": "contact.email@csc.fi",
                    }
                ],
                "organization": {
                    "organization_identifier": "1234567abc",
                    "name": "Mysterious organization",
                },
                "related_service": [{"identifier": "local:service:id", "name": "Name of Service"}],
                "validity": {"start_date": "2014-01-17"},
            }
        }

    @responses.activate
    def test_create_contract_v3_sync(self):
        self.mock_responses()
        response = self.client.post("/rest/contracts/", self.test_new_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)

        self.assertEqual(len(responses.calls), 1)
        self.assertEqual(
            responses.calls[0].request.url, "https://metax-test/v3/contracts/from-legacy"
        )
        self.assertEqual(responses.calls[0].request.method, "POST")
        data = json.loads(responses.calls[0].request.body)
        self.assertEqual(data["contract_json"], self.test_new_data["contract_json"])
        self.assertEqual(data["id"], response.data["id"])

    @responses.activate
    def test_bulk_create_contract_v3_sync(self):
        self.mock_responses()
        response = self.client.post("/rest/contracts/", [self.test_new_data], format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)

        self.assertEqual(len(responses.calls), 1)
        self.assertEqual(
            responses.calls[0].request.url, "https://metax-test/v3/contracts/from-legacy"
        )
        self.assertEqual(responses.calls[0].request.method, "POST")
        data = json.loads(responses.calls[0].request.body)
        self.assertEqual(data["contract_json"], self.test_new_data["contract_json"])
        self.assertEqual(data["id"], response.data["success"][0]["object"]["id"])

    @responses.activate
    def test_update_contract_v3_sync(self):
        self.mock_responses()
        contract_json = self.test_new_data["contract_json"]
        response = self.client.patch(
            f"/rest/contracts/{self.pk}", {"contract_json": contract_json}, format="json"
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)

        self.assertEqual(len(responses.calls), 1)
        self.assertEqual(
            responses.calls[0].request.url, "https://metax-test/v3/contracts/from-legacy"
        )
        self.assertEqual(responses.calls[0].request.method, "POST")
        data = json.loads(responses.calls[0].request.body)
        self.assertEqual(data["contract_json"], self.test_new_data["contract_json"])
        self.assertEqual(data["id"], response.data["id"])

    @responses.activate
    def test_bulk_update_contract_v3_sync(self):
        self.mock_responses()
        contract_json = self.test_new_data["contract_json"]
        response = self.client.patch(
            "/rest/contracts", [{"id": self.pk, "contract_json": contract_json}], format="json"
        )
        self.assertEqual(response.status_code, status.HTTP_405_METHOD_NOT_ALLOWED, response.data)

    @responses.activate
    def test_delete_contract_v3_sync(self):
        self.mock_responses()
        response = self.client.delete(f"/rest/contracts/{self.pk}", format="json")
        self.assertEqual(response.status_code, status.HTTP_204_NO_CONTENT, response.data)

        self.assertEqual(len(responses.calls), 1)
        self.assertEqual(
            responses.calls[0].request.url, "https://metax-test/v3/contracts/from-legacy"
        )
        self.assertEqual(responses.calls[0].request.method, "POST")
        data = json.loads(responses.calls[0].request.body)
        self.assertEqual(data["contract_json"], self.test_data["contract_json"])
        self.assertEqual(data["id"], self.pk)
        self.assertTrue(data["removed"])
        self.assertIsNotNone(data["date_removed"])
