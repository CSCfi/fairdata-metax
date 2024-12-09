import logging
import responses
import re
from uuid import uuid4

from django.core.management import call_command
from django.test import TestCase
from django.test import override_settings
from rest_framework.test import APITestCase
from rest_framework import status

from metax_api.models import CatalogRecord
from metax_api.services import PIDMSService
from metax_api.tests.utils import TestClassUtils, test_data_file_path


_logger = logging.getLogger(__name__)


# metax_api.tests.management.commands.migrate_pids.PIDMigrationTest
@override_settings(
    PID_MS={
        "HOST": "pidms-test",
        "TOKEN": "test-token",
        "PROTOCOL": "https",
        "CATALOGS_TO_MIGRATE": [
            "urn:nbn:fi:att:data-catalog-ida",
            "urn:nbn:fi:att:data-catalog-att",
        ],
    }
)
class PIDMigrationTest(APITestCase, TestClassUtils):
    @classmethod
    def setUpClass(cls):
        """
        Loaded only once for test cases inside this class.
        """
        call_command("loaddata", test_data_file_path, verbosity=0)

        super(PIDMigrationTest, cls).setUpClass()

    def setUp(self):
        self._use_http_authorization()

        # Init catalog record
        research_dataset = self._get_object_from_test_data("catalogrecord", requested_index=0)[
            "research_dataset"
        ]

        self.cr = {
            "metadata_provider_org": "abc-org-123",
            "metadata_provider_user": "abc-user-123",
            "research_dataset": research_dataset,
        }

        self.cr["research_dataset"].pop("preferred_identifier", None)
        self.cr["research_dataset"].pop("identifier", None)

        # Create IDA catalog
        data_catalog = self._get_object_from_test_data("datacatalog", requested_index=0)[
            "catalog_json"
        ]

        data_catalog = {
            "catalog_json": data_catalog,
            "catalog_record_services_create": "testuser,api_auth_user,metax,tpas,metax_service",
            "catalog_record_services_edit": "testuser,api_auth_user,metax,tpas,metax_service",
            "catalog_record_services_read": "testuser,api_auth_user,metax,tpas,metax_service",
        }
        data_catalog["catalog_json"]["identifier"] = "urn:nbn:fi:att:data-catalog-ida"

        response = self.client.post("/rest/v2/datacatalogs", data_catalog, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED)

    def mock_responses(self):
        responses.add(
            responses.POST,
            url=re.compile("https://pidms-test/v1/pid/.*"),
            json={},
        )
        responses.add(
            responses.POST,
            url=re.compile("https://pidms-test/v1/pid/.*"),
            body=Exception("PID exists already"),
            status=400,
        )
        responses.add(
            responses.GET,
            url=re.compile("https://pidms-test/get/v1/pid/.*"),
        )

    def mock_responses_already_exists(self):
        responses.add(
            responses.POST,
            url=re.compile("https://pidms-test/v1/pid/.*"),
            body=Exception("PID exists already"),
            status=400,
        )

    def mock_responses_error(self):
        responses.add(
            responses.POST,
            url=re.compile("https://pidms-test/v1/pid/.*"),
            body=Exception("Random Error"),
            status=503,
        )
        responses.add(
            responses.GET,
            url=re.compile("https://pidms-test/get/v1/pid/.*"),
        )

    @responses.activate
    def testMigrateURNSuccessfully(self):
        self.mock_responses()
        cr = self.cr

        # Create dataset with URN
        cr["data_catalog"] = "urn:nbn:fi:att:data-catalog-ida"
        response = self.client.post("/rest/v2/datasets?pid_type=urn", cr, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED)
        cr_id = response.json()["id"]

        catalog_record = CatalogRecord.objects.get(id=cr_id)
        self.assertFalse(catalog_record.pid_migrated)

        # Migrate PIDs
        call_command("migrate_pids")

        # Assert PID has been migrated
        catalog_record = CatalogRecord.objects.get(id=cr_id)
        self.assertTrue(catalog_record.pid_migrated)
        self.assertEqual(len(responses.calls), 1)

    @responses.activate
    def testMigrateDOISuccessfully(self):
        self.mock_responses()
        cr = self.cr

        # Create dataset with DOI
        cr["data_catalog"] = "urn:nbn:fi:att:data-catalog-ida"
        response = self.client.post("/rest/v2/datasets?pid_type=doi", cr, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED)
        cr_id = response.json()["id"]

        catalog_record = CatalogRecord.objects.get(id=cr_id)
        self.assertFalse(catalog_record.pid_migrated)

        # Migrate PIDs
        call_command("migrate_pids")

        # Assert PID has been migrated
        catalog_record = CatalogRecord.objects.get(id=cr_id)
        self.assertTrue(catalog_record.pid_migrated)
        self.assertEqual(len(responses.calls), 1)

    @responses.activate
    def testMigrateAlreadyMigratedPID(self):
        self.mock_responses()
        cr = self.cr

        # Create dataset with URN
        cr["data_catalog"] = "urn:nbn:fi:att:data-catalog-ida"
        response = self.client.post("/rest/v2/datasets?pid_type=urn", cr, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED)
        cr_id = response.json()["id"]

        catalog_record = CatalogRecord.objects.get(id=cr_id)
        self.assertFalse(catalog_record.pid_migrated)

        # Migrate PIDs
        call_command("migrate_pids")

        # Assert PID has been migrated
        catalog_record = CatalogRecord.objects.get(id=cr_id)
        self.assertTrue(catalog_record.pid_migrated)
        self.assertEqual(len(responses.calls), 1)

        # Try to migrate PID again
        with self.assertRaises(Exception) as cm:
            PIDMSService().insert_pid(catalog_record)

        self.assertIn("PID exists already", str(cm.exception))

    @responses.activate
    def testMigratePIDErrorInPIDMS(self):
        self.mock_responses_error()
        cr = self.cr

        # Create dataset with URN
        cr["data_catalog"] = "urn:nbn:fi:att:data-catalog-ida"
        response = self.client.post("/rest/v2/datasets?pid_type=urn", cr, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED)
        cr_id = response.json()["id"]

        catalog_record = CatalogRecord.objects.get(id=cr_id)
        self.assertFalse(catalog_record.pid_migrated)

        # Try to migrate PIDs
        with self.assertRaises(Exception) as cm:
            call_command("migrate_pids")

        # Assert PID has not been migrated
        catalog_record = CatalogRecord.objects.get(id=cr_id)
        self.assertFalse(catalog_record.pid_migrated)
        self.assertEqual(len(responses.calls), 1)

        # Try to migrate the PID again
        with self.assertRaises(Exception) as cm:
            PIDMSService().insert_pid(catalog_record)

        self.assertIn("Exception in PIDMSClient", str(cm.exception))
        catalog_record = CatalogRecord.objects.get(id=cr_id)
        self.assertFalse(catalog_record.pid_migrated)

    @responses.activate
    def testMigratePIDWrongCatalog(self):
        self.mock_responses()
        cr = self.cr

        # Create foo catalog
        data_catalog = self._get_object_from_test_data("datacatalog", requested_index=0)[
            "catalog_json"
        ]

        data_catalog = {
            "catalog_json": data_catalog,
            "catalog_record_services_create": "testuser,api_auth_user,metax,tpas",
            "catalog_record_services_edit": "testuser,api_auth_user,metax,tpas",
            "catalog_record_services_read": "testuser,api_auth_user,metax,tpas",
        }
        data_catalog["catalog_json"]["identifier"] = "urn:nbn:fi:att:data-catalog-foo"

        response = self.client.post("/rest/v2/datacatalogs", data_catalog, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED)

        # Create dataset with URN in foo catalog
        cr["data_catalog"] = "urn:nbn:fi:att:data-catalog-foo"
        response = self.client.post("/rest/v2/datasets?pid_type=urn", cr, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED)
        cr_id = response.json()["id"]

        catalog_record = CatalogRecord.objects.get(id=cr_id)
        self.assertFalse(catalog_record.pid_migrated)

        # Migrate PIDs
        call_command("migrate_pids")

        # Assert PID has not been migrated
        catalog_record = CatalogRecord.objects.get(id=cr_id)
        self.assertFalse(catalog_record.pid_migrated)

        # Try executing the method directly
        with self.assertRaises(ValueError) as cm:
            PIDMSService().insert_pid(catalog_record)

        self.assertIn("Only catalog records in the following catalogs", str(cm.exception))
        catalog_record = CatalogRecord.objects.get(id=cr_id)
        self.assertFalse(catalog_record.pid_migrated)

        self.assertEqual(len(responses.calls), 0)

    @responses.activate
    def testMigratePIDDraft(self):
        self.mock_responses()
        cr = self.cr

        # Create a draft dataset with URN
        cr["data_catalog"] = "urn:nbn:fi:att:data-catalog-ida"
        response = self.client.post("/rest/v2/datasets?pid_type=urn&draft=true", cr, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED)
        self.assertEqual(response.json()["state"], "draft")
        cr_id = response.json()["id"]

        catalog_record = CatalogRecord.objects.get(id=cr_id)
        self.assertFalse(catalog_record.pid_migrated)

        # Migrate PIDs
        call_command("migrate_pids")

        # Assert PID has not been migrated
        catalog_record = CatalogRecord.objects.get(id=cr_id)
        self.assertFalse(catalog_record.pid_migrated)

        # Try executing the method directly
        with self.assertRaises(ValueError) as cm:
            PIDMSService().insert_pid(catalog_record)

        self.assertIn("Only catalog records in state", str(cm.exception))
        catalog_record = CatalogRecord.objects.get(id=cr_id)
        self.assertFalse(catalog_record.pid_migrated)

        self.assertEqual(len(responses.calls), 0)

    @responses.activate
    def testMigratePIDFromV3(self):
        self.mock_responses()
        cr = self.cr

       	# Create a dataset from V3
        self._use_http_authorization("metax_service")
        cr_identifier = str(uuid4())
        cr["research_dataset"]["preferred_identifier"] = "urn:nbn:fi:att:12345678-abcd-abcd-abcd-12345678912"
        cr["identifier"] = cr_identifier
        cr["api_meta"] = {"version": 3}
        cr["data_catalog"] = "urn:nbn:fi:att:data-catalog-ida"
        response = self.client.post("/rest/v2/datasets?migration_override", cr, format="json")

        self.assertEqual(response.status_code, status.HTTP_201_CREATED)
        cr_id = response.json()["id"]

        catalog_record = CatalogRecord.objects.get(id=cr_id)
        self.assertFalse(catalog_record.pid_migrated)

        # Migrate PIDs
        call_command("migrate_pids")

        # Assert PID has not been migrated
        catalog_record = CatalogRecord.objects.get(id=cr_id)
        self.assertFalse(catalog_record.pid_migrated)

        # Try executing the method directly
        with self.assertRaises(ValueError) as cm:
            PIDMSService().insert_pid(catalog_record)

        self.assertIn("Only catalog records with api_version", str(cm.exception))
        catalog_record = CatalogRecord.objects.get(id=cr_id)
        self.assertFalse(catalog_record.pid_migrated)

        self.assertEqual(len(responses.calls), 0)


    @responses.activate
    def testMigratePIDExternalURN(self):
        self.mock_responses()
        cr = self.cr

       	# Create a dataset from V3 with non-metax URN
        self._use_http_authorization("metax_service")
        cr_identifier = str(uuid4())
        cr["research_dataset"]["preferred_identifier"] = "test_pid"
        cr["identifier"] = cr_identifier
        cr["api_meta"] = {"version": 3}
        cr["data_catalog"] = "urn:nbn:fi:att:data-catalog-ida"
        response = self.client.post("/rest/v2/datasets?migration_override", cr, format="json")

        self.assertEqual(response.status_code, status.HTTP_201_CREATED)
        cr_id = response.json()["id"]

        catalog_record = CatalogRecord.objects.get(id=cr_id)
        self.assertFalse(catalog_record.pid_migrated)

        # Migrate PIDs
        call_command("migrate_pids")

        # Assert PID has not been migrated
        catalog_record = CatalogRecord.objects.get(id=cr_id)
        self.assertFalse(catalog_record.pid_migrated)

        # Try executing the method directly
        with self.assertRaises(ValueError) as cm:
            PIDMSService().insert_pid(catalog_record)

        self.assertIn("Only PIDs generated by Metax are inserted to PID-MS", str(cm.exception))
        catalog_record = CatalogRecord.objects.get(id=cr_id)
        self.assertFalse(catalog_record.pid_migrated)

        self.assertEqual(len(responses.calls), 0)


    @responses.activate
    def testMigratePIDExternalDOI(self):
        self.mock_responses()
        cr = self.cr

       	# Create a dataset from V3 with non-metax DOI
        self._use_http_authorization("metax_service")
        cr_identifier = str(uuid4())
        cr["research_dataset"]["preferred_identifier"] = "doi:10.12345/12345678-abcd-abcd-abcd-12345678912"
        cr["identifier"] = cr_identifier
        cr["api_meta"] = {"version": 3}
        cr["data_catalog"] = "urn:nbn:fi:att:data-catalog-ida"
        response = self.client.post("/rest/v2/datasets?migration_override", cr, format="json")

        self.assertEqual(response.status_code, status.HTTP_201_CREATED)
        cr_id = response.json()["id"]

        catalog_record = CatalogRecord.objects.get(id=cr_id)
        self.assertFalse(catalog_record.pid_migrated)

        # Migrate PIDs
        call_command("migrate_pids")

        # Assert PID has not been migrated
        catalog_record = CatalogRecord.objects.get(id=cr_id)
        self.assertFalse(catalog_record.pid_migrated)

        # Try executing the method directly
        with self.assertRaises(ValueError) as cm:
            PIDMSService().insert_pid(catalog_record)

        self.assertIn("Only PIDs generated by Metax are inserted to PID-MS", str(cm.exception))
        catalog_record = CatalogRecord.objects.get(id=cr_id)
        self.assertFalse(catalog_record.pid_migrated)

        self.assertEqual(len(responses.calls), 0)
