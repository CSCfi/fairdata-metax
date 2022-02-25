# This file is part of the Metax API service
#
# Copyright 2021-2022 Ministry of Education and Culture, Finland
#
# :author: CSC - IT Center for Science Ltd., Espoo Finland <servicedesk@csc.fi>
# :license: MIT
import json

import responses
from django.core.management import call_command
from django.test import TestCase
from django.conf import settings

from metax_api.models import CatalogRecordV2
from metax_api.tests.utils import test_data_file_path

access_granter = {
    "userid": "access_granter",
    "name": "Access Granter",
    "email": "granter@example.com",
}

license = [
    {
        "title": {
            "fi": "Creative Commons Nimeä-EiKaupallinen 2.0 Yleinen (CC BY-NC 2.0)",
            "en": "Creative Commons Attribution-NonCommercial 2.0 Generic (CC BY-NC 2.0",
            "und": "Creative Commons Nimeä-EiKaupallinen 2.0 Yleinen (CC BY-NC 2.0)",
        },
        "identifier": "http://uri.suomi.fi/codelist/fairdata/license/code/CC-BY-NC-2.0",
        "description": {"en": "Free account of the rights"},
        "license": "https://creativecommons.org/licenses/by-nc/2.0/",
    },
    {
        "title": {"fi": "Muu", "en": "Other", "und": "Muu"},
        "identifier": "http://uri.suomi.fi/codelist/fairdata/license/code/other",
    },
]


def get_calls_for_url(url):
    """Get deserialized list of calls for url."""
    calls = []
    for call, response in responses.calls:
        if call.url == url:
            calls.append(json.loads(call.body))
    return calls


class TestCreateMissingREMSItems(TestCase):
    def setUp(self):
        call_command("loaddata", test_data_file_path, verbosity=0)
        self.setup_catalog_records()
        self.mock_rems_responses()

    def setup_catalog_records(self):
        # has REMS entity
        cr = CatalogRecordV2.objects.get(pk=1)
        cr._initial_data["access_granter"] = access_granter
        cr.research_dataset["access_rights"]["license"] = license
        cr.rems_identifier = "already-exists"
        cr.save()

        # missing REMS entity
        cr = CatalogRecordV2.objects.get(pk=2)
        cr._initial_data["access_granter"] = access_granter
        cr.research_dataset["access_rights"]["license"] = license
        cr.rems_identifier = "does-not-exist"
        cr.save()
        self.no_rems_cr = cr

        # missing REMS entity, missing license
        cr = CatalogRecordV2.objects.get(pk=3)
        cr._initial_data["access_granter"] = access_granter
        cr.research_dataset["access_rights"]["license"] = None
        cr.rems_identifier = "does-not-have-license"
        cr.save()
        self.no_license_cr = cr

    def mock_rems_responses(self):
        rems_url = settings.REMS["BASE_URL"]
        rems_org = settings.REMS["ORGANIZATION"]
        responses.add(responses.GET, f"{rems_url}/health", json={"healthy": True}, status=200)

        # organization
        responses.add(
            responses.GET,
            f"{rems_url}/organizations/{rems_org}",
            json={
                "archived": False,
                "organization/id": rems_org,
                "organization/short-name": {"fi": "CSC", "en": "CSC", "sv": "CSC"},
                "organization/review-emails": [],
                "enabled": True,
                "organization/owners": [],
                "organization/name": {
                    "fi": "Rems test org",
                    "en": "Rems test org",
                    "sv": "Rems test org",
                },
            },
            status=200,
        )

        # catalogue-items handling and creation
        responses.add(
            responses.GET,
            f"{rems_url}/catalogue-items?resource=already-exists&archived=true&disabled=true",
            json=[{}],
            status=200,
        )
        responses.add(
            responses.GET,
            f"{rems_url}/catalogue-items?resource=does-not-exist&archived=true&disabled=true",
            json=[],
            status=200,
        )
        responses.add(
            responses.GET,
            f"{rems_url}/catalogue-items?resource=does-not-have-license&archived=true&disabled=true",
            json=[],
            status=200,
        )
        responses.add(
            responses.POST,
            f"{rems_url}/catalogue-items/create",
            json={"success": True, "id": "created-catalogue-item-id"},
            status=200,
        )

        # user creation
        responses.add(
            responses.POST,
            f"{rems_url}/users/create",
            json={"success": True},
            status=200,
        )

        # workflow creation
        responses.add(
            responses.POST,
            f"{rems_url}/workflows/create",
            json={"success": True, "id": "created-workflow-id"},
            status=200,
        )

        # license handling
        responses.add(
            responses.GET,
            f"{rems_url}/licenses?disabled=true&archived=true",
            json=[{"localizations": {"en": {"textcontent": "https://example.com/license_x/en"}}}],
            status=200,
        )
        responses.add(
            responses.POST,
            f"{rems_url}/licenses/create",
            json={"success": True, "id": "created-license-id"},
            status=200,
        )

        # resource creation
        def create_resource_with_resid(request):
            data = json.loads(request.body)
            return 200, {}, json.dumps({"success": True, "id": data["resid"]})

        responses.add_callback(
            responses.POST,
            f"{rems_url}/resources/create",
            callback=create_resource_with_resid,
            content_type="application/json",
        )

    @responses.activate
    def test_rems_creates_catalogue_item(self):
        call_command("create_missing_rems_items")
        calls = get_calls_for_url("https://mock-rems/api/catalogue-items/create")
        assert calls == [
            {
                "form": 1,
                "resid": "does-not-exist",
                "wfid": "created-workflow-id",
                "localizations": {
                    "en": {
                        "title": self.no_rems_cr.research_dataset["title"]["en"],
                        "infourl": settings.REMS["ETSIN_URL_TEMPLATE"] % self.no_rems_cr.identifier,
                    }
                },
                "enabled": True,
                "organization": {"organization/id": settings.REMS["ORGANIZATION"]},
            }
        ]

    @responses.activate
    def test_rems_logs(self):
        with self.assertLogs() as logs:
            call_command("create_missing_rems_items")
        msgs = [log.msg % log.args for log in logs.records]
        assert "Found 3 CatalogRecords with rems_identifiers" in msgs
        assert "CatalogRecords with existing REMS entities: 1" in msgs
        assert (
            f"Missing license for {self.no_license_cr.identifier}, not creating REMS entity" in msgs
        )
        assert "Missing REMS entities: 2" in msgs
        assert "Created REMS entities: 1" in msgs
