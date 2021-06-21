# This file is part of the Metax API service
#
# Copyright 2017-2018 Ministry of Education and Culture, Finland
#
# :author: CSC - IT Center for Science Ltd., Espoo Finland <servicedesk@csc.fi>
# :license: MIT

import lxml.etree
from django.conf import settings
from django.core.management import call_command
from lxml.etree import Element
from oaipmh import common
from rest_framework import status
from rest_framework.test import APITestCase

from metax_api.api.oaipmh.base.metax_oai_server import MetaxOAIServer
from metax_api.models import CatalogRecord, DataCatalog
from metax_api.tests.utils import TestClassUtils, test_data_file_path

IDA_CATALOG = settings.IDA_DATA_CATALOG_IDENTIFIER
ATT_CATALOG = settings.ATT_DATA_CATALOG_IDENTIFIER


class OAIPMHReadTests(APITestCase, TestClassUtils):

    _namespaces = {
        "o": "http://www.openarchives.org/OAI/2.0/",
        "oai_dc": "http://www.openarchives.org/OAI/2.0/oai_dc/",
        "dc": "http://purl.org/dc/elements/1.1/",
        "dct": "http://purl.org/dc/terms/",
        "datacite": "http://schema.datacite.org/oai/oai-1.0/",
    }

    @classmethod
    def setUpClass(cls):
        """
        Loaded only once for test cases inside this class.
        """
        call_command("loaddata", test_data_file_path, verbosity=0)
        super(OAIPMHReadTests, cls).setUpClass()

    def setUp(self):
        cr = CatalogRecord.objects.get(pk=1)
        cr.data_catalog.catalog_json["identifier"] = ATT_CATALOG
        cr.data_catalog.force_save()

        cr = CatalogRecord.objects.get(pk=14)
        cr.data_catalog.catalog_json["identifier"] = IDA_CATALOG
        cr.data_catalog.force_save()

        # some cr that has publisher set...
        cr = CatalogRecord.objects.filter(research_dataset__publisher__isnull=False).first()
        self.identifier = cr.identifier
        self.id = cr.id
        self.preferred_identifier = cr.preferred_identifier
        self._use_http_authorization()

    def _get_results(self, data, xpath):
        root = data
        if isinstance(data, bytes):
            root = lxml.etree.fromstring(data)
        return root.xpath(xpath, namespaces=self._namespaces)

    def _get_single_result(self, data, xpath):
        results = self._get_results(data, xpath)
        return results[0]

    def _set_dataset_as_draft(self, cr_id):
        cr = CatalogRecord.objects.get(pk=cr_id)
        cr.state = "draft"
        cr.force_save()

    # VERB: Identify

    def test_identify(self):
        response = self.client.get("/oai/?verb=Identify")
        self.assertEqual(response.status_code, status.HTTP_200_OK)

    # VERB: ListMetadataFormats

    def test_list_metadata_formats(self):
        response = self.client.get("/oai/?verb=ListMetadataFormats")
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        formats = self._get_single_result(response.content, "//o:ListMetadataFormats")
        self.assertEqual(len(formats), 4)

        metadataPrefix = self._get_results(formats, '//o:metadataPrefix[text() = "oai_dc"]')
        self.assertEqual(len(metadataPrefix), 1)

        metadataPrefix = self._get_results(formats, '//o:metadataPrefix[text() = "oai_datacite"]')
        self.assertEqual(len(metadataPrefix), 1)

        metadataPrefix = self._get_results(
            formats, '//o:metadataPrefix[text() = "oai_dc_urnresolver"]'
        )
        self.assertEqual(len(metadataPrefix), 1)

    # VERB: ListSets

    def test_list_sets(self):
        response = self.client.get("/oai/?verb=ListSets")
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        sets = self._get_results(response.content, "//o:setSpec")
        # urnresolver set should be hidden
        self.assertEquals(len(sets), 4)

    # VERB: ListIdentifiers

    def test_list_identifiers(self):
        ms = settings.OAI["BATCH_SIZE"]
        allRecords = CatalogRecord.objects.filter(
            data_catalog__catalog_json__identifier__in=MetaxOAIServer._get_default_set_filter()
        )
        response = self.client.get("/oai/?verb=ListIdentifiers&metadataPrefix=oai_dc")
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        headers = self._get_results(response.content, "//o:header")
        self.assertTrue(len(headers) == ms, len(headers))

        token = self._get_single_result(response.content, '//o:resumptionToken')
        response = self.client.get(f"/oai/?verb=ListIdentifiers&resumptionToken={token.text}")
        self.assertEqual(response.status_code, status.HTTP_200_OK)

        headers = self._get_results(response.content, "//o:header")
        self.assertEqual(len(allRecords) - ms, len(headers))

        response = self.client.get("/oai/?verb=ListIdentifiers&metadataPrefix=oai_datacite")
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        headers = self._get_results(response.content, "//o:header")
        self.assertTrue(len(headers) == ms, len(headers))

        response = self.client.get("/oai/?verb=ListIdentifiers&metadataPrefix=oai_dc_urnresolver")
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        errors = self._get_results(response.content, '//o:error[@code="badArgument"]')
        self.assertTrue(len(errors) == 1, response.content)

    def test_list_identifiers_for_drafts(self):
        """ Tests that drafts are not returned from ListIdentifiers """
        ms = settings.OAI["BATCH_SIZE"]
        allRecords = CatalogRecord.objects.filter(
            data_catalog__catalog_json__identifier__in=MetaxOAIServer._get_default_set_filter()
        )[:ms]

        self._set_dataset_as_draft(25)
        self._set_dataset_as_draft(26)

        # headers should be reduced when some datasets are set as drafts
        response = self.client.get("/oai/?verb=ListIdentifiers&metadataPrefix=oai_dc")
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        headers = self._get_results(response.content, "//o:header")
        self.assertFalse(len(headers) == len(allRecords), len(headers))

    def test_list_identifiers_from_datacatalogs_set(self):
        allRecords = DataCatalog.objects.all()[: settings.OAI["BATCH_SIZE"]]
        response = self.client.get(
            "/oai/?verb=ListIdentifiers&metadataPrefix=oai_dc&set=datacatalogs"
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        records = self._get_results(response.content, "//o:header")
        self.assertTrue(len(records) == len(allRecords), len(records))

    # VERB: ListRecords

    def test_list_records(self):
        ms = settings.OAI["BATCH_SIZE"]
        allRecords = CatalogRecord.objects.filter(
            data_catalog__catalog_json__identifier__in=MetaxOAIServer._get_default_set_filter()
        )

        response = self.client.get("/oai/?verb=ListRecords&metadataPrefix=oai_dc")
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        records = self._get_results(response.content, "//o:record")
        self.assertTrue(len(records) == ms)

        token = self._get_single_result(response.content, '//o:resumptionToken')
        response = self.client.get(f"/oai/?verb=ListRecords&resumptionToken={token.text}")
        self.assertEqual(response.status_code, status.HTTP_200_OK)

        records = self._get_results(response.content, "//o:record")
        self.assertTrue(len(allRecords) - ms == len(records))

        response = self.client.get("/oai/?verb=ListRecords&metadataPrefix=oai_fairdata_datacite")
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        records = self._get_results(response.content, "//o:record")
        self.assertTrue(len(records) == ms)

        response = self.client.get("/oai/?verb=ListRecords&metadataPrefix=oai_dc_urnresolver")
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        records = self._get_results(response.content, "//o:record")
        self.assertTrue(len(records) == ms)

    def test_list_records_for_drafts(self):
        """ Tests that drafts are not returned from ListRecords """
        ms = settings.OAI["BATCH_SIZE"]
        allRecords = CatalogRecord.objects.filter(
            data_catalog__catalog_json__identifier__in=MetaxOAIServer._get_default_set_filter()
        )[:ms]

        self._set_dataset_as_draft(25)
        self._set_dataset_as_draft(26)

        response = self.client.get("/oai/?verb=ListRecords&metadataPrefix=oai_fairdata_datacite")
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        records = self._get_results(response.content, "//o:record")
        self.assertFalse(len(records) == len(allRecords))

    def test_list_records_urnresolver_from_datacatalogs_set(self):
        response = self.client.get(
            "/oai/?verb=ListRecords&metadataPrefix=oai_dc_urnresolver&set=datacatalogs"
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        errors = self._get_results(response.content, '//o:error[@code="badArgument"]')
        self.assertTrue(len(errors) == 1, response.content)

        response = self.client.get(
            "/oai/?verb=ListRecords&metadataPrefix=oai_dc_urnresolver&set=ida_datasets"
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        errors = self._get_results(response.content, '//o:error[@code="badArgument"]')
        self.assertTrue(len(errors) == 1, response.content)

        response = self.client.get("/oai/?verb=ListRecords&metadataPrefix=oai_dc&set=invalid")
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        errors = self._get_results(response.content, '//o:error[@code="badArgument"]')
        self.assertTrue(len(errors) == 1, response.content)

    def test_list_records_urnresolver_for_datasets_set(self):
        response = self.client.get(
            "/oai/?verb=ListRecords&metadataPrefix=oai_dc_urnresolver&set=datasets"
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        headers = self._get_results(response.content, "//o:header")
        self.assertTrue(len(headers) > 0)
        records = self._get_results(response.content, "//oai_dc:dc")
        for record_metadata in records:
            urn_elements = self._get_results(
                record_metadata, 'dc:identifier[starts-with(text(), "urn")]'
            )
            self.assertTrue(urn_elements is not None or len(urn_elements) > 0)
            url_element = self._get_single_result(
                record_metadata, 'dc:identifier[starts-with(text(), "http")]'
            )
            self.assertTrue(url_element is not None)

    def test_list_records_from_datasets_set(self):
        ms = settings.OAI["BATCH_SIZE"]
        allRecords = CatalogRecord.objects.filter(
            data_catalog__catalog_json__identifier__in=MetaxOAIServer._get_default_set_filter()
        )[:ms]

        response = self.client.get("/oai/?verb=ListRecords&metadataPrefix=oai_dc&set=datasets")
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        records = self._get_results(response.content, "//o:record")
        self.assertTrue(len(records) == len(allRecords), len(records))

    def test_list_records_from_datacatalogs_set(self):
        allRecords = DataCatalog.objects.all()[: settings.OAI["BATCH_SIZE"]]

        response = self.client.get("/oai/?verb=ListRecords&metadataPrefix=oai_dc&set=datacatalogs")
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        records = self._get_results(response.content, "//o:record")
        self.assertTrue(len(records) == len(allRecords), len(records))

    def test_list_records_from_att_datasets_set(self):
        allRecords = CatalogRecord.objects.filter(
            data_catalog__catalog_json__identifier__in=[ATT_CATALOG]
        )[: settings.OAI["BATCH_SIZE"]]

        response = self.client.get("/oai/?verb=ListRecords&metadataPrefix=oai_dc&set=att_datasets")
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        records = self._get_results(response.content, "//o:record")
        self.assertTrue(len(records) == len(allRecords), len(records))

    def test_list_records_from_ida_datasets_set(self):
        allRecords = CatalogRecord.objects.filter(
            data_catalog__catalog_json__identifier__in=[IDA_CATALOG]
        )[: settings.OAI["BATCH_SIZE"]]

        response = self.client.get("/oai/?verb=ListRecords&metadataPrefix=oai_dc&set=ida_datasets")
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        records = self._get_results(response.content, "//o:record")
        self.assertTrue(len(records) == len(allRecords))

    def test_list_records_using_invalid_metadata_prefix(self):
        response = self.client.get("/oai/?verb=ListRecords&metadataPrefix=oai_notavailable")
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        errors = self._get_results(response.content, '//o:error[@code="cannotDisseminateFormat"]')
        self.assertTrue(len(errors) == 1, response.content)

        response = self.client.get(
            "/oai/?verb=GetRecord&identifier=%s&metadataPrefix=oai_notavailable" % self.identifier
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        errors = self._get_results(response.content, '//o:error[@code="cannotDisseminateFormat"]')
        self.assertTrue(len(errors) == 1, response.content)

    def test_distinct_records_in_set(self):
        att_resp = self.client.get("/oai/?verb=ListRecords&metadataPrefix=oai_dc&set=att_datasets")
        self.assertEqual(att_resp.status_code, status.HTTP_200_OK)
        ida_resp = self.client.get("/oai/?verb=ListRecords&metadataPrefix=oai_dc&set=ida_datasets")
        self.assertEqual(ida_resp.status_code, status.HTTP_200_OK)

        att_records = self._get_results(att_resp.content, "//o:record")

        att_identifier = att_records[0][0][0].text

        ida_records = self._get_results(
            ida_resp.content,
            '//o:record/o:header/o:identifier[text()="%s"]' % att_identifier,
        )
        self.assertTrue(len(ida_records) == 0)

    # VERB: GetRecord

    def test_record_get_datacatalog(self):
        dc = DataCatalog.objects.get(pk=1)
        dc_identifier = dc.catalog_json["identifier"]
        response = self.client.get(
            "/oai/?verb=GetRecord&identifier=%s&metadataPrefix=oai_dc" % dc_identifier
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        identifiers = self._get_results(
            response.content,
            '//o:record/o:metadata/oai_dc:dc/dc:identifier[text()="%s"]' % dc_identifier,
        )
        self.assertTrue(len(identifiers) == 1, response.content)

    def test_get_record(self):
        response = self.client.get(
            "/oai/?verb=GetRecord&identifier=%s&metadataPrefix=oai_dc" % self.identifier
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        identifiers = self._get_results(
            response.content,
            '//o:record/o:header/o:identifier[text()="%s"]' % self.identifier,
        )
        self.assertTrue(len(identifiers) == 1, response.content)

        response = self.client.get(
            "/oai/?verb=GetRecord&identifier=%s&metadataPrefix=oai_fairdata_datacite"
            % self.identifier
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        identifiers = self._get_results(
            response.content,
            "//o:record/o:metadata/datacite:oai_fairdata_datacite/"
            + 'datacite:schemaVersion[text()="%s"]' % "4.1",
        )
        self.assertTrue(len(identifiers) == 1, response.content)
        identifiers = self._get_results(
            response.content,
            '//o:record/o:header/o:identifier[text()="%s"]' % self.identifier,
        )
        self.assertTrue(len(identifiers) == 1, response.content)

    def test_get_record_for_drafts(self):
        """ Tests that GetRecord doesn't return drafts """

        response = self.client.get(
            "/oai/?verb=GetRecord&identifier=%s&metadataPrefix=oai_dc" % self.identifier
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        identifiers = self._get_results(
            response.content,
            '//o:record/o:header/o:identifier[text()="%s"]' % self.identifier,
        )
        self.assertTrue(len(identifiers) == 1, response.content)

        # Set same dataset as draft
        self._set_dataset_as_draft(self.id)

        response = self.client.get(
            "/oai/?verb=GetRecord&identifier=%s&metadataPrefix=oai_dc" % self.identifier
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        identifiers = self._get_results(
            response.content,
            '//o:record/o:header/o:identifier[text()="%s"]' % self.identifier,
        )
        self.assertTrue(len(identifiers) == 0, response.content)

    def test_get_record_non_existing(self):
        response = self.client.get(
            "/oai/?verb=GetRecord&identifier=urn:non:existing&metadataPrefix=oai_dc"
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        errors = self._get_results(response.content, '//o:error[@code="idDoesNotExist"]')
        self.assertTrue(len(errors) == 1, response.content)

    def test_get_record_urnresolver(self):
        response = self.client.get(
            "/oai/?verb=GetRecord&identifier=%s&metadataPrefix=oai_dc_urnresolver" % self.identifier
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        errors = self._get_results(response.content, '//o:error[@code="badArgument"]')
        self.assertTrue(len(errors) == 1, response.content)

    def test_get_record_datacatalog_unsupported_in_urnresolver(self):
        dc = DataCatalog.objects.get(pk=1)
        dc_identifier = dc.catalog_json["identifier"]
        response = self.client.get(
            "/oai/?verb=GetRecord&identifier=%s&metadataPrefix=oai_dc_urnresolver" % dc_identifier
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        errors = self._get_results(response.content, '//o:error[@code="badArgument"]')
        self.assertTrue(len(errors) == 1, response.content)

    def test_get_record_datacatalog_unsupported_in_datacite(self):
        dc = DataCatalog.objects.get(pk=1)
        dc_identifier = dc.catalog_json["identifier"]
        response = self.client.get(
            "/oai/?verb=GetRecord&identifier=%s&metadataPrefix=oai_datacite" % dc_identifier
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        errors = self._get_results(response.content, '//o:error[@code="badArgument"]')
        self.assertTrue(len(errors) == 1, response.content)

    def test_get_record_legacy_catalog_datasets_are_not_urnresolved(self):
        cr = CatalogRecord.objects.get(identifier=self.identifier)
        cr.data_catalog.catalog_json["identifier"] = settings.LEGACY_CATALOGS[0]
        cr.data_catalog.force_save()

        response = self.client.get(
            "/oai/?verb=GetRecord&identifier=%s&metadataPrefix=oai_dc_urnresolver&set=datasets"
            % self.identifier
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        identifiers = self._get_results(
            response.content,
            '//o:record/o:metadata/oai_dc:dc/dc:identifier[text()="%s"]'
            % self.preferred_identifier,
        )
        self.assertTrue(len(identifiers) == 0, response.content)

    # OAI-PMH Utilities & functionalities

    def test_write_oai_dc_with_lang(self):
        from metax_api.api.oaipmh.base.view import oai_dc_writer_with_lang

        e = Element("Test")
        md = {
            "title": [
                {"value": "title1", "lang": "en"},
                {"value": "title2", "lang": "fi"},
            ],
            "description": [{"value": "value"}],
        }
        metadata = common.Metadata("", md)
        oai_dc_writer_with_lang(e, metadata)
        result = str(lxml.etree.tostring(e, pretty_print=True))
        self.assertTrue('<dc:title xml:lang="en">title1</dc:title>' in result)
        self.assertTrue('<dc:title xml:lang="fi">title2</dc:title>' in result)
        self.assertTrue("<dc:description>value</dc:description>" in result)

    def test_get_oai_dc_metadata_dataset(self):
        cr = CatalogRecord.objects.get(pk=11)
        from metax_api.api.oaipmh.base.metax_oai_server import MetaxOAIServer

        s = MetaxOAIServer()
        md = s._get_oai_dc_metadata(cr, cr.research_dataset)
        self.assertTrue("identifier" in md)
        self.assertTrue("title" in md)
        self.assertTrue("lang" in md["title"][0])

    def test_get_oai_dc_metadata_datacatalog(self):
        dc = DataCatalog.objects.get(pk=1)
        from metax_api.api.oaipmh.base.metax_oai_server import MetaxOAIServer

        s = MetaxOAIServer()
        md = s._get_oai_dc_metadata(dc, dc.catalog_json)
        self.assertTrue("identifier" in md)
        self.assertTrue("title" in md)
        self.assertTrue("lang" in md["title"][0])

    def test_sensitive_fields_are_removed(self):
        """
        Ensure some sensitive fields are never present in output of OAI-PMH apis
        """
        sensitive_field_values = ["email@mail.com", "999-123-123", "999-456-456"]

        def _check_fields(content):
            """
            Verify sensitive fields values are not in the content. Checking for field value, instead
            of field name, since the field names might be different in Datacite etc other formats.
            """
            for sensitive_field_value in sensitive_field_values:
                self.assertEqual(
                    sensitive_field_value not in str(content),
                    True,
                    "field %s should have been stripped" % sensitive_field_value,
                )

        for cr in CatalogRecord.objects.filter(identifier=self.identifier):
            cr.research_dataset["curator"][0].update(
                {
                    "email": sensitive_field_values[0],
                    "phone": sensitive_field_values[1],
                    "telephone": sensitive_field_values[2],
                }
            )
            cr.force_save()

        response = self.client.get(
            "/oai/?verb=GetRecord&identifier=%s&metadataPrefix=oai_dc" % self.identifier
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        _check_fields(response.content)

        response = self.client.get(
            "/oai/?verb=GetRecord&identifier=%s&metadataPrefix=oai_datacite" % self.identifier
        )
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        _check_fields(response.content)
