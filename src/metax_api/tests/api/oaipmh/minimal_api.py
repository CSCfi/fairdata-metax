from django.core.management import call_command
import lxml.etree
from rest_framework import status
from rest_framework.test import APITestCase

from metax_api.tests.utils import test_data_file_path, TestClassUtils


class OAIPMHReadTests(APITestCase, TestClassUtils):

    _namespaces = {'o': 'http://www.openarchives.org/OAI/2.0/',
                   'oai_dc': "http://www.openarchives.org/OAI/2.0/oai_dc/",
                   'dc': "http://purl.org/dc/elements/1.1/",
                   'dct': "http://purl.org/dc/terms/"}

    @classmethod
    def setUpClass(cls):
        """
        Loaded only once for test cases inside this class.
        """
        call_command('loaddata', test_data_file_path, verbosity=0)
        super(OAIPMHReadTests, cls).setUpClass()

    def setUp(self):
        catalog_record_from_test_data = self._get_object_from_test_data('catalogrecord', requested_index=0)
        self.urn_identifier = catalog_record_from_test_data['research_dataset']['urn_identifier']

    def _get_results(self, data, xpath):
        root = data
        if isinstance(data, bytes):
            root = lxml.etree.fromstring(data)
        return root.xpath(xpath, namespaces=self._namespaces)

    def _get_single_result(self, data, xpath):
        results = self._get_results(data, xpath)
        self.assertEquals(len(results), 1)
        return results[0]

    def test_identity(self):
        response = self.client.get('/oai/?verb=Identity')
        self.assertEqual(response.status_code, status.HTTP_200_OK)

    def test_list_metadata_formats(self):
        response = self.client.get('/oai/?verb=ListMetadataFormats')
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        #import ipdb;ipdb.set_trace(context=6)
        formats = self._get_single_result(response.content, '//o:ListMetadataFormats')
        self.assertEqual(len(formats), 1)

        format = formats[0]
        metadataPrefix = self._get_single_result(format, '//o:metadataPrefix')
        self.assertEqual(metadataPrefix.text, 'oai_dc')

    def test_list_sets(self):
        response = self.client.get('/oai/?verb=ListSets')
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        sets = self._get_results(response.content, '//o:setSpec')
        self.assertEquals(len(sets), 0)

    def test_list_identifiers(self):
        response = self.client.get('/oai/?verb=ListIdentifiers&metadataPrefix=oai_dc')
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        headers = self._get_results(response.content, '//o:header')
        self.assertTrue(len(headers) > 0)

    def test_list_records(self):
        response = self.client.get('/oai/?verb=ListRecords&metadataPrefix=oai_dc')
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        records = self._get_results(response.content, '//o:record')
        self.assertTrue(len(records) > 0)

    def test_list_records_for_urn_resolver(self):
        response = self.client.get('/oai/?verb=ListRecords&metadataPrefix=oai_dc')
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        headers = self._get_results(response.content, '//o:header')
        self.assertTrue(len(headers) > 0)
        records = self._get_results(response.content, '//oai_dc:dc')
        for record_metadata in records:
            urn_element = self._get_single_result(record_metadata, 'dc:identifier[starts-with(text(), "urn")]')
            url_element = self._get_single_result(record_metadata, 'dc:identifier[starts-with(text(), "http")]')
            self.assertTrue(urn_element is not None)
            self.assertTrue(url_element is not None)

    def test_get_record(self):
        response = self.client.get(
            '/oai/?verb=GetRecord&identifier=%s&metadataPrefix=oai_dc' % self.urn_identifier)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        identifiers = self._get_results(response.content,
            '//o:record/o:header/o:identifier[text()="%s"]' % self.urn_identifier)
        self.assertTrue(len(identifiers) == 1, response.content)

    def test_get_record_non_existing(self):
        response = self.client.get('/oai/?verb=GetRecord&identifier=urn:non:existing&metadataPrefix=oai_dc')
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        errors = self._get_results(response.content, '//o:error[@code="idDoesNotExist"]')
        self.assertTrue(len(errors) == 1, response.content)