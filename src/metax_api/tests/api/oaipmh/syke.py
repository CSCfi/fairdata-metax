from django.core.management import call_command
import lxml.etree
from rest_framework import status
from rest_framework.test import APITestCase
from metax_api.models import CatalogRecord
from metax_api.api.oaipmh.base.metax_oai_server import syke_url_prefix_template

from metax_api.tests.utils import test_data_file_path, TestClassUtils


class SYKEOAIPMHReadTests(APITestCase, TestClassUtils):

    _namespaces = {'o': 'http://www.openarchives.org/OAI/2.0/',
                   'oai_dc': "http://www.openarchives.org/OAI/2.0/oai_dc/",
                   'dc': "http://purl.org/dc/elements/1.1/",
                   'dct': "http://purl.org/dc/terms/",
                   'datacite': 'http://schema.datacite.org/oai/oai-1.0/'}

    @classmethod
    def setUpClass(cls):
        """
        Loaded only once for test cases inside this class.
        """
        call_command('loaddata', test_data_file_path, verbosity=0)
        super(SYKEOAIPMHReadTests, cls).setUpClass()

    def setUp(self):
        cr = CatalogRecord.objects.get(pk=1)
        cr.data_catalog.catalog_json["identifier"] = "urn:nbn:fi:att:data-catalog-harvest-syke"
        cr.data_catalog.force_save()
        cr.research_dataset.update({
            "preferred_identifier": "urn:nbn:fi:csc-kata20170613100856741858",
            "other_identifier": [
                {
                    "notation": "{55AB842F-9CED-4E80-A7E5-07A54F0AE4A4}"
                }
            ]
        })
        cr.force_save()
        self.identifier = cr.identifier
        self.pref_identifier = cr.research_dataset['preferred_identifier']
        self.dc = cr.data_catalog.catalog_json["identifier"]
        self._use_http_authorization()


    def _get_results(self, data, xpath):
        root = data
        if isinstance(data, bytes):
            root = lxml.etree.fromstring(data)
        return root.xpath(xpath, namespaces=self._namespaces)

    def test_get_urn_resolver_record(self):
        response = self.client.get(
            '/oai/?verb=GetRecord&identifier=%s&metadataPrefix=oai_dc_urnresolver' % self.identifier)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        identifiers = self._get_results(response.content,
            '//o:record/o:metadata/oai_dc:dc/dc:identifier[text()="%s"]' % self.pref_identifier)
        self.assertTrue(len(identifiers) == 1, response.content)

        syke_url = syke_url_prefix_template % '{55AB842F-9CED-4E80-A7E5-07A54F0AE4A4}'
        identifiers = self._get_results(response.content,
            '//o:record/o:metadata/oai_dc:dc/dc:identifier[text()="%s"]' % syke_url)
        self.assertTrue(len(identifiers) == 1, response.content)
