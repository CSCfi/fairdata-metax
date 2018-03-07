from django.core.management import call_command
import lxml.etree
from rest_framework import status
from rest_framework.test import APITestCase

from metax_api.models import CatalogRecord
from metax_api.tests.utils import test_data_file_path, TestClassUtils


class OAIPMHReadTests(APITestCase, TestClassUtils):

    _namespaces = {'o': 'http://www.openarchives.org/OAI/2.0/',
                   'oai_dc': "http://www.openarchives.org/OAI/2.0/oai_dc/",
                   'dc': "http://purl.org/dc/elements/1.1/",
                   'dct': "http://purl.org/dc/terms/",
                   'datacite': 'http://schema.datacite.org/oai/oai-1.0/'}

    new_version = None

    @classmethod
    def setUpClass(cls):
        """
        Loaded only once for test cases inside this class.
        """
        call_command('loaddata', test_data_file_path, verbosity=0)
        super(OAIPMHReadTests, cls).setUpClass()

    def setUp(self):
        catalog_record_from_test_data = self._get_object_from_test_data('catalogrecord', requested_index=0)
        self.metadata_version_identifier = catalog_record_from_test_data['research_dataset']['preferred_identifier']

        self._use_http_authorization()

        if not self.new_version:
            self.new_version = self._get_new_version()

    def _get_new_test_cr_data(self, cr_index=0, dc_index=0, c_index=0):
        catalog_record_from_test_data = self._get_object_from_test_data('catalogrecord', requested_index=cr_index)

        catalog_record_from_test_data['research_dataset'].update({
            "metadata_version_identifier": "urn:nbn:fi:att:ec55c1dd-668d-43ae-b51b-f6c56a5bd4d6",
            "preferred_identifier": None,
            "other_identifier": [
                {
                    "notation": "urn:nbn:fi:csc-kata:12345",
                }
            ],
            "creator": [{
                "@type": "Person",
                "name": "Teppo Testaaja",
                "member_of": {
                    "@type": "Organization",
                    "name": {"fi": "Mysterious Organization"}
                }
            }],
            "curator": [{
                "@type": "Person",
                "name": "Default Owner",
                "member_of": {
                    "@type": "Organization",
                    "name": {"fi": "Mysterious Organization"}
                }
            }]
        })

        return catalog_record_from_test_data

    def _get_new_version(self):
        # add one versioned record for datasets/1
        target_catalog = 1
        data = {'research_dataset': self._get_new_test_cr_data(cr_index=0)['research_dataset']}
        data['research_dataset']['preferred_identifier'] = self.metadata_version_identifier + '-new-version'
        data['data_catalog'] = target_catalog

        response = self.client.patch('/rest/datasets/1', data, format="json")
        self.assertEqual(response.status_code, status.HTTP_200_OK)

        new_version = CatalogRecord.objects.get(pk=response.data['next_version']['id'])
        return new_version

    def _get_results(self, data, xpath):
        root = data
        if isinstance(data, bytes):
            root = lxml.etree.fromstring(data)
        return root.xpath(xpath, namespaces=self._namespaces)

    def _get_single_result(self, data, xpath):
        results = self._get_results(data, xpath)
        return results[0]

    def test_identity(self):
        response = self.client.get('/oai/?verb=Identity')
        self.assertEqual(response.status_code, status.HTTP_200_OK)

    def test_list_metadata_formats(self):
        response = self.client.get('/oai/?verb=ListMetadataFormats')
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        formats = self._get_single_result(response.content, '//o:ListMetadataFormats')
        self.assertEqual(len(formats), 3)

        metadataPrefix = self._get_results(formats, '//o:metadataPrefix[text() = "oai_dc"]')
        self.assertEqual(len(metadataPrefix), 1)

        metadataPrefix = self._get_results(formats, '//o:metadataPrefix[text() = "oai_datacite"]')
        self.assertEqual(len(metadataPrefix), 1)

        metadataPrefix = self._get_results(formats, '//o:metadataPrefix[text() = "oai_dc_urnresolver"]')
        self.assertEqual(len(metadataPrefix), 1)

    def test_list_sets(self):
        response = self.client.get('/oai/?verb=ListSets')
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        sets = self._get_results(response.content, '//o:setSpec')
        # urnresolver set should be hidden
        self.assertEquals(len(sets), 3)

    def test_list_identifiers(self):
        response = self.client.get('/oai/?verb=ListIdentifiers&metadataPrefix=oai_dc')
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        headers = self._get_results(response.content, '//o:header')
        self.assertTrue(len(headers) == 13, len(headers))

    def test_list_records(self):
        response = self.client.get('/oai/?verb=ListRecords&metadataPrefix=oai_dc')
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        records = self._get_results(response.content, '//o:record')
        self.assertTrue(len(records) == 13)

    def test_metadataformat_for_urn_resolver(self):
        response = self.client.get('/oai/?verb=ListRecords&metadataPrefix=oai_dc_urnresolver')
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        headers = self._get_results(response.content, '//o:header')
        self.assertTrue(len(headers) > 0)
        records = self._get_results(response.content, '//oai_dc:dc')
        for record_metadata in records:
            urn_elements = self._get_results(record_metadata, 'dc:identifier[starts-with(text(), "urn")]')
            self.assertTrue(urn_elements is not None or len(urn_elements) > 0)
            url_element = self._get_single_result(record_metadata, 'dc:identifier[starts-with(text(), "http")]')
            self.assertTrue(url_element is not None)

    def test_get_record(self):
        response = self.client.get(
            '/oai/?verb=GetRecord&identifier=%s&metadataPrefix=oai_dc' % self.metadata_version_identifier)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        identifiers = self._get_results(response.content,
                                        '//o:record/o:header/o:identifier[text()="%s"]'
                                        % self.metadata_version_identifier)
        self.assertTrue(len(identifiers) == 1, response.content)

    def test_get_record_non_existing(self):
        response = self.client.get('/oai/?verb=GetRecord&identifier=urn:non:existing&metadataPrefix=oai_dc')
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        errors = self._get_results(response.content, '//o:error[@code="idDoesNotExist"]')
        self.assertTrue(len(errors) == 1, response.content)

    def test_get_using_invalid_metadata_prefix(self):
        response = self.client.get('/oai/?verb=ListRecords&metadataPrefix=oai_notavailable')
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        errors = self._get_results(response.content, '//o:error[@code="cannotDisseminateFormat"]')
        self.assertTrue(len(errors) == 1, response.content)

        response = self.client.get(
            '/oai/?verb=GetRecord&identifier=%s&metadataPrefix=oai_notavailable' % self.metadata_version_identifier)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        errors = self._get_results(response.content, '//o:error[@code="cannotDisseminateFormat"]')
        self.assertTrue(len(errors) == 1, response.content)

    def test_get_datacite_record(self):
        response = self.client.get(
            '/oai/?verb=GetRecord&identifier=%s&metadataPrefix=oai_datacite' % self.metadata_version_identifier)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        identifiers = self._get_results(response.content,
                                        '//o:record/o:metadata/datacite:oai_datacite/' +
                                        'datacite:schemaVersion[text()="%s"]' % '4.1')
        self.assertTrue(len(identifiers) == 1, response.content)

    def test_get_urnresolver_record(self):
        response = self.client.get(
            '/oai/?verb=GetRecord&identifier=%s&metadataPrefix=oai_dc_urnresolver' % self.metadata_version_identifier)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        identifiers = self._get_results(response.content,
                                        '//o:record/o:metadata/oai_dc:dc/dc:identifier[text()="%s"]' %
                                        self.metadata_version_identifier)
        self.assertTrue(len(identifiers) == 1, response.content)

    def test_list_records_from_datasets_set(self):
        response = self.client.get('/oai/?verb=ListRecords&metadataPrefix=oai_dc&set=datasets')
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        records = self._get_results(response.content, '//o:record')
        self.assertTrue(len(records) == 13)

    def test_list_records_from_att_datasets_set(self):
        response = self.client.get('/oai/?verb=ListRecords&metadataPrefix=oai_dc&set=att_datasets')
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        records = self._get_results(response.content, '//o:record')
        self.assertTrue(len(records) == 12)

    def test_list_records_from_ida_datasets_set(self):
        response = self.client.get('/oai/?verb=ListRecords&metadataPrefix=oai_dc&set=ida_datasets')
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        records = self._get_results(response.content, '//o:record')
        self.assertTrue(len(records) == 1)

    def test_list_records_from_urnresolver_datasets_set(self):
        response = self.client.get('/oai/?verb=ListRecords&metadataPrefix=oai_dc&set=urnresolver')
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        records = self._get_results(response.content, '//o:record')
        self.assertTrue(len(records) == 13)

    def test_get_urnresolver_record_with_multiple_ids(self):
        response = self.client.get('/oai/?verb=GetRecord&identifier=%s&metadataPrefix=oai_dc_urnresolver'
                                   % self.new_version.research_dataset['preferred_identifier'])

        self.assertEqual(response.status_code, status.HTTP_200_OK)
        records = self._get_results(response.content, '//o:record')
        self.assertTrue(len(records) == 1)

        ids = self._get_results(records[0], '//o:record/o:metadata//dc:identifier')
        self.assertTrue(len(ids) == 3)

    def test_distinct_records_in_set(self):
        att_resp = self.client.get('/oai/?verb=ListRecords&metadataPrefix=oai_dc&set=att_datasets')
        self.assertEqual(att_resp.status_code, status.HTTP_200_OK)
        ida_resp = self.client.get('/oai/?verb=ListRecords&metadataPrefix=oai_dc&set=ida_datasets')
        self.assertEqual(ida_resp.status_code, status.HTTP_200_OK)

        att_records = self._get_results(att_resp.content, '//o:record')
        att_identifier = att_records[0][0][0].text

        ida_records = self._get_results(ida_resp.content,
                                        '//o:record/o:header/o:identifier[text()="%s"]' % att_identifier)
        self.assertTrue(len(ida_records) == 0)

    def test_get_records_from_invalid_set(self):
        response = self.client.get('/oai/?verb=ListRecords&metadataPrefix=oai_dc&set=invalid')
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        errors = self._get_results(response.content, '//o:error[@code="badArgument"]')
        self.assertTrue(len(errors) == 1, response.content)
