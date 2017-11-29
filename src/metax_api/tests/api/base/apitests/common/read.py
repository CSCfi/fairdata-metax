from datetime import timedelta

from django.utils import timezone
from pytz import timezone as tz
from rest_framework import status

from metax_api.models import CatalogRecord
from metax_api.tests.api.base.apitests.catalog_records.read import CatalogRecordApiReadCommon


class ApiReadPaginationTests(CatalogRecordApiReadCommon):
    """
    pagination
    """

    def test_read_catalog_record_list_pagination_1(self):
        response = self.client.get('/rest/datasets?limit=2&offset=0')
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data['results']), 2, 'There should have been exactly two results')
        self.assertEqual(response.data['results'][0]['id'], 1, 'Id of first result should have been 1')

    def test_read_catalog_record_list_pagination_2(self):
        response = self.client.get('/rest/datasets?limit=2&offset=2')
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data['results']), 2, 'There should have been exactly two results')
        self.assertEqual(response.data['results'][0]['id'], 3, 'Id of first result should have been 3')


class ApiReadHTTPHeaderTests(CatalogRecordApiReadCommon):
    #
    # header if-modified-since tests, single
    #

    # If the value of the timestamp given in the header is equal or greater than the value of modified_by_api field,
    # 404 should be returned since nothing has been modified. If the value of the timestamp given in the header is
    # less than value of modified_by_api field, the object should be returned since it means the object has been
    # modified after the header timestamp

    def test_get_with_if_modified_since_header_ok(self):
        cr = CatalogRecord.objects.get(pk=self.pk)
        modified_by_api = cr.modified_by_api
        modified_by_api_in_gmt = timezone.localtime(modified_by_api, timezone=tz('GMT'))

        if_modified_since_header_value = modified_by_api_in_gmt.strftime('%a, %d %b %Y %H:%M:%S GMT')
        headers = {'HTTP_IF_MODIFIED_SINCE': if_modified_since_header_value}
        response = self.client.get('/rest/datasets/%s' % self.urn_identifier, **headers)
        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND)

        if_modified_since_header_value = (modified_by_api_in_gmt + timedelta(seconds=1)).strftime(
            '%a, %d %b %Y %H:%M:%S GMT')
        headers = {'HTTP_IF_MODIFIED_SINCE': if_modified_since_header_value}
        response = self.client.get('/rest/datasets/%s' % self.urn_identifier, **headers)
        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND)

        if_modified_since_header_value = (modified_by_api_in_gmt - timedelta(seconds=1)).strftime(
            '%a, %d %b %Y %H:%M:%S GMT')
        headers = {'HTTP_IF_MODIFIED_SINCE': if_modified_since_header_value}
        response = self.client.get('/rest/datasets/%s' % self.urn_identifier, **headers)
        self.assertEqual(response.status_code, status.HTTP_200_OK)

    def test_get_with_if_modified_since_header_syntax_error(self):
        cr = CatalogRecord.objects.get(pk=self.pk)
        modified_by_api = cr.modified_by_api
        modified_by_api_in_gmt = timezone.localtime(modified_by_api, timezone=tz('GMT'))

        if_modified_since_header_value = modified_by_api_in_gmt.strftime('%a, %d %b %Y %H:%M:%S UTC')
        headers = {'HTTP_IF_MODIFIED_SINCE': if_modified_since_header_value}
        response = self.client.get('/rest/datasets/%s' % self.urn_identifier, **headers)
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)

    #
    # header if-modified-since tests, list
    #

    # List operation returns always 200 even if no datasets match the if-modified-since criterium

    def test_list_get_with_if_modified_since_header_ok(self):
        cr = CatalogRecord.objects.get(pk=self.pk)
        modified_by_api = cr.modified_by_api
        modified_by_api_in_gmt = timezone.localtime(modified_by_api, timezone=tz('GMT'))

        if_modified_since_header_value = modified_by_api_in_gmt.strftime('%a, %d %b %Y %H:%M:%S GMT')
        headers = {'HTTP_IF_MODIFIED_SINCE': if_modified_since_header_value}
        response = self.client.get('/rest/datasets?limit=100', **headers)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertTrue(len(response.data.get('results')) == 2)

        if_modified_since_header_value = (modified_by_api_in_gmt + timedelta(seconds=1)).strftime(
            '%a, %d %b %Y %H:%M:%S GMT')
        headers = {'HTTP_IF_MODIFIED_SINCE': if_modified_since_header_value}
        response = self.client.get('/rest/datasets?limit=100', **headers)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertTrue(len(response.data.get('results')) == 2)

        # The asserts below may brake if the modified_by_api timestamps or the amount of test data objects are altered
        # in the test data

        if_modified_since_header_value = (modified_by_api_in_gmt - timedelta(seconds=1)).strftime(
            '%a, %d %b %Y %H:%M:%S GMT')
        headers = {'HTTP_IF_MODIFIED_SINCE': if_modified_since_header_value}
        response = self.client.get('/rest/datasets?limit=100', **headers)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertTrue(len(response.data.get('results')) > 2)
        self.assertTrue(len(response.data.get('results')) == 14)
