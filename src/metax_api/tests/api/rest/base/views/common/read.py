# This file is part of the Metax API service
#
# Copyright 2017-2018 Ministry of Education and Culture, Finland
#
# :author: CSC - IT Center for Science Ltd., Espoo Finland <servicedesk@csc.fi>
# :license: MIT

from datetime import timedelta

from django.utils import timezone
from pytz import timezone as tz
from rest_framework import status

from metax_api.models import CatalogRecord, File
from metax_api.tests.api.rest.base.views.datasets.read import CatalogRecordApiReadCommon


class ApiReadGetDeletedObjects(CatalogRecordApiReadCommon):

    """
    Test use of query parameter removed=bool, which is common for all apis
    """

    def test_removed_query_param(self):
        obj = CatalogRecord.objects.get(pk=1)
        obj.removed = True
        obj.force_save()
        obj2 = CatalogRecord.objects.get(pk=2)
        obj2.removed = True
        obj2.force_save()
        response = self.client.get('/rest/datasets/1')
        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND)
        response = self.client.get('/rest/datasets/1?removed=true')
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        response = self.client.get('/rest/datasets/metadata_version_identifiers')
        self.assertEqual(obj.metadata_version_identifier not in response.data, True)
        self.assertEqual(obj2.metadata_version_identifier not in response.data, True)

        obj = File.objects.get(pk=1)
        obj.removed = True
        obj.force_save()
        obj2 = File.objects.get(pk=2)
        obj2.removed = True
        obj2.force_save()
        response = self.client.get('/rest/files/1')
        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND)
        response = self.client.get('/rest/files/1?removed=true')
        self.assertEqual(response.status_code, status.HTTP_200_OK)

    def test_removed_parameter_gets_correct_amount_of_objects(self):
        path = '/rest/datasets'
        objects = CatalogRecord.objects.all().values()

        results = self.client.get('{0}?no_pagination&removed=false'.format(path)).json()
        initial_amt = len(results)

        results = self.client.get('{0}?no_pagination&removed=true'.format(path)).json()
        self.assertEqual(len(results), 0, "Without removed objects remove=true should return 0 results")

        self._use_http_authorization()
        amt_to_delete = 2
        for i in range(amt_to_delete):
            response = self.client.delete('{0}/{1}'.format(path, objects[i]['id']))
            self.assertEqual(response.status_code, status.HTTP_204_NO_CONTENT, response.data)

        results = self.client.get('{0}?no_pagination&removed=false'.format(path)).json()
        self.assertEqual(len(results), initial_amt - amt_to_delete, "Non-removed object amount is incorrect")

        results = self.client.get('{0}?no_pagination&removed=true'.format(path)).json()
        self.assertEqual(len(results), amt_to_delete, "Removed object amount is incorrect")


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

    def test_disable_pagination(self):
        response = self.client.get('/rest/datasets?no_pagination=true')
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual('next' not in response.data, True)
        self.assertEqual('results' not in response.data, True)

    def test_pagination_ordering(self):
        limit = 5

        for order in ('preservation_state', '-preservation_state'):

            # vary offset from 0 to 20, in increments of 5
            for offset in range(0, 20, 5):

                response = self.client.get(f'/rest/datasets?limit={limit}&offset={offset}&ordering={order}')
                self.assertEqual(response.status_code, status.HTTP_200_OK)

                from_api = [cr['preservation_state'] for cr in response.data['results']]

                from_db = [
                    r for r in CatalogRecord.objects
                    .filter()
                    .order_by(order)
                    .values_list('preservation_state', flat=True)[offset:offset + limit]
                ]

                self.assertEqual(from_api, from_db)


class ApiReadHTTPHeaderTests(CatalogRecordApiReadCommon):
    #
    # header if-modified-since tests, single
    #

    # If the value of the timestamp given in the header is equal or greater than the value of date_modified field,
    # 404 should be returned since nothing has been modified. If the value of the timestamp given in the header is
    # less than value of date_modified field, the object should be returned since it means the object has been
    # modified after the header timestamp

    def test_get_with_if_modified_since_header_ok(self):
        cr = CatalogRecord.objects.get(pk=self.pk)
        date_modified = cr.date_modified
        date_modified_in_gmt = timezone.localtime(date_modified, timezone=tz('GMT'))

        if_modified_since_header_value = date_modified_in_gmt.strftime('%a, %d %b %Y %H:%M:%S GMT')
        headers = {'HTTP_IF_MODIFIED_SINCE': if_modified_since_header_value}
        response = self.client.get('/rest/datasets/%s' % self.identifier, **headers)
        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND)

        if_modified_since_header_value = (date_modified_in_gmt + timedelta(seconds=1)).strftime(
            '%a, %d %b %Y %H:%M:%S GMT')
        headers = {'HTTP_IF_MODIFIED_SINCE': if_modified_since_header_value}
        response = self.client.get('/rest/datasets/%s' % self.identifier, **headers)
        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND)

        if_modified_since_header_value = (date_modified_in_gmt - timedelta(seconds=1)).strftime(
            '%a, %d %b %Y %H:%M:%S GMT')
        headers = {'HTTP_IF_MODIFIED_SINCE': if_modified_since_header_value}
        response = self.client.get('/rest/datasets/%s' % self.identifier, **headers)
        self.assertEqual(response.status_code, status.HTTP_200_OK)

    def test_get_with_if_modified_since_header_syntax_error(self):
        cr = CatalogRecord.objects.get(pk=self.pk)
        date_modified = cr.date_modified
        date_modified_in_gmt = timezone.localtime(date_modified, timezone=tz('GMT'))

        if_modified_since_header_value = date_modified_in_gmt.strftime('%a, %d %b %Y %H:%M:%S UTC')
        headers = {'HTTP_IF_MODIFIED_SINCE': if_modified_since_header_value}
        response = self.client.get('/rest/datasets/%s' % self.identifier, **headers)
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)

    #
    # header if-modified-since tests, list
    #

    # List operation returns always 200 even if no datasets match the if-modified-since criterium

    def test_list_get_with_if_modified_since_header_ok(self):
        cr = CatalogRecord.objects.get(pk=self.pk)
        date_modified = cr.date_modified
        date_modified_in_gmt = timezone.localtime(date_modified, timezone=tz('GMT'))

        if_modified_since_header_value = date_modified_in_gmt.strftime('%a, %d %b %Y %H:%M:%S GMT')
        headers = {'HTTP_IF_MODIFIED_SINCE': if_modified_since_header_value}
        response = self.client.get('/rest/datasets?limit=100', **headers)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertTrue(len(response.data.get('results')) == 6)

        if_modified_since_header_value = (date_modified_in_gmt + timedelta(seconds=1)).strftime(
            '%a, %d %b %Y %H:%M:%S GMT')
        headers = {'HTTP_IF_MODIFIED_SINCE': if_modified_since_header_value}
        response = self.client.get('/rest/datasets?limit=100', **headers)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertTrue(len(response.data.get('results')) == 6)

        # The asserts below may brake if the date_modified timestamps or the amount of test data objects are altered
        # in the test data

        if_modified_since_header_value = (date_modified_in_gmt - timedelta(seconds=1)).strftime(
            '%a, %d %b %Y %H:%M:%S GMT')
        headers = {'HTTP_IF_MODIFIED_SINCE': if_modified_since_header_value}
        response = self.client.get('/rest/datasets?limit=100', **headers)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertTrue(len(response.data.get('results')) > 6)
        self.assertTrue(len(response.data.get('results')) == 28)

        # should also work with records that have been recently created, and date_modified is empty

        cr.date_created = date_modified
        cr.date_modified = None
        cr.force_save()
        response = self.client.get('/rest/datasets?limit=100', **headers)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertTrue(len(response.data.get('results')) > 6)
        self.assertTrue(len(response.data.get('results')) == 28)


class ApiReadQueryParamTests(CatalogRecordApiReadCommon):

    """
    Misc common query params tests
    """

    def test_return_requested_fields_only(self):
        """
        While the param ?fields works with write operations too, the primary use case is when GETting.
        """
        response = self.client.get('/rest/datasets?fields=identifier')
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual('identifier' in response.data['results'][0], True)
        self.assertEqual(len(response.data['results'][0].keys()), 1)
        self.assertEqual(len(response.data['results'][1].keys()), 1)

        response = self.client.get('/rest/datasets/1?fields=identifier')
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual('identifier' in response.data, True)
        self.assertEqual(len(response.data.keys()), 1)

        response = self.client.get('/rest/datasets/1?fields=identifier,data_catalog')
        self.assertEqual('identifier' in response.data, True)
        self.assertEqual('data_catalog' in response.data, True)
        self.assertEqual(len(response.data.keys()), 2)

        response = self.client.get('/rest/datasets/1?fields=not_found')
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)

        # Anonymous user using fields parameter and not including research_dataset should not cause crashing
        self.client._credentials = {}
        response = self.client.get('/rest/datasets/1?fields=identifier')
        self.assertEqual(response.status_code, status.HTTP_200_OK)

    def test_checksum_field_for_file(self):
        """
        Check that checksum field works correctly
        """

        self._use_http_authorization('metax')
        response = self.client.get('/rest/files/1?fields=checksum')
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertTrue(response.data.get('checksum'), 'Checksum JSON should be returned')
        self.assertTrue(response.data['checksum'].get('algorithm'))
        self.assertTrue(response.data['checksum'].get('checked'))
        self.assertTrue(response.data['checksum'].get('value'))

        response = self.client.get('/rest/files/1?fields=checksum:value')
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertTrue(response.data.get('checksum'), 'Checksum JSON should be returned')
        self.assertTrue(response.data['checksum'].get('value'))
        self.assertFalse(response.data['checksum'].get('algorithm'))

        response = self.client.get('/rest/files/1?fields=checksum:badvalue')
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertTrue('is not part of' in response.data['detail'][0], 'Should complain about field not found')
