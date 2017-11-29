from rest_framework import status

from metax_api.models import CatalogRecord
from metax_api.tests.api.base.apitests.catalog_records.write import CatalogRecordApiWriteCommon

"""
Common phenomenas that concern all API's.

The particular API selected for a test should not matter. However,
choosing to do tests using api /datasets is not a bad choice, since that
API is currently the most complex, where things are most likely to experience
a RUD.
"""


class ApiWriteHTTPHeaderTests(CatalogRecordApiWriteCommon):

    #
    # header if-unmodified-since tests, single
    #

    def test_update_with_if_unmodified_since_header_ok(self):
        self.test_new_data['preservation_description'] = 'damn this is good coffee'
        cr = CatalogRecord.objects.get(pk=1)
        headers = {'HTTP_IF_UNMODIFIED_SINCE': cr.modified_by_api.strftime('%a, %d %b %Y %H:%M:%S GMT')}
        response = self.client.put('/rest/datasets/%s' % self.urn_identifier, self.test_new_data, format="json",
                                   **headers)
        self.assertEqual(response.status_code, status.HTTP_204_NO_CONTENT, response.data)

    def test_update_with_if_unmodified_since_header_precondition_failed_error(self):
        self.test_new_data['preservation_description'] = 'the owls are not what they seem'
        headers = {'HTTP_IF_UNMODIFIED_SINCE': 'Wed, 23 Sep 2009 22:15:29 GMT'}
        response = self.client.put('/rest/datasets/%s' % self.urn_identifier, self.test_new_data, format="json",
                                   **headers)
        self.assertEqual(response.status_code, 412, 'http status should be 412 = precondition failed')

    def test_update_with_if_unmodified_since_header_syntax_error(self):
        self.test_new_data['preservation_description'] = 'the owls are not what they seem'
        cr = CatalogRecord.objects.get(pk=1)
        headers = {'HTTP_IF_UNMODIFIED_SINCE': cr.modified_by_api.strftime('%a, %d %b %Y %H:%M:%S UTC')}
        response = self.client.put('/rest/datasets/%s' % self.urn_identifier, self.test_new_data, format="json",
                                   **headers)
        self.assertEqual(response.status_code, 400, 'http status should be 400')

    #
    # header if-unmodified-since tests, list
    #

    def test_update_list_with_if_unmodified_since_header_ok(self):
        response = self.client.get('/rest/datasets/1', format="json")
        data_1 = response.data
        response = self.client.get('/rest/datasets/2', format="json")
        data_2 = response.data

        data_1['preservation_description'] = 'damn this is good coffee'
        data_2['preservation_description'] = 'damn this is good coffee also'

        headers = {'HTTP_IF_UNMODIFIED_SINCE': 'value is not checked'}
        response = self.client.put('/rest/datasets', [data_1, data_2], format="json", **headers)

        self.assertEqual(response.status_code, status.HTTP_204_NO_CONTENT, response.data)

    def test_update_list_with_if_unmodified_since_header_error_1(self):
        """
        One resource being updated was updated in the meantime, resulting in an error
        """
        response = self.client.get('/rest/datasets/1', format="json")
        data_1 = response.data
        response = self.client.get('/rest/datasets/2', format="json")
        data_2 = response.data

        data_1['preservation_description'] = 'damn this is good coffee'

        # should result in error for this record
        data_2['modified_by_api'] = '2002-01-01T10:10:10Z'

        headers = {'HTTP_IF_UNMODIFIED_SINCE': 'value is not checked'}
        response = self.client.put('/rest/datasets', [data_1, data_2], format="json", **headers)
        self.assertEqual(len(response.data['failed']) == 1, True, 'there should be only one failed update')
        self.assertEqual('modified' in response.data['failed'][0]['errors']['detail'][0], True,
                         'error should indicate resource has been modified')

    def test_update_list_with_if_unmodified_since_header_error_2(self):
        """
        Field modified_by_api is missing, while if-modified-since header is set, resulting in an error.
        """
        response = self.client.get('/rest/datasets/1', format="json")
        data_1 = response.data
        response = self.client.get('/rest/datasets/2', format="json")
        data_2 = response.data

        data_1['preservation_description'] = 'damn this is good coffee'

        # should result in error for this record
        data_2.pop('modified_by_api')

        headers = {'HTTP_IF_UNMODIFIED_SINCE': 'value is not checked'}
        response = self.client.patch('/rest/datasets', [data_1, data_2], format="json", **headers)
        self.assertEqual('required' in response.data['failed'][0]['errors']['detail'][0], True,
                         'error should be about field modified_by_api is required')

    def test_update_list_with_if_unmodified_since_header_error_3(self):
        """
        One resource being updated has never been modified before. Make sure that modified_by_api = None
        is an accepted value. The end result should be that the resource has been modified, since the
        server version has a timestamp set in modified_by_api.
        """
        response = self.client.get('/rest/datasets/1', format="json")
        data_1 = response.data
        response = self.client.get('/rest/datasets/2', format="json")
        data_2 = response.data

        data_1['preservation_description'] = 'damn this is good coffee'
        data_2['preservation_description'] = 'damn this is good coffee also'
        data_2['modified_by_api'] = None

        headers = {'HTTP_IF_UNMODIFIED_SINCE': 'value is not checked'}
        response = self.client.put('/rest/datasets', [data_1, data_2], format="json", **headers)
        self.assertEqual('modified' in response.data['failed'][0]['errors']['detail'][0], True,
                         'error should indicate resource has been modified')
