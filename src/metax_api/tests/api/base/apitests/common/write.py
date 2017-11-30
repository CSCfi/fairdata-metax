from django.core.management import call_command
from rest_framework import status
from rest_framework.test import APITestCase

from metax_api.models import CatalogRecord
from metax_api.tests.api.base.apitests.catalog_records.write import CatalogRecordApiWriteCommon
from metax_api.tests.utils import test_data_file_path, TestClassUtils


"""
Common phenomenas that concern all API's.

The particular API selected for a test should not matter. However,
choosing to do tests using api /datasets is not a bad choice, since that
API is currently the most complex, where things are most likely to experience
a RUD.
"""


class ApiWriteCommon(APITestCase, TestClassUtils):

    def setUp(self):
        call_command('loaddata', test_data_file_path, verbosity=0)
        catalog_record_from_test_data = self._get_object_from_test_data('catalogrecord')
        self.urn_identifier = catalog_record_from_test_data['research_dataset']['urn_identifier']
        self.pk = catalog_record_from_test_data['id']
        self.test_new_data = self._get_new_test_data()
        self._use_http_authorization()

    def _get_new_test_data(self):
        catalog_record_from_test_data = self._get_object_from_test_data('catalogrecord', requested_index=0)
        catalog_record_from_test_data.update({
            "data_catalog": 1,
        })
        catalog_record_from_test_data['research_dataset'].update({
            "preferred_identifier": None,
        })
        catalog_record_from_test_data.pop('id', None)
        catalog_record_from_test_data.pop('contract', None)
        return catalog_record_from_test_data


class ApiWriteCommonFieldsTests(ApiWriteCommon):

    def test_certain_create_fields_are_read_only_after_create(self):
        """
        The following fields should be read-only after initial creation of a resource:
        - date_created
        - user_created
        - service_created
        """
        response = self.client.post('/rest/datasets', self.test_new_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)

        # some of the fields could be empty in test data. that is fine tho, the point is that
        # they should not change later.
        orig_date_created = response.data.get('date_created', None)
        orig_user_created = response.data.get('user_created', None)
        orig_service_created = response.data.get('service_created', None)

        altered = response.data
        altered['date_created'] = altered['date_created'].replace('2017', '2010')
        altered['user_created'] = 'changed'
        altered['service_created'] = 'changed'

        response = self.client.put('/rest/datasets/%d' % altered['id'], altered, format="json")
        self.assertEqual(response.status_code, status.HTTP_204_NO_CONTENT, response.data)

        response = self.client.get('/rest/datasets/%d' % altered['id'], format="json")
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        self.assertEqual(orig_date_created, response.data.get('date_created', None))
        self.assertEqual(orig_user_created, response.data.get('user_created', None))
        self.assertEqual(orig_service_created, response.data.get('service_created', None))


class ApiWriteHTTPHeaderTests(CatalogRecordApiWriteCommon):

    #
    # header if-unmodified-since tests, single
    #

    def test_update_with_if_unmodified_since_header_ok(self):
        self.test_new_data['preservation_description'] = 'damn this is good coffee'
        cr = CatalogRecord.objects.get(pk=1)
        headers = {'HTTP_IF_UNMODIFIED_SINCE': cr.date_modified.strftime('%a, %d %b %Y %H:%M:%S GMT')}
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
        headers = {'HTTP_IF_UNMODIFIED_SINCE': cr.date_modified.strftime('%a, %d %b %Y %H:%M:%S UTC')}
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
        data_2['date_modified'] = '2002-01-01T10:10:10Z'

        headers = {'HTTP_IF_UNMODIFIED_SINCE': 'value is not checked'}
        response = self.client.put('/rest/datasets', [data_1, data_2], format="json", **headers)
        self.assertEqual(len(response.data['failed']) == 1, True, 'there should be only one failed update')
        self.assertEqual('modified' in response.data['failed'][0]['errors']['detail'][0], True,
                         'error should indicate resource has been modified')

    def test_update_list_with_if_unmodified_since_header_error_2(self):
        """
        Field date_modified is missing, while if-modified-since header is set, resulting in an error.
        """
        response = self.client.get('/rest/datasets/1', format="json")
        data_1 = response.data
        response = self.client.get('/rest/datasets/2', format="json")
        data_2 = response.data

        data_1['preservation_description'] = 'damn this is good coffee'

        # should result in error for this record
        data_2.pop('date_modified')

        headers = {'HTTP_IF_UNMODIFIED_SINCE': 'value is not checked'}
        response = self.client.patch('/rest/datasets', [data_1, data_2], format="json", **headers)
        self.assertEqual('required' in response.data['failed'][0]['errors']['detail'][0], True,
                         'error should be about field date_modified is required')

    def test_update_list_with_if_unmodified_since_header_error_3(self):
        """
        One resource being updated has never been modified before. Make sure that date_modified = None
        is an accepted value. The end result should be that the resource has been modified, since the
        server version has a timestamp set in date_modified.
        """
        response = self.client.get('/rest/datasets/1', format="json")
        data_1 = response.data
        response = self.client.get('/rest/datasets/2', format="json")
        data_2 = response.data

        data_1['preservation_description'] = 'damn this is good coffee'
        data_2['preservation_description'] = 'damn this is good coffee also'
        data_2['date_modified'] = None

        headers = {'HTTP_IF_UNMODIFIED_SINCE': 'value is not checked'}
        response = self.client.put('/rest/datasets', [data_1, data_2], format="json", **headers)
        self.assertEqual('modified' in response.data['failed'][0]['errors']['detail'][0], True,
                         'error should indicate resource has been modified')
