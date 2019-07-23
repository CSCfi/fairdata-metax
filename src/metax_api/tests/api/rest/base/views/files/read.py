# This file is part of the Metax API service
#
# Copyright 2017-2018 Ministry of Education and Culture, Finland
#
# :author: CSC - IT Center for Science Ltd., Espoo Finland <servicedesk@csc.fi>
# :license: MIT

from django.core.management import call_command
from django.db import connection
from rest_framework import status
from rest_framework.test import APITestCase
import responses

from metax_api.models import File
from metax_api.tests.utils import get_test_oidc_token, test_data_file_path, TestClassUtils


class FileApiReadCommon(APITestCase, TestClassUtils):
    @classmethod
    def setUpClass(cls):
        """
        Loaded only once for test cases inside this class.
        """
        call_command('loaddata', test_data_file_path, verbosity=0)
        super(FileApiReadCommon, cls).setUpClass()

    def setUp(self):
        file_from_test_data = self._get_object_from_test_data('file')
        self.identifier = file_from_test_data['identifier']
        self.pk = file_from_test_data['id']
        self._use_http_authorization()


class FileApiReadBasicTests(FileApiReadCommon):

    def test_read_file_list(self):
        response = self.client.get('/rest/files')
        self.assertEqual(response.status_code, status.HTTP_200_OK)

    def test_read_file_list_filter_by_project(self):
        proj = File.objects.get(pk=1).project_identifier
        file_count = File.objects.filter(project_identifier=proj).count()
        response = self.client.get('/rest/files?project_identifier=%s' % proj)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data['count'], file_count)

    def test_read_file_list_filter_by_project_and_path(self):
        proj = File.objects.get(pk=1).project_identifier
        path = "/project_x_FROZEN/Experiment_X/Phase_1/2017/01"
        file_count = File.objects.filter(file_path__contains=path).count()
        response = self.client.get('/rest/files?project_identifier=%s&file_path=%s' % (proj, path))
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data['count'], file_count)

        response = self.client.get('/rest/files?file_path=%s' % path)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertNotEqual(response.data['count'], file_count)

    def test_read_file_details_by_pk(self):
        response = self.client.get('/rest/files/%s' % self.pk)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(hasattr(response, 'data'), True, 'Request response object is missing attribute \'data\'')
        self.assertEqual('file_name' in response.data, True)
        self.assertEqual(response.data['identifier'], self.identifier)
        self.assertEqual('identifier' in response.data['file_storage'], True)

    def test_read_file_details_by_identifier(self):
        response = self.client.get('/rest/files/%s' % self.identifier)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(hasattr(response, 'data'), True, 'Request response object is missing attribute \'data\'')
        self.assertEqual('file_name' in response.data.keys(), True)
        self.assertEqual(response.data['identifier'], self.identifier)

    def test_read_file_details_not_found(self):
        response = self.client.get('/rest/files/shouldnotexist')
        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND)

    def test_read_file_details_checksum_relation(self):
        response = self.client.get('/rest/files/%s' % self.pk)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual('checksum' in response.data, True)
        self.assertEqual('value' in response.data['checksum'], True)

    def test_expand_relations(self):
        response = self.client.get('/rest/files/1?expand_relation=file_storage,parent_directory')
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        self.assertEqual('file_storage_json' in response.data['file_storage'], True, response.data['file_storage'])
        self.assertEqual('date_created' in response.data['parent_directory'], True, response.data['parent_directory'])


class FileApiReadGetRelatedDatasets(FileApiReadCommon):

    def test_get_related_datasets_ok_1(self):
        """
        File pk 1 should belong to only 3 datasets
        """
        response = self.client.post('/rest/files/datasets', [1], format='json')
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        self._assert_results_length(response, 3)

    def test_get_related_datasets_ok_2(self):
        """
        File identifiers listed below should belong to 5 datasets
        """
        file_identifiers = File.objects.filter(id__in=[1, 2, 3, 4, 5]).values_list('identifier', flat=True)
        response = self.client.post('/rest/files/datasets', file_identifiers, format='json')
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        self._assert_results_length(response, 5)

    def test_get_related_datasets_files_not_found(self):
        """
        When the files themselves are not found, 404 should be returned
        """
        response = self.client.post('/rest/files/datasets', ['doesnotexist'], format='json')
        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND, response.data)

    def test_get_related_datasets_records_not_found(self):
        """
        When files are found, but no records for them, an empty list should be returned
        """
        with connection.cursor() as cr:
            # detach file pk 1 from any datasets
            cr.execute('delete from metax_api_catalogrecord_files where file_id = 1')

        response = self.client.post('/rest/files/datasets', [1], format='json')
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        self._assert_results_length(response, 0)

    def _assert_results_length(self, response, length):
        self.assertEqual(isinstance(response.data, list), True, response.data)
        self.assertEqual(len(response.data), length)


class FileApiReadEndUserAccess(FileApiReadCommon):

    def setUp(self):
        super().setUp()
        self.token = get_test_oidc_token()
        self._mock_token_validation_succeeds()

    @responses.activate
    def test_user_can_read_owned_files(self):
        '''
        Ensure users can only read files owned by them from /rest/files api.
        '''

        # first read files without project access - should fail
        self._use_http_authorization(method='bearer', token=self.token)
        proj = File.objects.get(pk=1).project_identifier

        response = self.client.get('/rest/files/1')
        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN, response.data)
        response = self.client.get('/rest/files?project_identifier=%s' % proj)
        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN, response.data)
        response = self.client.get('/rest/files?no_pagination')
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        self.assertEqual(len(response.data), 0, 'should return 200 OK, but user projects has no files')

        # set user to same project as previous files and try again. should now succeed
        self.token['group_names'].append('fairdata:IDA01:%s' % proj)
        self._use_http_authorization(method='bearer', token=self.token)

        response = self.client.get('/rest/files')
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        self.assertEqual(len(response.data) > 0, True, 'user should only see their own files')

        response = self.client.get('/rest/files/1')
        self.assertEqual(response.status_code, status.HTTP_200_OK)

        response = self.client.get('/rest/files?project_identifier=%s' % proj)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
