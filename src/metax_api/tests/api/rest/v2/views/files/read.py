# This file is part of the Metax API service
#
# Copyright 2017-2018 Ministry of Education and Culture, Finland
#
# :author: CSC - IT Center for Science Ltd., Espoo Finland <servicedesk@csc.fi>
# :license: MIT

import responses
from django.core.management import call_command
from django.db import connection
from rest_framework import status
from rest_framework.test import APITestCase

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
        response = self.client.get('/rest/v2/files')
        self.assertEqual(response.status_code, status.HTTP_200_OK)

    def test_read_file_list_filter_by_project(self):
        proj = File.objects.get(pk=1).project_identifier
        file_count = File.objects.filter(project_identifier=proj).count()
        response = self.client.get('/rest/v2/files?project_identifier=%s' % proj)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data['count'], file_count)

    def test_read_file_list_filter_by_project_and_path(self):
        proj = File.objects.get(pk=1).project_identifier
        path = "/project_x_FROZEN/Experiment_X/Phase_1/2017/01"
        file_count = File.objects.filter(project_identifier=proj, file_path__contains=path).count()
        response = self.client.get('/rest/v2/files?project_identifier=%s&file_path=%s' % (proj, path))
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data['count'], file_count)

        # missing project_identifier
        response = self.client.get('/rest/v2/files?file_path=%s' % path)
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)

    def test_read_file_details_by_pk(self):
        response = self.client.get('/rest/v2/files/%s' % self.pk)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(hasattr(response, 'data'), True, 'Request response object is missing attribute \'data\'')
        self.assertEqual('file_name' in response.data, True)
        self.assertEqual(response.data['identifier'], self.identifier)
        self.assertEqual('identifier' in response.data['file_storage'], True)

    def test_read_file_details_by_identifier(self):
        response = self.client.get('/rest/v2/files/%s' % self.identifier)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(hasattr(response, 'data'), True, 'Request response object is missing attribute \'data\'')
        self.assertEqual('file_name' in response.data.keys(), True)
        self.assertEqual(response.data['identifier'], self.identifier)

    def test_read_file_details_not_found(self):
        response = self.client.get('/rest/v2/files/shouldnotexist')
        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND)

    def test_read_file_details_checksum_relation(self):
        response = self.client.get('/rest/v2/files/%s' % self.pk)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual('checksum' in response.data, True)
        self.assertEqual('value' in response.data['checksum'], True)

    def test_expand_relations(self):
        response = self.client.get('/rest/v2/files/1?expand_relation=file_storage,parent_directory')
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        self.assertEqual('file_storage_json' in response.data['file_storage'], True, response.data['file_storage'])
        self.assertEqual('date_created' in response.data['parent_directory'], True, response.data['parent_directory'])


class FileApiReadGetRelatedDatasets(FileApiReadCommon):

    def test_get_related_datasets_ok_1(self):
        """
        File pk 1 should belong to only 3 datasets
        """
        response = self.client.post('/rest/v2/files/datasets', [1], format='json')
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        self._assert_results_length(response, 3)

    def test_get_related_datasets_ok_2(self):
        """
        File identifiers listed below should belong to 5 datasets
        """
        file_identifiers = File.objects.filter(id__in=[1, 2, 3, 4, 5]).values_list('identifier', flat=True)
        response = self.client.post('/rest/v2/files/datasets', file_identifiers, format='json')
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        self._assert_results_length(response, 5)

    def test_keysonly(self):
        """
        Parameter ?keysonly should return just values
        """
        response = self.client.post('/rest/v2/files/datasets?keys=files&keysonly', [1, 2, 121], format='json')
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        self._assert_results_length(response, 2) # pid:urn:121 does not belong to any dataset
        self.assertEqual(type(response.data), list, type(response.data)) # no dict keys

        response = self.client.post('/rest/v2/files/datasets?keys=files&keysonly=false', [1, 2], format='json')
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        self.assertEqual(type(response.data), dict, response.data) # Return by keys

        response = self.client.post('/rest/v2/files/datasets?keys=datasets&keysonly', [1, 2, 14], format='json')
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        self._assert_results_length(response, 2) # Only datasets 1 and 2 have files
        self.assertEqual(type(response.data), list, type(response.data)) # no dict keys

    def test_get_detailed_related_datasets_ok_1(self):
        """
        File identifiers listed below should belong to 3 datasets
        """
        response = self.client.post('/rest/v2/files/datasets?keys=files', [1], format='json')
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        self._assert_results_length(response, 1)
        self.assertEqual(len(list(response.data.values())[0]), 3, response.data)

        # Support for ?detailed
        response = self.client.post('/rest/v2/files/datasets?detailed', [1], format='json')
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        self._assert_results_length(response, 1)
        self.assertEqual(len(list(response.data.values())[0]), 3, response.data)

    def test_get_detailed_related_datasets_ok_2(self):
        """
        File identifiers listed below should belong to 5 datasets
        """
        file_identifiers = [1, 2, 3, 4, 5]

        response = self.client.post('/rest/v2/files/datasets?keys=files', file_identifiers, format='json')
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        self._assert_results_length(response, 5)

        # set of all returned datasets
        self.assertEqual(len(set(sum(response.data.values(), []))), 5, response.data)

        # check if identifiers work
        file_identifiers = ['pid:urn:1', 'pid:urn:2', 'pid:urn:3', 'pid:urn:4', 'pid:urn:5']

        response = self.client.post('/rest/v2/files/datasets?keys=files', file_identifiers, format='json')
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        self._assert_results_length(response, 5)

        # set of all returned datasets
        self.assertEqual(len(set(sum(response.data.values(), []))), 5, response.data)

    def test_get_detailed_related_files_ok_1(self):
        """
        Dataset identifiers listed below should have 2 files
        """
        response = self.client.post('/rest/v2/files/datasets?keys=datasets', [1], format='json')
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        self._assert_results_length(response, 1)
        self.assertEqual(len(list(response.data.values())[0]), 2, response.data)

    def test_get_detailed_related_files_ok_2(self):
        """
        Tests that datasets return files correctly
        """
        dataset_identifiers = [1, 2, 3, 4, 5]

        response = self.client.post('/rest/v2/files/datasets?keys=datasets', dataset_identifiers, format='json')
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        self._assert_results_length(response, 5)

        # set of all returned datasets
        self.assertEqual(len(set(sum(response.data.values(), []))), 10, response.data)

        # check if identifiers work
        dataset_identifiers = ["cr955e904-e3dd-4d7e-99f1-3fed446f96d1",
                            "cr955e904-e3dd-4d7e-99f1-3fed446f96d2",
                            "cr955e904-e3dd-4d7e-99f1-3fed446f96d3",
                            "cr955e904-e3dd-4d7e-99f1-3fed446f96d4",
                            "cr955e904-e3dd-4d7e-99f1-3fed446f96d5"]

        response = self.client.post('/rest/v2/files/datasets?keys=datasets', dataset_identifiers, format='json')
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        self._assert_results_length(response, 5)

        # set of all returned datasets
        self.assertEqual(len(set(sum(response.data.values(), []))), 10, response.data)

    def test_get_right_files_and_datasets(self):
        """
        Check that returned files and datasets are the right ones
        """
        testfile = self._get_object_from_test_data('file')

        cr = self.client.get('/rest/v2/datasets/10', format='json')
        self.assertEqual(cr.status_code, status.HTTP_200_OK, cr.data)

        response = self.client.post('/rest/v2/files/datasets?keys=datasets', [cr.data['identifier']], format='json')
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        # cr 10 has 2 default files
        for keys, values in response.data.items():
            self.assertEqual(keys == 'cr955e904-e3dd-4d7e-99f1-3fed446f9610', True, response.data)
            self.assertEqual('pid:urn:19' and 'pid:urn:20' in values, True, response.data)

        response = self.client.post('/rest/files/datasets?keys=files', [testfile['identifier']], format='json')
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        # file 1 belongs to 3 datasets
        for keys, values in response.data.items():
            self.assertEqual(keys == 'pid:urn:1', True, response.data)
            self.assertEqual('cr955e904-e3dd-4d7e-99f1-3fed446f96d1' and 'cr955e904-e3dd-4d7e-99f1-3fed446f9612'
                and 'cr955e904-e3dd-4d7e-99f1-3fed446f9611' in values, True, response.data)

        # Dataset 11 has 20 files in a directory
        cr = self.client.get('/rest/v2/datasets/11', format='json')
        self.assertEqual(cr.status_code, status.HTTP_200_OK, cr.data)

        # Compare using return from different api
        files_in_cr11 = self.client.get('/rest/v2/datasets/11/files', format='json')
        self.assertEqual(files_in_cr11.status_code, status.HTTP_200_OK, files_in_cr11.data)
        identifiers = []
        [identifiers.append(i['identifier']) for i in files_in_cr11.data]

        response = self.client.post('/rest/v2/files/datasets?keys=datasets', [11], format='json')
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        # This should have the same file id's as the return from /rest/v2/datasets/11/files
        self.assertEqual(sorted(response.data['cr955e904-e3dd-4d7e-99f1-3fed446f9611']), sorted(identifiers),
            response.data)

        response = self.client.post('/rest/v2/files/datasets?keys=files', ['pid:urn:20'], format='json')
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        # Dataset 11 should be found from results
        self.assertTrue('cr955e904-e3dd-4d7e-99f1-3fed446f9611' in response.data['pid:urn:20'], response.data)

    def test_get_related_datasets_files_not_found(self):
        """
        When the files themselves are not found, 404 should be returned
        """
        response = self.client.post('/rest/v2/files/datasets', ['doesnotexist'], format='json')
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        self._assert_results_length(response, 0)

        response = self.client.post('/rest/v2/files/datasets?keys=files', ['doesnotexist'], format='json')
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        self._assert_results_length(response, 0)

        # Support for ?detailed
        response = self.client.post('/rest/v2/files/datasets?detailed', ['doesnotexist'], format='json')
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        self._assert_results_length(response, 0)

    def test_get_related_datasets_records_not_found(self):
        """
        When files are found, but no records for them, an empty list should be returned
        """
        with connection.cursor() as cr:
            # detach file pk 1 from any datasets
            cr.execute('delete from metax_api_catalogrecord_files where file_id = 1')

        response = self.client.post('/rest/v2/files/datasets', [1], format='json')
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        self._assert_results_length(response, 0)

        response = self.client.post('/rest/v2/files/datasets?keys=files', [1], format='json')
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        self._assert_results_length(response, 0)

        # Support for ?detailed
        response = self.client.post('/rest/v2/files/datasets?detailed', [1], format='json')
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        self._assert_results_length(response, 0)

    def _assert_results_length(self, response, length):
        self.assertTrue(isinstance(response.data, dict) or isinstance(response.data, list), response.data)
        self.assertEqual(len(response.data), length)


class FileApiReadEndUserAccess(FileApiReadCommon):

    def setUp(self):
        super().setUp()
        self.token = get_test_oidc_token()
        self._mock_token_validation_succeeds()

    @responses.activate
    def test_user_can_read_owned_files(self):
        '''
        Ensure users can only read files owned by them from /rest/v2/files api.
        '''

        # first read files without project access - should fail
        self._use_http_authorization(method='bearer', token=self.token)
        proj = File.objects.get(pk=1).project_identifier

        response = self.client.get('/rest/v2/files/1')
        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN, response.data)
        response = self.client.get('/rest/v2/files?project_identifier=%s' % proj)
        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN, response.data)
        response = self.client.get('/rest/files?pagination=false')
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        self.assertEqual(len(response.data), 0, 'should return 200 OK, but user projects has no files')

        # set user to same project as previous files and try again. should now succeed
        self.token['group_names'].append('IDA01:%s' % proj)
        self._use_http_authorization(method='bearer', token=self.token)

        response = self.client.get('/rest/v2/files')
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        self.assertEqual(len(response.data) > 0, True, 'user should only see their own files')

        response = self.client.get('/rest/v2/files/1')
        self.assertEqual(response.status_code, status.HTTP_200_OK)

        response = self.client.get('/rest/v2/files?project_identifier=%s' % proj)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
