# This file is part of the Metax API service
#
# Copyright 2017-2018 Ministry of Education and Culture, Finland
#
# :author: CSC - IT Center for Science Ltd., Espoo Finland <servicedesk@csc.fi>
# :license: MIT

from django.core.management import call_command
from rest_framework import status
from rest_framework.test import APITestCase

from metax_api.models import CatalogRecordV2, Directory, File
from metax_api.tests.utils import TestClassUtils, test_data_file_path


class FileRPCTests(APITestCase, TestClassUtils):

    def setUp(self):
        """
        Reloaded for every test case
        """
        super().setUp()
        call_command('loaddata', test_data_file_path, verbosity=0)
        self._use_http_authorization()

class DeleteProjectTests(FileRPCTests):

    """
    Tests cover user authentication, wrong type of requests and
    correct result for successful operations.
    """

    def test_wrong_parameters(self):
        # correct user, no project identifier
        response = self.client.post('/rpc/v2/files/delete_project')
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)

        # nonexisting project identifier:
        response = self.client.post('/rpc/v2/files/delete_project?project_identifier=non_existing')
        self.assertEqual(response.data['deleted_files_count'], 0)

        # wrong request method
        response = self.client.delete('/rpc/v2/files/delete_project?project_identifier=research_project_112')
        self.assertEqual(response.status_code, 501)

        # wrong user
        self._use_http_authorization('api_auth_user')
        response = self.client.post('/rpc/v2/files/delete_project?project_identifier=research_project_112')
        self.assertEqual(response.status_code, status.HTTP_200_OK)

    def test_known_project_identifier(self):
        response = self.client.post('/rpc/v2/files/delete_project?project_identifier=research_project_112')
        self.assertEqual(response.status_code, status.HTTP_200_OK)

    def test_files_are_marked_deleted(self):
        files_count_before = File.objects.filter(project_identifier='research_project_112').count()
        response = self.client.post('/rpc/v2/files/delete_project?project_identifier=research_project_112')
        self.assertEqual(files_count_before, response.data['deleted_files_count'])

    def test_directories_are_deleted(self):
        self.client.post('/rpc/v2/files/delete_project?project_identifier=research_project_112')
        directories_count_after = Directory.objects.filter(project_identifier='research_project_112').count()
        self.assertEqual(directories_count_after, 0)

    def test_datasets_are_marked_deprecated(self):
        file_ids = File.objects.filter(project_identifier='project_x').values_list('id', flat=True)
        related_dataset = CatalogRecordV2.objects.filter(files__in=file_ids).distinct('id')[0]

        response = self.client.post('/rpc/v2/files/delete_project?project_identifier=project_x')
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)

        response = self.client.get('/rest/v2/datasets/%s' % related_dataset.identifier)
        self.assertEqual(response.data['deprecated'], True)
