# This file is part of the Metax API service
#
# Copyright 2017-2018 Ministry of Education and Culture, Finland
#
# :author: CSC - IT Center for Science Ltd., Espoo Finland <servicedesk@csc.fi>
# :license: MIT

from django.db.models import Sum
from django.core.management import call_command
from rest_framework.test import APITestCase

from metax_api.models import Directory, File
from metax_api.tests.utils import test_data_file_path, TestClassUtils


class DirectoryModelTests(APITestCase, TestClassUtils):

    @classmethod
    def setUpClass(cls):
        """
        Loaded only once for test cases inside this class.
        """
        call_command('loaddata', test_data_file_path, verbosity=0)
        super(DirectoryModelTests, cls).setUpClass()

    def setUp(self):
        self._use_http_authorization()

    def test_calculate_byte_size_and_file_count(self):
        """
        Test the method for all projects in the test data, and verify resulting numbers
        for the root dir matches to what is in the db.
        """
        for root_dir in Directory.objects.filter(parent_directory_id=None):
            root_dir.calculate_byte_size_and_file_count()

            byte_size = File.objects.filter(project_identifier=root_dir.project_identifier) \
                .aggregate(Sum('byte_size'))['byte_size__sum']
            file_count = File.objects.filter(project_identifier=root_dir.project_identifier).count()

            response = self.client.get('/rest/directories/root?project=%s' % root_dir.project_identifier)
            self.assertEqual(response.data['byte_size'], byte_size)
            self.assertEqual(response.data['file_count'], file_count)

    def test_disallow_calculate_byte_size_and_file_count_for_non_root(self):
        with self.assertRaises(Exception):
            Directory.objects.get(pk=3).calculate_byte_size_and_file_count()
