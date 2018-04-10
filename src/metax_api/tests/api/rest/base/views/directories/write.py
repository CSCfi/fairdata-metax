from django.core.management import call_command
# from rest_framework import status
from rest_framework.test import APITestCase

# from metax_api.models import Directory
from metax_api.tests.utils import test_data_file_path, TestClassUtils

d = print


class DirectoryApiWriteCommon(APITestCase, TestClassUtils):
    @classmethod
    def setUpClass(cls):
        """
        Loaded only once for test cases inside this class.
        """
        call_command('loaddata', test_data_file_path, verbosity=0)
        super(DirectoryApiWriteCommon, cls).setUpClass()

    def setUp(self):
        """
        Reloaded for every test case
        """
        call_command('loaddata', test_data_file_path, verbosity=0)
        dir_from_test_data = self._get_object_from_test_data('directory')
        self.identifier = dir_from_test_data['identifier']
        self.directory_name = dir_from_test_data['directory_name']

        """
        New data that is sent to the server for POST, PUT, PATCH requests. Modified
        slightly as approriate for different purposes
        """
        self.test_new_data = self._get_new_test_data()
        self.second_test_new_data = self._get_second_new_test_data()
        self._use_http_authorization()

    def _get_new_test_data(self):
        from_test_data = self._get_object_from_test_data('directory', requested_index=0)
        from_test_data.update({
            "identifier": "urn:nbn:fi:csc-ida201401200000000001",
        })
        return from_test_data

    def _get_second_new_test_data(self):
        from_test_data = self._get_new_test_data()
        from_test_data.update({
            "identifier": "urn:nbn:fi:csc-ida201401200000000002",
        })
        return from_test_data
