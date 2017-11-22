from django.core.management import call_command
# from metax_api.models import Directory
from metax_api.tests.utils import test_data_file_path, TestClassUtils
# from rest_framework import status
from rest_framework.test import APITestCase

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


# class DirectoryApiWriteCreateTests(DirectoryApiWriteCommon):

#     #
#     #
#     #
#     # create apis
#     #
#     #
#     #

#     def test_create_file(self):
#         newly_created_directory_name = 'newly_created_directory_name'
#         self.test_new_data['directory_name'] = newly_created_directory_name
#         self.test_new_data['identifier'] = 'urn:nbn:fi:csc-thisisanewurn'

#         response = self.client.post('/rest/directories', self.test_new_data, format="json")

#         self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)
#         self.assertEqual('directory_name' in response.data.keys(), True)
#         self.assertEqual(response.data['directory_name'], newly_created_directory_name)

#     #
#     # create list operations
#     #

#     def test_create_directory_list(self):
#         newly_created_directory_name = 'newly_created_directory_name'
#         self.test_new_data['directory_name'] = newly_created_directory_name
#         self.test_new_data['identifier'] = 'urn:nbn:fi:csc-thisisanewurn'
#         self.second_test_new_data['identifier'] = 'urn:nbn:fi:csc-thisisanewurnalso'

#         response = self.client.post('/rest/directories', [self.test_new_data, self.second_test_new_data], format="json")
#         self.assertEqual(response.status_code, status.HTTP_201_CREATED)
#         self.assertEqual('success' in response.data.keys(), True)
#         self.assertEqual('failed' in response.data.keys(), True)
#         self.assertEqual('object' in response.data['success'][0].keys(), True)
#         self.assertEqual(len(response.data['success']), 2)
#         self.assertEqual(len(response.data['failed']), 0)


# class DirectoryApiWriteUpdateTests(DirectoryApiWriteCommon):

#     """
#     update operations PUT
#     """

#     def test_update_file(self):
#         response = self.client.put('/rest/directories/%s' % self.identifier, self.test_new_data, format="json")
#         self.assertEqual(response.status_code, status.HTTP_204_NO_CONTENT)
#         self.assertEqual(len(response.data.keys()), 0, 'Returned dict should be empty')


# class DirectoryApiWritePartialUpdateTests(DirectoryApiWriteCommon):

#     """
#     update operations PATCH
#     """

#     def test_update_file_partial(self):
#         new_data = {
#             "directory_name": "new_directory_name",
#         }
#         response = self.client.patch('/rest/directories/%s' % self.identifier, new_data, format="json")

#         self.assertEqual(response.status_code, status.HTTP_200_OK)
#         self.assertEqual('directory_name' in response.data.keys(), True)
#         self.assertEqual('file_path' in response.data.keys(), True, 'PATCH operation should return full content')
#         self.assertEqual(response.data['directory_name'], 'new_directory_name', 'Field directory_name was not updated')


# class DirectoryApiWriteDeleteTests(DirectoryApiWriteCommon):

#     """
#     DELETE /directories/pid
#     """

#     OTHER_PATH = '/animals/pets/cats'
#     OTHER_PATH_DEEPER = '/animals/pets/cats/furry'
#     OTHER_PATH_SIMILAR = '/animals/pets/cats_poisonous'

#     def setUp(self):
#         super(DirectoryApiWriteDeleteTests, self).setUp()
#         self._insert_files_with_file_paths([
#             self.OTHER_PATH,
#             self.OTHER_PATH_DEEPER,
#             self.OTHER_PATH_SIMILAR,
#         ])

#     def test_delete_directory(self):
#         file = File.objects.get(pk=1)
#         file_count = File.objects.filter(file_path=file.file_path).count()

#         response = self.client.delete('/rest/directories?path=%s' % file.file_path)
#         self.assertEqual(response.status_code, status.HTTP_200_OK)
#         self.assertEqual(response.data['affected_files'], file_count, response.data)

#         response = self.client.get('/rest/directories/%d' % file.id)
#         self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND)

#         try:
#             deleted_file = File.objects_unfiltered.get(pk=file.id)
#         except File.DoesNotExist:
#             raise Exception('Deleted file should not be deleted from the db, but marked as removed')

#         self.assertEqual(deleted_file.removed, True)
#         self.assertEqual(deleted_file.file_path.startswith(file.file_path), True, deleted_file.file_path)

#     def test_delete_directory_path_not_found(self):
#         file_count_before = File.objects.all().count()
#         response = self.client.delete('/rest/directories?path=/should/not/exist')
#         file_count_after = File.objects.all().count()

#         self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND)
#         self.assertEqual(file_count_before, file_count_after)

#     def test_delete_directory_path_is_required_parameter(self):
#         response = self.client.delete('/rest/directories')
#         self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
#         self.assertEqual('required' in response.data['detail'], True, response.data)

#     def test_delete_directory_paths_are_not_greedy(self):
#         """
#         File path must match in full, partial match must not delete files:
#         1) animals/pets/cats
#         2) animals/pets/cats/furry
#         3) animals/pets/cats_poisonous

#         when 1) is deleted, 2) is deleted as well, 3) must not be affected
#         """

#         # note that django orm filter 'file_path__startswith' is greedy, so this filter returns
#         # everything that starts with OTHER_PATH -> 3 results.
#         #
#         # the delete_directory() operation in the api however is not greedy, it will only
#         # match full exact file path, OR all file paths that continue after
#         # OTHER_PATH + /.
#         file_count_before = File.objects.filter(file_path__startswith=self.OTHER_PATH).count()

#         response = self.client.delete('/rest/directories?path=%s' % self.OTHER_PATH)

#         file_count_after = File.objects.filter(file_path__startswith=self.OTHER_PATH).count()

#         self.assertEqual(response.status_code, status.HTTP_200_OK)
#         self.assertEqual(response.data['affected_files'], 2, response.data)
#         self.assertEqual(file_count_after, file_count_before - 2, 'only one file should have been deleted')

#     def test_delete_directory_generic_apis_give_errors(self):
#         self.assertEqual(self.client.get('/rest/directories').status_code, status.HTTP_404_NOT_FOUND)
#         self.assertEqual(self.client.post('/rest/directories').status_code, status.HTTP_405_METHOD_NOT_ALLOWED)
#         self.assertEqual(self.client.get('/rest/directories/1').status_code, status.HTTP_404_NOT_FOUND)
#         self.assertEqual(self.client.put('/rest/directories').status_code, status.HTTP_405_METHOD_NOT_ALLOWED)
#         self.assertEqual(self.client.put('/rest/directories/1').status_code, status.HTTP_404_NOT_FOUND)
#         self.assertEqual(self.client.patch('/rest/directories').status_code, status.HTTP_404_NOT_FOUND)
#         self.assertEqual(self.client.patch('/rest/directories/1').status_code, status.HTTP_404_NOT_FOUND)

#     def _insert_files_with_file_paths(self, file_paths):
#         fs = FileStorage.objects.get(pk=1)
#         for idx, path in enumerate(file_paths):
#             from_test_data = self._get_object_from_test_data('file')
#             from_test_data.update({
#                 'identifier': '%s-%d' % (from_test_data['identifier'], idx),
#                 'file_path': path,
#                 'file_storage': fs,
#                 'id': None,
#             })
#             file = File(**from_test_data)
#             file.save()
