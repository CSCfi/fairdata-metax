from django.core.management import call_command
from django.test import TestCase
from rest_framework.serializers import ValidationError

from metax_api.models import File
from metax_api.tests.utils import test_data_file_path, TestClassUtils

d = print


class FileModelBasicTest(TestCase, TestClassUtils):
    """
    Verify that the model works at least on a basic level
    """

    @classmethod
    def setUpClass(cls):
        """
        Loaded only once for test cases inside this class.
        """
        call_command('loaddata', test_data_file_path)
        super(FileModelBasicTest, cls).setUpClass()

    def setUp(self):
        file_from_test_data = self._get_object_from_test_data('file')
        self.identifier = file_from_test_data['identifier']
        self.result_file_name = file_from_test_data['file_name']

    def test_get_by_identifier(self):
        file = File.objects.get(identifier=self.identifier)
        self.assertEqual(file.file_name, self.result_file_name)


class FileManagerTests(TestCase, TestClassUtils):
    @classmethod
    def setUpClass(cls):
        call_command('loaddata', test_data_file_path, verbosity=0)
        super(FileManagerTests, cls).setUpClass()

    def setUp(self):
        file_from_test_data = self._get_object_from_test_data('file', requested_index=0)
        self.identifier = file_from_test_data['identifier']
        self.pk = file_from_test_data['id']

    def test_get_using_dict_with_id(self):
        row = {'id': 1, 'other_stuff': 'doesnt matter'}
        try:
            obj = File.objects.get(using_dict=row)
        except File.DoesNotExist:
            found = False
        else:
            found = True

        self.assertEqual(found, True, 'get with using_dict should have returned a result')
        self.assertEqual(obj.id, 1)

    def test_get_using_dict_with_identifier(self):
        row = {'identifier': self.identifier, 'other_stuff': 'doesnt matter'}
        try:
            obj = File.objects.get(using_dict=row)
        except File.DoesNotExist:
            found = False
        else:
            found = True

        self.assertEqual(found, True, 'get with using_dict should have returned a result')
        self.assertEqual(obj.id, 1)

    def test_get_using_dict_error_not_found_1(self):
        row = {'id': 101010, 'other_stuff': 'doesnt matter'}
        try:
            File.objects.get(using_dict=row)
        except File.DoesNotExist:
            found = False
        else:
            found = True

        self.assertEqual(found, False, 'get with using_dict should have not returned a result')

    def test_get_using_dict_error_preferred_identifier_not_allowed(self):
        row = {'research_dataset': {'preferred_identifier': 'urn:nbn:fi:att:e1a2a565-f4e5-4b55-bc92-790482e69845'},
               'other_stuff': 'doesnt matter'}
        try:
            File.objects.get(using_dict=row)
        except ValidationError:
            found = False
        else:
            found = True

        self.assertEqual(found, False,
                         'get with using_dict should have not returned a result, because preferred_identifier was used')

    def test_get_using_dict_error_identifier_field_missing(self):
        row = {'somefield': 111, 'other_stuff': 'doesnt matter'}
        try:
            File.objects.get(using_dict=row)
        except ValidationError:
            found = False
        else:
            found = True

        self.assertEqual(found, False,
                         'get with using_dict should have not returned a result because an identifier field is missing')
