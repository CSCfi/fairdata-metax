from django.core.management import call_command
from django.test import TestCase
from rest_framework.serializers import ValidationError

from metax_api.tests.utils import test_data_file_path, TestClassUtils
from metax_api.models import CatalogRecord

class CatalogRecordModelBasicTest(TestCase, TestClassUtils):

    """
    Verify that the model at least works on the basic level.
    """

    @classmethod
    def setUpClass(cls):
        """
        Loaded only once for test cases inside this class.
        """
        call_command('loaddata', test_data_file_path, verbosity=0)
        super(CatalogRecordModelBasicTest, cls).setUpClass()

    def setUp(self):
        dataset_from_test_data = self._get_object_from_test_data('catalogrecord')
        self.urn_identifier = dataset_from_test_data['research_dataset']['urn_identifier']

    def test_get_by_identifier(self):
        catalog_record = CatalogRecord.objects.get(research_dataset__contains={ 'urn_identifier': self.urn_identifier })
        self.assertEqual(catalog_record.urn_identifier, self.urn_identifier)


class CatalogRecordManagerTests(TestCase, TestClassUtils):

    @classmethod
    def setUpClass(cls):
        call_command('loaddata', test_data_file_path, verbosity=0)
        super(CatalogRecordManagerTests, cls).setUpClass()

    def test_get_using_dict_with_id(self):
        row = { 'id': 1, 'other_stuff': 'doesnt matter' }
        try:
            obj = CatalogRecord.objects.get(using_dict=row)
        except CatalogRecord.DoesNotExist:
            found = False
        else:
            found = True

        self.assertEqual(found, True, 'get with using_dict should have returned a result')
        self.assertEqual(obj.id, 1)

    def test_get_using_dict_with_urn_identifier(self):
        row = { 'research_dataset': { 'urn_identifier': 'pid:urn:cr1' }, 'other_stuff': 'doesnt matter' }
        try:
            obj = CatalogRecord.objects.get(using_dict=row)
        except CatalogRecord.DoesNotExist:
            found = False
        else:
            found = True

        self.assertEqual(found, True, 'get with using_dict should have returned a result')
        self.assertEqual(obj.id, 1)

    def test_get_using_dict_error_not_found_1(self):
        row = { 'id': 101010, 'other_stuff': 'doesnt matter'}
        try:
            CatalogRecord.objects.get(using_dict=row)
        except CatalogRecord.DoesNotExist:
            found = False
        else:
            found = True

        self.assertEqual(found, False, 'get with using_dict should have not returned a result')

    def test_get_using_dict_error_preferred_identifier_not_allowed(self):
        row = { 'research_dataset': { 'preferred_identifier': 'pid:urn:cr1' }, 'other_stuff': 'doesnt matter' }
        try:
            CatalogRecord.objects.get(using_dict=row)
        except ValidationError:
            found = False
        else:
            found = True

        self.assertEqual(found, False, 'get with using_dict should have not returned a result, because preferred_identifier was used')

    def test_get_using_dict_error_identifier_field_missing(self):
        row = { 'somefield': 111, 'other_stuff': 'doesnt matter'}
        try:
            CatalogRecord.objects.get(using_dict=row)
        except ValidationError:
            found = False
        else:
            found = True

        self.assertEqual(found, False, 'get with using_dict should have not returned a result because an identifier field is missing')
