from datetime import datetime, timedelta
from decimal import Decimal
from jsonschema import validate as json_validate

from django.core.management import call_command
from django.test import TestCase
from rest_framework.serializers import ValidationError

from metax_api.tests.utils import get_json_schema, datetime_format, test_data_file_path, TestClassUtils
from metax_api.models import File

d = print

class FileModelBasicTest(TestCase, TestClassUtils):

    """
    Verify that at least test data is correct on a basic level, and the model contains
    the expected fields.
    """

    file_field_names = (
        'byte_size',
        'checksum_algorithm',
        'checksum_checked',
        'checksum_value',
        'download_url',
        'file_deleted',
        'file_frozen',
        'file_format',
        'file_modified',
        'file_name',
        'file_path',
        'file_storage',
        'file_uploaded',
        'identifier',
        'file_characteristics',
        'file_characteristics_extension',
        'open_access',
        'project_identifier',
        'replication_path',

        # backwards relations
        'xmlmetadata',
        'catalogrecord',
    )

    common_fields_names = (
        'id',
        'active',
        'removed',
        'modified_by_api',
        'modified_by_user_id',
        'created_by_api',
        'created_by_user_id',
    )

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

    def test_validate_file_characteristics_json(self):
        file = File.objects.get(identifier=self.identifier)

        try:
            json_validate(file.file_characteristics, get_json_schema('file'))
            json_validation_success = True
        except Exception as e:
            d(e)
            json_validation_success = False

        self.assertEqual(json_validation_success, True, 'file.file_characteristics failed json schema validation')

    def test_model_fields_as_expected(self):
        actual_model_fields = [ f.name for f in File._meta.get_fields() ]
        self._test_model_fields_as_expected(self.file_field_names + self.common_fields_names, actual_model_fields)

    def test_model_field_values(self):
        test_file_data = self._get_object_from_test_data('file')
        test_file_characteristics = test_file_data.pop('file_characteristics')
        test_file_data.pop('file_storage')
        file = File.objects.get(identifier=self.identifier)
        self._dict_comparison(test_file_data, file)
        self._dict_comparison(test_file_characteristics, file.file_characteristics)

    def _dict_comparison(self, test_data, object):
        """
        Run through all keys in test data/object and see that all expected fields are present
        and their values are as expected.
        """
        for field in test_data.keys():

            if isinstance(object, dict):
                # dict, or "json field"
                value = object[field]
            else:
                # django model
                value = getattr(object, field)

            try:
                if isinstance(value, Decimal):
                    self.assertEqual(Decimal(test_data[field]), Decimal(value))
                elif isinstance(value, datetime):
                    # timezones, man
                    self.assertEqual(datetime.strptime(test_data[field], datetime_format) + timedelta(hours=3), value)
                else:
                    self.assertEqual(test_data[field], value)
            except Exception as e:
                d(type(value))
                d(field)
                d(test_data[field])
                d(value)
                raise


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
        row = { 'id': 1, 'other_stuff': 'doesnt matter' }
        try:
            obj = File.objects.get(using_dict=row)
        except File.DoesNotExist:
            found = False
        else:
            found = True

        self.assertEqual(found, True, 'get with using_dict should have returned a result')
        self.assertEqual(obj.id, 1)

    def test_get_using_dict_with_identifier(self):
        row = { 'identifier': self.identifier, 'other_stuff': 'doesnt matter' }
        try:
            obj = File.objects.get(using_dict=row)
        except File.DoesNotExist:
            found = False
        else:
            found = True

        self.assertEqual(found, True, 'get with using_dict should have returned a result')
        self.assertEqual(obj.id, 1)

    def test_get_using_dict_error_not_found_1(self):
        row = { 'id': 101010, 'other_stuff': 'doesnt matter'}
        try:
            File.objects.get(using_dict=row)
        except File.DoesNotExist:
            found = False
        else:
            found = True

        self.assertEqual(found, False, 'get with using_dict should have not returned a result')

    def test_get_using_dict_error_preferred_identifier_not_allowed(self):
        row = { 'research_dataset': { 'preferred_identifier': 'pid:urn:cr1' }, 'other_stuff': 'doesnt matter' }
        try:
            File.objects.get(using_dict=row)
        except ValidationError:
            found = False
        else:
            found = True

        self.assertEqual(found, False, 'get with using_dict should have not returned a result, because preferred_identifier was used')

    def test_get_using_dict_error_identifier_field_missing(self):
        row = { 'somefield': 111, 'other_stuff': 'doesnt matter'}
        try:
            File.objects.get(using_dict=row)
        except ValidationError:
            found = False
        else:
            found = True

        self.assertEqual(found, False, 'get with using_dict should have not returned a result because an identifier field is missing')
