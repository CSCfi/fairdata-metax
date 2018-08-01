# This file is part of the Metax API service
#
# Copyright 2017-2018 Ministry of Education and Culture, Finland
#
# :author: CSC - IT Center for Science Ltd., Espoo Finland <servicedesk@csc.fi>
# :license: MIT

from django.core.exceptions import FieldError
from django.core.management import call_command
from django.test import TestCase

from metax_api.models import CatalogRecord
from metax_api.tests.utils import test_data_file_path, TestClassUtils


class CommonModelTests(TestCase, TestClassUtils):

    def setUp(self):
        call_command('loaddata', test_data_file_path, verbosity=0)

    def get(self):
        return CatalogRecord.objects.get(pk=1)


class CommonModelTrackNormalFieldsTests(CommonModelTests):

    """
    Make sure CommonModel.field_changed(field_name) recognized changes correctly,
    and correctly raises errors when a field is not being tracked, or does not have
    a field's initial value loaded before checking.
    """

    def test_field_changed_ok(self):
        cr = self.get()
        cr.preservation_state = 1
        cr.force_save()

        cr = self.get()
        cr.preservation_state = 2
        self.assertEqual(cr.field_changed('preservation_state'), True)

    def test_field_not_changed_ok(self):
        cr = self.get()
        cr.preservation_state = 1
        cr.force_save()

        cr = self.get()
        cr.preservation_state = 1
        self.assertEqual(cr.field_changed('preservation_state'), False)

    def test_field_is_not_tracked(self):
        cr = self.get()
        with self.assertRaises(FieldError, msg='field is not tracked, so checking changes should be an error'):
            cr.field_changed('user_modified')

    def test_field_is_tracked_but_not_loaded(self):
        """
        If a field is not retrieved from the db durin record initialization (__init__()),
        then the field will not have its initial value loaded for tracking. That is OK,
        for the cases when that field is not otherwise being modified, to not do needless work.

        However, if the field does end up being modified, then the caller should make sure the
        field is included when the object is being created.
        """
        queryset = CatalogRecord.objects.filter(pk=1).only('id')
        cr = queryset[0]
        cr.preservation_state = 2
        with self.assertRaises(FieldError, msg='field is not loaded in init, so checking changes should be an error'):
            cr.field_changed('preservation_state')

    def test_field_is_tracked_and_explicitly_loaded(self):
        """
        Same as above, but the caller makes sure to include the field in the ORM query.
        """
        queryset = CatalogRecord.objects.filter(pk=1).only('preservation_state')
        cr = queryset[0]
        cr.preservation_state = 2
        self.assertEqual(cr.field_changed('preservation_state'), True)


class CommonModelTrackJsonFieldsTests(CommonModelTests):

    """
    Due to json field tracking using dot-notation such as research_dataset.preferred_identifier
    to track a specific field inside a json-field, such fields require separate testing.
    """

    def test_json_field_changed_ok(self):
        cr = self.get()
        cr.research_dataset['preferred_identifier'] = 'new'
        self.assertEqual(cr.field_changed('research_dataset.preferred_identifier'), True)

    def test_json_field_not_changed_ok(self):
        cr = self.get()
        cr.research_dataset['preferred_identifier'] = cr.research_dataset['preferred_identifier']
        self.assertEqual(cr.field_changed('research_dataset.preferred_identifier'), False)

    def test_json_field_is_tracked_but_not_loaded(self):
        queryset = CatalogRecord.objects.filter(pk=1).only('id')
        cr = queryset[0]
        cr.research_dataset['preferred_identifier'] = cr.research_dataset['preferred_identifier']
        with self.assertRaises(FieldError, msg='field is not loaded in init, so checking changes should be an error'):
            cr.field_changed('research_dataset.preferred_identifier')
