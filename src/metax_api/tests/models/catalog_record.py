# This file is part of the Metax API service
#
# Copyright 2017-2018 Ministry of Education and Culture, Finland
#
# :author: CSC - IT Center for Science Ltd., Espoo Finland <servicedesk@csc.fi>
# :license: MIT

from django.core.management import call_command
from django.test import TestCase
from rest_framework.serializers import ValidationError

from metax_api.models import CatalogRecord, File
from metax_api.tests.utils import TestClassUtils, test_data_file_path


class CatalogRecordModelBasicTest(TestCase, TestClassUtils):
    """
    Verify that the model at least works on the basic level.
    """

    @classmethod
    def setUpClass(cls):
        """
        Loaded only once for test cases inside this class.
        """
        call_command("loaddata", test_data_file_path, verbosity=0)
        super(CatalogRecordModelBasicTest, cls).setUpClass()

    def setUp(self):
        dataset_from_test_data = self._get_object_from_test_data("catalogrecord")
        self.metadata_version_identifier = dataset_from_test_data["research_dataset"][
            "metadata_version_identifier"
        ]
        self.identifier = dataset_from_test_data["identifier"]

    def test_get_by_identifiers(self):
        catalog_record = CatalogRecord.objects.get(identifier=self.identifier)
        self.assertEqual(catalog_record.identifier, self.identifier)

        catalog_record = CatalogRecord.objects.get(
            research_dataset__contains={
                "metadata_version_identifier": self.metadata_version_identifier
            }
        )
        self.assertEqual(
            catalog_record.metadata_version_identifier, self.metadata_version_identifier
        )


class CatalogRecordModelTests(TestCase, TestClassUtils):
    @classmethod
    def setUpClass(cls):
        """
        Loaded only once for test cases inside this class.
        """
        call_command("loaddata", test_data_file_path, verbosity=0)
        super(CatalogRecordModelTests, cls).setUpClass()

    def setUp(self):
        self.cr = CatalogRecord.objects.get(pk=1)

    def test_disallow_total_files_byte_size_manual_update(self):
        cr = self.cr
        old = cr.research_dataset["total_files_byte_size"]
        cr.research_dataset["total_files_byte_size"] = 999
        cr.save()
        self.assertEqual(old, cr.research_dataset["total_files_byte_size"])

    def test_disallow_metadata_version_identifier_manual_update(self):
        cr = self.cr
        old = cr.research_dataset["metadata_version_identifier"]
        cr.research_dataset["metadata_version_identifier"] = "changed"
        cr.save()
        self.assertEqual(old, cr.research_dataset["metadata_version_identifier"])

    def test_total_files_byte_size_auto_update_on_files_changed(self):
        """
        Changing files of a dataset creates a new version, so make sure that the file size
        of the old version does NOT change, and the file size of the new version DOES change.
        """
        already_included_files = (
            CatalogRecord.objects.get(pk=1).files.all().values_list("id", flat=True)
        )
        new_file_id = File.objects.all().exclude(id__in=already_included_files).first().id
        file_from_testdata = self._get_object_from_test_data("file", requested_index=new_file_id)

        cr = CatalogRecord.objects.get(pk=1)
        old = cr.research_dataset["total_files_byte_size"]
        cr.research_dataset["files"] = [file_from_testdata]
        cr.save()
        new_version = cr.next_dataset_version

        self.assertEqual(old, cr.research_dataset["total_files_byte_size"])
        self.assertNotEqual(old, new_version.research_dataset["total_files_byte_size"])

    def test_preservation_state_modified_auto_update(self):
        cr = self.cr
        old = cr.preservation_state_modified
        cr.preservation_state = 1
        cr.save()
        self.assertNotEqual(
            old,
            cr.preservation_state_modified,
            "preservation_state_modified should be automatically updated if changed",
        )


class CatalogRecordManagerTests(TestCase, TestClassUtils):
    @classmethod
    def setUpClass(cls):
        call_command("loaddata", test_data_file_path, verbosity=0)
        super(CatalogRecordManagerTests, cls).setUpClass()

    def test_get_using_dict_with_id(self):
        row = {"id": 1, "other_stuff": "doesnt matter"}
        try:
            obj = CatalogRecord.objects.get(using_dict=row)
        except CatalogRecord.DoesNotExist:
            found = False
        else:
            found = True

        self.assertEqual(found, True, "get with using_dict should have returned a result")
        self.assertEqual(obj.id, 1)

    def test_get_using_dict_with_metadata_version_identifier(self):
        row = {
            "research_dataset": {
                "metadata_version_identifier": CatalogRecord.objects.first().metadata_version_identifier
            },
            "other_stuff": "doesnt matter",
        }
        try:
            obj = CatalogRecord.objects.get(using_dict=row)
        except CatalogRecord.DoesNotExist:
            found = False
        else:
            found = True

        self.assertEqual(found, True, "get with using_dict should have returned a result")
        self.assertEqual(obj.id, 1)

    def test_get_using_dict_error_not_found_1(self):
        row = {"id": 101010, "other_stuff": "doesnt matter"}
        try:
            CatalogRecord.objects.get(using_dict=row)
        except CatalogRecord.DoesNotExist:
            found = False
        else:
            found = True

        self.assertEqual(found, False, "get with using_dict should have not returned a result")

    def test_get_using_dict_error_preferred_identifier_not_allowed(self):
        row = {
            "research_dataset": {
                "preferred_identifier": CatalogRecord.objects.first().preferred_identifier
            },
            "other_stuff": "doesnt matter",
        }
        try:
            CatalogRecord.objects.get(using_dict=row)
        except ValidationError:
            found = False
        else:
            found = True

        self.assertEqual(
            found,
            False,
            "get with using_dict should have not returned a result, because preferred_identifier was used",
        )

    def test_get_using_dict_error_identifier_field_missing(self):
        row = {"somefield": 111, "other_stuff": "doesnt matter"}
        try:
            CatalogRecord.objects.get(using_dict=row)
        except ValidationError:
            found = False
        else:
            found = True

        self.assertEqual(
            found,
            False,
            "get with using_dict should have not returned a result because an identifier field is missing",
        )
