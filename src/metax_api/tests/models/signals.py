# This file is part of the Metax API service
#
# Copyright 2017-2018 Ministry of Education and Culture, Finland
#
# :author: CSC - IT Center for Science Ltd., Espoo Finland <servicedesk@csc.fi>
# :license: MIT

from datetime import date

from django.core.management import call_command
from django.test import TestCase

from metax_api.models import CatalogRecord, CatalogRecordV2, DeletedObject, Directory, File
from metax_api.tests.utils import TestClassUtils, test_data_file_path


class SignalTests(TestCase, TestClassUtils):

    def setUp(self):
        call_command("loaddata", test_data_file_path, verbosity=0)
        self.today = date.today().strftime("%d/%m/%Y")

"""    def test_deleting_catalog_record_creates_new_deleted_object(self):
        # test that deleting CatalogRecord object creates a new deleted object
        CatalogRecord.objects_unfiltered.get(pk=1).delete(hard=True)
        deleted_object = DeletedObject.objects.last()
        self.assertEqual(deleted_object.model_name, "CatalogRecord")
        self.assertEqual(deleted_object.date_deleted.strftime("%d/%m/%Y"), self.today)

        # test that deleting CatalogRecordV2 object creates a new deleted object
        CatalogRecordV2.objects_unfiltered.get(pk=2).delete(hard=True)
        deleted_object_v2 = DeletedObject.objects.last()
        self.assertEqual(deleted_object_v2.model_name, "CatalogRecordV2")
        self.assertEqual(deleted_object_v2.date_deleted.strftime("%d/%m/%Y"), self.today)"""


