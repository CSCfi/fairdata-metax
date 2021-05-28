# This file is part of the Metax API service
#
# Copyright 2017-2018 Ministry of Education and Culture, Finland
#
# :author: CSC - IT Center for Science Ltd., Espoo Finland <servicedesk@csc.fi>
# :license: MIT

from django.core.management import call_command
from django.test import TestCase

from metax_api.models import DataCatalog
from metax_api.tests.utils import TestClassUtils, test_data_file_path


class DataCatalogModelTests(TestCase, TestClassUtils):
    @classmethod
    def setUpClass(cls):
        """
        Loaded only once for test cases inside this class.
        """
        call_command("loaddata", test_data_file_path, verbosity=0)
        super(DataCatalogModelTests, cls).setUpClass()

    def setUp(self):
        self.dc = DataCatalog.objects.get(pk=1)

    def test_disallow_identifier_manual_update(self):
        dc = self.dc
        old = dc.catalog_json["identifier"]
        dc.catalog_json["identifier"] = "changed value"
        dc.save()
        self.assertEqual(old, dc.catalog_json["identifier"])
