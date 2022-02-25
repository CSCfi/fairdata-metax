# This file is part of the Metax API service
#
# Copyright 2017-2018 Ministry of Education and Culture, Finland
#
# :author: CSC - IT Center for Science Ltd., Espoo Finland <servicedesk@csc.fi>
# :license: MIT

from io import StringIO
import unittest

from django.conf import settings
from django.core.management import call_command
from django.test.testcases import LiveServerTestCase

from metax_api.models import DataCatalog, FileStorage


@unittest.skip("out of date with current implementation")
class LoadInitialDataTest(LiveServerTestCase):

    """
    Uses LiveServerTestCase, which starts up a real django testserver with hostname and all,
    which can then be passed to the management command to call the testserver api. More convenient
    perhaps than settings up mocked responses...
    """

    def setUp(self, *args, **kwargs):
        self._test_settings = {
            "metax_url": self.live_server_url,
            "metax_credentials": settings.API_METAX_USER,
        }
        super(LoadInitialDataTest, self).setUp(*args, **kwargs)

    def test_create(self):
        catalogs_before = DataCatalog.objects.all().count()
        storages_before = FileStorage.objects.all().count()

        out = StringIO()
        call_command(
            "loadinitialdata",
            **{
                "test_settings": self._test_settings,
                "stdout": out,
            },
        )
        cmd_output = out.getvalue()

        self.assertIn("Created catalog", cmd_output)
        self.assertIn("Created file storage", cmd_output)
        self.assertEqual(catalogs_before < DataCatalog.objects.all().count(), True)
        self.assertEqual(storages_before < FileStorage.objects.all().count(), True)

    def test_update(self):
        """
        If data already exists, it should be updated instead.
        """
        out = StringIO()
        # create
        call_command(
            "loadinitialdata",
            **{
                "test_settings": self._test_settings,
                "stdout": out,
            },
        )
        # update
        call_command(
            "loadinitialdata",
            **{
                "test_settings": self._test_settings,
                "stdout": out,
            },
        )
        cmd_output = out.getvalue()

        self.assertIn("Updated catalog", cmd_output)
        self.assertIn("Updated file storage", cmd_output)
