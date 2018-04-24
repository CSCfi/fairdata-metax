from io import StringIO

from django.conf import settings
from django.core.management import call_command
from django.test.testcases import LiveServerTestCase

from metax_api.models import DataCatalog, FileStorage


class LoadInitialDataTest(LiveServerTestCase):

    """
    Uses LiveServerTestCase, which starts up a real django testserver with hostname and all,
    which can then be passed to the management command to call the testserver api. More convenient
    perhaps than settings up mocked responses...
    """

    def test_command(self):
        catalogs_before = DataCatalog.objects.all().count()
        storages_before = FileStorage.objects.all().count()

        test_settings = {
            'metax_url': self.live_server_url,
            'metax_credentials': settings.API_METAX_USER,
        }
        out = StringIO()
        call_command('loadinitialdata', **{ 'test_settings': test_settings, 'stdout': out, })
        cmd_output = out.getvalue()

        self.assertIn('Created catalog', cmd_output)
        self.assertIn('Created storage', cmd_output)
        self.assertEqual(catalogs_before < DataCatalog.objects.all().count(), True)
        self.assertEqual(storages_before < FileStorage.objects.all().count(), True)
