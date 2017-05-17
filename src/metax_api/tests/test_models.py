from django.core.management import call_command
from django.test import TestCase

from metax_api.models import File

class FileTest(TestCase):

    def setUp(self):
        call_command('loaddata', 'metax_api/tests/test_data.json')

    def test_read(self):
        file = File.objects.get(pk="00492195-58de-4343-ad75-4d82011d131e")
        self.assertEqual(file.file_name, "testifile 2")
