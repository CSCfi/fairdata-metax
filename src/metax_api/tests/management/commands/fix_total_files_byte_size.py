from django.core.management import call_command
from django.test import TestCase
from django.db.models import Model
from django.utils import timezone

from metax_api.models import CatalogRecord
from metax_api.tests.utils import test_data_file_path


class FixTotalFilesByteSizeTest(TestCase):
    def setUp(self):
        call_command("loaddata", test_data_file_path, verbosity=0)

    def test_command_output(self):
        # Wrong byte size should be fixed
        cr = CatalogRecord.objects.first()  # has 300 bytes of files
        cr.research_dataset["total_files_byte_size"] = 1234567
        Model.save(cr)

        # Missing byte size should be fixed
        cr2 = CatalogRecord.objects.get(id=2)  # has 700 bytes of files
        cr2.research_dataset.pop("total_files_byte_size")
        Model.save(cr2)

        # Byte size should be fixed also for removed datasets
        cr3 = CatalogRecord.objects.get(id=3)  # has 1100 bytes of files
        cr3.research_dataset["total_files_byte_size"] = 789
        cr3.date_removed = timezone.now()
        cr3.removed = True
        Model.save(cr3)

        call_command("fix_total_files_byte_size")
        cr.refresh_from_db()
        self.assertEqual(cr.research_dataset["total_files_byte_size"], 300)
        cr2.refresh_from_db()
        self.assertEqual(cr2.research_dataset["total_files_byte_size"], 700)
        cr3.refresh_from_db()
        self.assertEqual(cr3.research_dataset["total_files_byte_size"], 1100)

        # ATT dataset should not get total_files_byte_size
        cr4 = CatalogRecord.objects.get(id=14)  # has no files
        self.assertEqual(cr4.files(manager="objects_unfiltered").count(), 0)
        self.assertIsNone(cr4.research_dataset.get("total_files_byte_size"))
