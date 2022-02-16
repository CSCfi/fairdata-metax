from io import StringIO

from django.core.management import call_command
from django.test import TestCase

from metax_api.models import File
from metax_api.tests.utils import test_data_file_path


class RemoveFilesTest(TestCase):
    def setUp(self):
        call_command("loaddata", test_data_file_path, verbosity=0)

    def test_command_output(self):
        project_identifier = "project_x"
        path_prefix = "/project_x_FROZEN/Experiment_X/Phase_1/2017"

        out = StringIO()
        args = [project_identifier]
        options = {"path_prefix": path_prefix, "stdout": out}
        call_command("mark_files_removed", *args, **options)

        self.assertIn(
            f"Found 10 files to remove in project: {project_identifier} with path prefix: {path_prefix}",
            out.getvalue(),
        )
        self.assertIn("Removed 10 files", out.getvalue())

        files = File.objects_unfiltered.filter(
            project_identifier=project_identifier, file_path__startswith=path_prefix
        )

        self.assertEqual(10, len(files))
        for file in files:
            self.assertTrue(file.removed)
