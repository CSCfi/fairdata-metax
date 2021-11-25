import logging

from django.core.management.base import BaseCommand
from metax_api.models import File
from metax_api.tests.utils import management_command_add_test_logs

logger = logging.getLogger(__name__)

class Command(BaseCommand):
	help = """Marks files which start with a given file path removed from a given project.
	File path prefix can be given as command line parameter or they can be read from a file.
	This command will produce duplicate prints, the first line is used in tests, but is not stored on logs.
	The second line is stored on logs, but can't be used by the tests."""

	def add_arguments(self, parser):
		parser.add_argument("project_identifier", type=str, help="Identifier of the project where the files are removed")
		parser.add_argument("--path_prefix", type=str, help="Prefix for the file path of the files that are removed")
		parser.add_argument("--path_prefix_file", type=str, help="Name of the file where to read the path prefixes (Required if --path-prefix is not given)")

	@management_command_add_test_logs(logger)
	def handle(self, *args, **options):
		logger.info("Remove files from database")
		if options["path_prefix"] is None and options["path_prefix_file"] is None:
			logger.error("path_prefix or path_prefix_file is required")
			return

		if options["path_prefix_file"]:
			path_prefixes = self.read_prefixes_from_file(options["path_prefix_file"])
		else:
			path_prefixes = [options["path_prefix"]]


		removed_files_sum = 0
		for prefix in path_prefixes:
			files = File.objects.filter(project_identifier = options["project_identifier"], file_path__startswith = prefix, removed = "f")
			logger.info(f"Found {len(files)} files to remove in project: {options['project_identifier']} with path prefix: {prefix}")
			for file in files:
				file.delete()
			removed_files_sum += len(files)

		logger.info(f"Removed {removed_files_sum} files")



	def read_prefixes_from_file(self, filename):
		with open(filename) as file:
			lines = file.readlines()
			lines = [line.rstrip() for line in lines]
		return lines
