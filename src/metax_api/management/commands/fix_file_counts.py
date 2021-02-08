import logging

from django.core.management.base import BaseCommand

from metax_api.models import Directory

logger = logging.getLogger(__name__)

class Command(BaseCommand):
    def handle(self, *args, **options):
        dirs_with_no_files = Directory.objects.filter(file_count=0, parent_directory=None)
        logger.info(f"fix_file_counts command found {dirs_with_no_files.count()} directories with file_count=0")
        for dir in dirs_with_no_files:
            dir.calculate_byte_size_and_file_count()
            logger.info(f"folder has {dir.file_count} files after recalculation")
        logger.info(f"fix_file_counts command executed successfully")