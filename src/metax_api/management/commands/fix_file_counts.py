import logging

from django.core.management.base import BaseCommand

from metax_api.models import Directory

logger = logging.getLogger(__name__)

class Command(BaseCommand):
    def handle(self, *args, **options):
        dirs_with_no_files = Directory.objects_unfiltered.all()
        dir_sum = dirs_with_no_files.count()
        logger.info(f"fix_file_counts command found {dir_sum} directories")
        i=0
        for dir in dirs_with_no_files:
            i += 1
            try:
                dir.calculate_byte_size_and_file_count()
            except Exception as e:
                logger.error(f"can't fix filecount for directory {i}/{dir_sum}")
            logger.info(f"folder {i}/{dir_sum} has {dir.file_count} files after recalculation")
        logger.info(f"fix_file_counts command executed successfully")