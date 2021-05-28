import logging

from django.core.management.base import BaseCommand

from metax_api.models import CatalogRecord

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    def handle(self, *args, **options):
        CRS = CatalogRecord.objects.all()
        for catalog_record in CRS:
            catalog_record.calculate_directory_byte_sizes_and_file_counts()
            logger.info(f"Calculating {catalog_record.identifier} ")
        logger.info(f"fix_file_counts command executed successfully")
