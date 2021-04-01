import logging

from django.core.management.base import BaseCommand

from metax_api.models import CatalogRecord

logger = logging.getLogger(__name__)

class Command(BaseCommand):
    def handle(self, *args, **options):
        CRS = CatalogRecord.objects.all()
        crs_sum = CRS.count()
        logger.info(f"fix_file_counts command found {crs_sum} catalog records with file_count=0 and byte_size=0")
        i = 1
        for catalog_record in CRS:
            logger.info(f"Calculating {i}/{crs_sum} {catalog_record.identifier} ")
            catalog_record.calculate_directory_byte_sizes_and_file_counts()
            i += 1
        logger.info(f"fix_file_counts command executed successfully")