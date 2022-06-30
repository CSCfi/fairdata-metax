import logging

from django.core.management.base import BaseCommand

from metax_api.models import CatalogRecord

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    def handle(self, *args, **options):
        CRS = CatalogRecord.objects.all()
        crs_sum = CRS.count()
        i = 1
        for catalog_record in CRS:
            logger.info(f"Calculating {i}/{crs_sum} {catalog_record.identifier} ")
            catalog_record._calculate_total_files_byte_size(save_cr=True)
            i += 1
        logger.info(f"fix_total_files_byte_size command executed successfully")
