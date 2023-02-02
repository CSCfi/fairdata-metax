import logging

from django.core.management.base import BaseCommand

from metax_api.models import DataCatalog, CatalogRecord
from metax_api.models.catalog_record import DataciteDOIUpdate
from metax_api.utils import catalog_allows_datacite_update
logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = """Resends the given datasets to Datacite"""

    def handle(self, *args, **options):
        cr_ids = options["catalog_record_identifiers"].split(",")
        crs = CatalogRecord.objects.filter(identifier__in = cr_ids)
        
        for cr in crs:
            if catalog_allows_datacite_update(cr.get_catalog_identifier()):
                logger.info(f"Resending Catalog Record: {cr}")
                DataciteDOIUpdate(cr, cr.research_dataset["preferred_identifier"], "update").__call__()

        logger.info("All Catalog Records resent to Datacite")

    def add_arguments(self, parser):
        parser.add_argument("catalog_record_identifiers", type=str,
                            help="Comma separated list of identifiers of the catalog records which are re-sent to Datacite")
