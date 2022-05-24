import logging

from django.core.management.base import BaseCommand

from metax_api.models import DataCatalog, CatalogRecord
from django.forms.models import model_to_dict
logger = logging.getLogger(__name__)


class Command(BaseCommand):
    def handle(self, *args, **options):
        data_catalog = DataCatalog.objects.get(catalog_json__identifier=options["data_catalog_identifier"])
        crs = CatalogRecord.objects_unfiltered.filter(data_catalog=data_catalog, removed=True)
        logger.info(f"found {crs.count()} removed datasets")
        deleted = 0
        for cr in crs:
            logger.info(f"deleting CatalogRecord: {model_to_dict(cr)}")
            cr.delete(hard=True)
            deleted += 1

        logger.info(f"hard deleted {deleted} datasets")

    def add_arguments(self, parser):
        parser.add_argument("data_catalog_identifier", type=str,
                            help="Identifier of the data catalog where the datasets are deleted")
