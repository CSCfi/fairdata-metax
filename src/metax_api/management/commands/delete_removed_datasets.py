import logging

from django.core.management.base import BaseCommand

from metax_api.models import DataCatalog, CatalogRecord
from django.forms.models import model_to_dict
logger = logging.getLogger(__name__)


class Command(BaseCommand):
    def handle(self, *args, **options):
        logger.info(f"{options=}")
        data_catalog = DataCatalog.objects.get(catalog_json__identifier=options["data_catalog_identifier"])
        del_limit = options["del_limit"]
        crs = CatalogRecord.objects_unfiltered.filter(data_catalog=data_catalog, removed=True)
        logger.info(f"found {crs.count()} removed datasets")
        logger.info(f"Will delete {del_limit} datasets at most")
        deleted = 0
        for cr in crs:
            logger.info(f"deleting CatalogRecord: {model_to_dict(cr)}")
            cr.delete(hard=True)
            deleted += 1
            if deleted >= del_limit:
                break

        logger.info(f"hard deleted {deleted} datasets")

    def add_arguments(self, parser):
        parser.add_argument("data_catalog_identifier", type=str,
                            help="Identifier of the data catalog where the datasets are deleted")
        parser.add_argument("--del-limit", type=int, help="Max number of datasets to delete")
