import logging

from django.conf import settings
from django.core.management.base import BaseCommand

from metax_api.models import CatalogRecordV2

from metax_api.services import PIDMSService

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    def handle(self, *args, **options):
        logger.info("Migrating PIDs to PID MicroService")

        pid_ms_client = PIDMSService()
        catalogs_to_migrate = settings.PID_MS["CATALOGS_TO_MIGRATE"]

        crs_to_migrate = CatalogRecordV2.objects.filter(
            pid_migrated=None,
            data_catalog__catalog_json__identifier__in=catalogs_to_migrate,
            state="published",
            api_meta__version__in=[1, 2],
        )
        logger.info(f"Found {len(crs_to_migrate)} catalog record(s) to migrate")

        for cr in crs_to_migrate:
            pid_ms_client.insert_pid(cr)

        logger.info(f"Migrated {len(crs_to_migrate)} catalog record(s)")
