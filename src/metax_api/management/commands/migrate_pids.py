import logging

from django.conf import settings
from django.core.management.base import BaseCommand

from metax_api.models import CatalogRecordV2

from metax_api.services import PIDMSService

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    allow_fail = False
    failed_pids = []
    successful_pids = []

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.failed_pids = []
        self.successful_pids = []

    def add_arguments(self, parser):
        parser.add_argument(
            "--allow-fail",
            required=False,
            action="store_true",
            default=False,
            help="Allow individual PIDs to fail without halting the migration",
        )

    def handle(self, *args, **options):
        logger.info("Migrating PIDs to PID MicroService")
        self.allow_fail = options.get("allow_fail")

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
            identifier = cr.identifier
            try:
                pid_ms_client.insert_pid(cr)
                self.successful_pids.append(identifier)
            except Exception as e:
                if self.allow_fail:
                    logger.info(repr(e))
                    logger.info(f"Exception migrating PID {identifier}\n\n")
                    self.failed_pids.append(identifier)
                else:
                    logger.error(f"Failed while processing {identifier}")
                    raise

        logger.info(f"Succesfully migrated {len(self.successful_pids)} catalog record(s)")
        if len(self.failed_pids) > 0:
            logger.info(f"Failed to migrate pids for these datasets: {self.failed_pids}")
