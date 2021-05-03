import logging

from django.core.management.base import BaseCommand
from django.core.management import call_command

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    def handle(self, *args, **options):
        call_command("migrate")
        call_command("index_refdata")
        call_command("reload_refdata_cache")
        call_command("loaddata", "metax_api/tests/testdata/test_data.json")
        call_command("loadinitialdata")
        logger.info("All first time setup commands completed successfully")