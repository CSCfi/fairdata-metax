import logging

from django.core.management.base import BaseCommand

from metax_api.services.redis_cache_service import RedisClient
from metax_api.utils import ReferenceDataLoader

_logger = logging.getLogger(__name__)


class Command(BaseCommand):
    def handle(self, *args, **options):
        try:
            cache = RedisClient()
            cache.set("reference_data", None)

            ReferenceDataLoader.populate_cache_reference_data(cache)
            _logger.info(f"event='reference_data_loaded'")
        except Exception as e:
            _logger.error(e)
            raise e
