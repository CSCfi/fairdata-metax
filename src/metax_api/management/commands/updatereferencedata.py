import logging

from django.core.management.base import BaseCommand, CommandError

from metax_api.utils import RedisSentinelCache, ReferenceDataLoader


_logger = logging.getLogger(__name__)


class Command(BaseCommand):

    help = 'Reload reference data to cache from ElasticSearch'

    def handle(self, *args, **options):
        self._update_reference_data()

    def _update_reference_data(self):
        _logger.info('Updating reference data...')
        try:
            cache = RedisSentinelCache(master_only=True)
            ReferenceDataLoader.populate_cache_reference_data(cache)
        except Exception as e:
            _logger.exception('Reference data update ended in an error: %s' % str(e))
            raise CommandError(e)
        _logger.info('Reference data updated')
