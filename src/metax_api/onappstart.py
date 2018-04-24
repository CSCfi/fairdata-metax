import logging
import sys
from os import makedirs
from shutil import rmtree
from time import sleep

from django.apps import AppConfig
from django.conf import settings

from metax_api.utils import RedisSentinelCache, executing_test_case, ReferenceDataLoader

_logger = logging.getLogger(__name__)
d = logging.getLogger(__name__).debug

class OnAppStart(AppConfig):

    name = 'metax_api'
    verbose_name = "Metax API"

    def ready(self): # pragma: no cover
        """
        Execute various tasks during application startup:

        - Populate cache with reference data from elasticsearch if it is missing
        - If the process is started by a test case, always flush the cache db used by the tests

        Usually there are many app processes being ran by gunicorn, but this action should only be executed
        once. The key 'on_app_start_executing' in the cache is set to true by the fastest process,
        informing other processes to not proceed further.
        """
        if any(cmd in sys.argv for cmd in ['manage.py']):
            return

        cache = RedisSentinelCache(master_only=True)

        # ex = expiration in seconds
        if not cache.get_or_set('on_app_start_executing', True, ex=120):
            # d('another process is already executing startup tasks, skipping')
            return

        # actual startup tasks ->
        _logger.info('Metax API startup tasks executing...')

        try:
            if executing_test_case():
                cache.get_master().flushdb()

            if settings.ELASTICSEARCH['ALWAYS_RELOAD_REFERENCE_DATA_ON_RESTART']:
                cache.set('reference_data', None)

            if not cache.get('reference_data'):
                ReferenceDataLoader.populate_cache_reference_data(cache)
            else:
                # d('cache already populated')
                pass
        except:
            raise
        finally:
            # ensure other processes have stopped at on_app_start_executing
            # before resetting the flag. (on local this method can be quite fast)
            sleep(2)
            cache.delete('on_app_start_executing')

        if executing_test_case():
            # reset error files location between tests
            rmtree(settings.ERROR_FILES_PATH, ignore_errors=True)

        try:
            makedirs(settings.ERROR_FILES_PATH)
        except FileExistsError:
            pass

        _logger.info('Metax API startup tasks finished')
