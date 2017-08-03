from time import sleep
from django.apps import AppConfig
from django.conf import settings

from metax_api.utils import RedisSentinelCache, executing_test_case, ReferenceDataService

import logging
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
        cache = RedisSentinelCache(master_only=True)

        if cache.get('on_app_start_executing'):
            # d('another process is already executing startup tasks, skipping')
            return

        cache.set('on_app_start_executing', True)

        # ensure other processes have stopped at on_app_start_executing
        # before we reset the flag.
        sleep(2)

        # reset the flag right after sleep, to ensure the reset is never prevented by any
        # unforeseen errors or cosmic rays during execution, which might block this method on a future restart.
        cache.set('on_app_start_executing', False)

        # actual startup tasks ->

        if executing_test_case():
            cache.get_master().flushdb()

        if settings.ELASTICSEARCH['ALWAYS_RELOAD_REFERENCE_DATA_ON_RESTART']:
            cache.set('reference_data', None)

        if cache.get('reference_data'):
            # d('cache already populated')
            return

        ReferenceDataService.populate_cache_reference_data(cache)
