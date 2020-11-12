# This file is part of the Metax API service
#
# Copyright 2017-2018 Ministry of Education and Culture, Finland
#
# :author: CSC - IT Center for Science Ltd., Espoo Finland <servicedesk@csc.fi>
# :license: MIT

import logging
import sys
from os import makedirs, getpid
from shutil import rmtree
from time import sleep

from django.apps import AppConfig
from django.conf import settings
from icecream import ic

from metax_api.utils import executing_test_case, json_logger, ReferenceDataLoader

_logger = logging.getLogger(__name__)


class OnAppStart(AppConfig):

    name = 'metax_api'
    verbose_name = "Metax API"
    _pid = getpid()

    def ready(self): # pragma: no cover
        """
        Execute various tasks during application startup:

        - Populate cache with reference data from elasticsearch if it is missing
        - If the process is started by a test case, always flush the cache db used by the tests

        Usually there are many app processes being ran by gunicorn, but this action should only be executed
        once. The key 'on_app_start_executing' in the cache is set to true by the fastest process,
        informing other processes to not proceed further.
        """

        # some imports from metax_api cannot be done at the beginning of the file,
        # because the "django apps" have not been loaded yet.
        from metax_api.services import RedisCacheService as cache, RabbitMQService as rabbitmq
        from metax_api.services.redis_cache_service import RedisClient

        _logger.info(f"event='process_started',process_id={self._pid}"
        )

        """if not executing_test_case() and any(cmd in sys.argv for cmd in ['manage.py']):
            _logger.info(f"process {self._pid} startapp task returned")
            return

        # ex = expiration in seconds
        if not cache.get_or_set('on_app_start_executing', True, ex=120):
            _logger.info(f"process {self._pid} startapp tasks returned")
            return"""

        # actual startup tasks ->
        _logger.info('Metax API startup tasks executing...')
        cache = RedisClient()

        try:

            if settings.ELASTICSEARCH['ALWAYS_RELOAD_REFERENCE_DATA_ON_RESTART']:
                cache.set('reference_data', None)

            if not cache.get('reference_data', master=True) or not cache.get('ref_data_up_to_date', master=True):
                ReferenceDataLoader.populate_cache_reference_data(cache)
                _logger.info(f"event='reference_data_loaded',process_id={self._pid}")
            else:
                ic()
        except Exception as e:
            _logger.error(e)
            # raise e
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

        try:
            rabbitmq.init_exchanges()
        except Exception as e:
            _logger.error(e)
            _logger.error("Unable to initialize RabbitMQ exchanges")

        _logger.info('Metax API startup tasks finished')
