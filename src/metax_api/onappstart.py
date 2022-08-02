# This file is part of the Metax API service
#
# Copyright 2017-2018 Ministry of Education and Culture, Finland
#
# :author: CSC - IT Center for Science Ltd., Espoo Finland <servicedesk@csc.fi>
# :license: MIT

import logging
from os import getpid, makedirs
from shutil import rmtree

from django.apps import AppConfig
from django.conf import settings

from metax_api.utils import ReferenceDataLoader, executing_test_case, convert_yaml_to_html


_logger = logging.getLogger(__name__)


class OnAppStart(AppConfig):

    name = "metax_api"
    verbose_name = "Metax API"
    _pid = getpid()

    def ready(self):  # pragma: no cover
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
        import json

        if settings.ENABLE_SIGNALS:
            import metax_api.signals # noqa
        from metax_api.services import RabbitMQService as rabbitmq
        from metax_api.services.redis_cache_service import RedisClient

        if settings.WATCHMAN_CONFIGURED:
            from watchman.utils import get_checks

            for check in get_checks():
                if callable(check):
                    try:
                        resp = json.dumps(check())
                        _logger.info(resp)
                    except TypeError as e:
                        e_resp = check()
                        _logger.error(
                            f"Error in system check: {e}, caused by check:{check.__name__} with return value of {e_resp}"
                        )
        _logger.info(f"event='process_started',process_id={self._pid}")

        # actual startup tasks ->
        _logger.info("Metax API startup tasks executing...")
        cache = RedisClient()

        try:

            if settings.ALWAYS_RELOAD_REFERENCE_DATA_ON_RESTART:
                cache.set("reference_data", None)

            if not cache.get("reference_data", master=True) or not cache.get(
                "ref_data_up_to_date", master=True
            ):
                ReferenceDataLoader.populate_cache_reference_data(cache)
                _logger.info(f"event='reference_data_loaded',process_id={self._pid}")

        except Exception as e:
            _logger.error(e)

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

        try:
            convert_yaml_to_html.yaml_to_html_convert()
        except Exception as e:
            _logger.error(e)
            _logger.error("Unable to convert swagger documentation")

        _logger.info("Metax API startup tasks finished")
