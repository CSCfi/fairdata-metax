import logging

from metax_api.settings import env
from metax_api.settings.components.common import INSTALLED_APPS

logger = logging.getLogger(__name__)

ENABLE_DJANGO_WATCHMAN = env("ENABLE_DJANGO_WATCHMAN")
WATCHMAN_CONFIGURED = False

if ENABLE_DJANGO_WATCHMAN:
    try:
        from watchman import constants as watchman_constants

        if "watchman" not in INSTALLED_APPS:
            INSTALLED_APPS += ["watchman"]

        WATCHMAN_CHECKS = watchman_constants.DEFAULT_CHECKS + (
            "metax_api.checks.elasticsearch_check",
            "metax_api.checks.redis_check",
            "metax_api.checks.finto_check",
            "metax_api.checks.v3_sync_check",
        )
        WATCHMAN_CONFIGURED = True

    except ImportError as e:
        logger.error(e)
