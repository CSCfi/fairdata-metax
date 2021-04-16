from environ import ImproperlyConfigured
import logging
from metax_api.settings import env

logger = logging.getLogger(__name__)

REDIS = {
    "HOST": env("REDIS_HOST"),
    "PORT": env("REDIS_PORT"),
    # https://github.com/andymccurdy/redis-py/issues/485#issuecomment-44555664
    "SOCKET_TIMEOUT": 0.1,
    # db index reserved for test suites
    "TEST_DB": env("REDIS_TEST_DB"),
    # enables extra logging to console during cache usage
    "DEBUG": False,
}

REDIS_USE_PASSWORD = env("REDIS_USE_PASSWORD")

if REDIS_USE_PASSWORD:
    try:
        REDIS["PASSWORD"] = env("REDIS_PASSWORD")
    except ImproperlyConfigured as e:
        logger.warning(e)

REDIS_USE_SENTINEL = False

if REDIS_USE_SENTINEL:
    try:
        REDIS["SENTINEL"] = {
            "HOSTS": [["127.0.0.1", 16379], ["127.0.0.1", 16380], ["127.0.0.1", 16381]],
            "SERVICE": env("REDIS_SENTINEL_SERVICE"),
        }
    except ImproperlyConfigured as e:
        logger.warning(e)
