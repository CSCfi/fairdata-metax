import datetime
import logging.config
import time

import structlog

from metax_api.settings import env
from metax_api.settings.components.common import DEBUG

# Logging rules:
# - Django DEBUG enabled: Print everything from logging level DEBUG and up, to both console, and log file.
# - Django DEBUG disabled: Print everything from logging level INFO and up, only to log file.

LOGGING_PATH = env("LOGGING_PATH")
LOGGING_DEBUG_HANDLER_FILE = f"{LOGGING_PATH}/metax_api.log"
LOGGING_JSON_FILE_HANDLER_FILE = f"{LOGGING_PATH}/metax_api.json.log"
LOGGING_GENERAL_HANDLER_FILE = f"{LOGGING_PATH}/metax_api.log"
LOGGING_LEVEL = env("LOGGING_LEVEL")


class DateTimeZFormatter(logging.Formatter):
    """Log formatter with datetime with milliseconds and Z timezone."""

    default_time_format = "%Y-%m-%d %H:%M:%S"
    default_msec_format = "%s.%03dZ"


LOGGING = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "standard": {
            # timestamp, process id, python module name, loglevel, msg content...
            "class": "metax_api.settings.components.logging.DateTimeZFormatter",
            "format": "%(asctime)s p%(process)d %(name)s %(levelname)s: %(message)s",
        },
    },
    "filters": {
        "require_debug_true": {
            "()": "django.utils.log.RequireDebugTrue",
        },
        "require_debug_false": {
            "()": "django.utils.log.RequireDebugFalse",
        },
    },
    "handlers": {
        "console": {
            "level": LOGGING_LEVEL,
            "class": "logging.StreamHandler",
            "formatter": "standard",
            "filters": ["require_debug_true"],
        },
        "debug": {
            "level": LOGGING_LEVEL,
            "class": "logging.FileHandler",
            "filename": LOGGING_DEBUG_HANDLER_FILE,
            "formatter": "standard",
            "filters": ["require_debug_true"],
        },
        "general": {
            "level": LOGGING_LEVEL,
            "class": "logging.FileHandler",
            "filename": LOGGING_DEBUG_HANDLER_FILE,
            "formatter": "standard",
            "filters": ["require_debug_false"],
        },
    },
    "loggers": {
        "django": {
            "handlers": ["general", "console", "debug"],
        },
        "metax_api": {
            "handlers": ["general", "console", "debug"],
        },
        "root": {"level": LOGGING_LEVEL, "handlers": ["console", "general"]},
    },
}

logging.Formatter.converter = time.gmtime
logger = logging.getLogger("metax_api")
logger.setLevel(LOGGING_LEVEL)

structlog.configure(
    processors=[
        structlog.stdlib.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.JSONRenderer(),
    ],
    context_class=dict,
    logger_factory=structlog.stdlib.LoggerFactory(),
    wrapper_class=structlog.stdlib.BoundLogger,
    cache_logger_on_first_use=True,
)

handler = logging.FileHandler(LOGGING_JSON_FILE_HANDLER_FILE)
handler.setFormatter(logging.Formatter("%(message)s"))
json_logger = logging.getLogger("structlog")
json_logger.addHandler(handler)
json_logger.setLevel(LOGGING_LEVEL)
