
from metax_api.settings.components.common import DEBUG
import logging.config
import time
import structlog
from metax_api.settings import env


# Logging rules:
# - Django DEBUG enabled: Print everything from logging level DEBUG and up, to both console, and log file.
# - Django DEBUG disabled: Print everything from logging level INFO and up, only to log file.


LOGGING = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'standard': {
            # timestamp, process id, python module name, loglevel, msg content...
            'format': '%(asctime)s p%(process)d %(name)s %(levelname)s: %(message)s',
            'datefmt': '%Y-%m-%dT%H:%M:%S.%03dZ',
        },
    },
    'filters': {
        'require_debug_true': {
            '()': 'django.utils.log.RequireDebugTrue',
        },
        'require_debug_false': {
            '()': 'django.utils.log.RequireDebugFalse',
        },
    },
    'handlers': {
        'console': {
            'level': 'DEBUG',
            'class': 'logging.StreamHandler',
            'formatter': 'standard',
        },
        'debug': {
            'level': 'DEBUG',
            'class': 'logging.FileHandler',
            'filename': env("LOGGING_DEBUG_HANDLER_FILE"),
            'formatter': 'standard',
            'filters': ['require_debug_true'],
        },
        'general': {
            'level': 'INFO',
            'class': 'logging.FileHandler',
            'filename': env("LOGGING_DEBUG_HANDLER_FILE"),
            'formatter': 'standard',
            'filters': ['require_debug_false'],
        },
    },
    'loggers': {
        'django': {
            'handlers': ['general', 'console', 'debug'],
        },
        'metax_api': {
            'handlers': ['general', 'console', 'debug'],
        },
        "root": {"level": "INFO", "handlers": ["console"]},
    }
}

logging.Formatter.converter = time.gmtime
logger = logging.getLogger('metax_api')
logger.setLevel(logging.DEBUG if DEBUG else logging.INFO)

structlog.configure(
    processors=[
        structlog.stdlib.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.JSONRenderer()
    ],
    context_class=dict,
    logger_factory=structlog.stdlib.LoggerFactory(),
    wrapper_class=structlog.stdlib.BoundLogger,
    cache_logger_on_first_use=True,
)

handler = logging.FileHandler(env("LOGGING_JSON_FILE_HANDLER_FILE"))
handler.setFormatter(logging.Formatter('%(message)s'))
json_logger = logging.getLogger('structlog')
json_logger.addHandler(handler)
json_logger.setLevel(logging.INFO)