from metax_api.settings.components.common import INSTALLED_APPS, ALLOWED_HOSTS
from watchman import constants as watchman_constants
INSTALLED_APPS += ['watchman']

ALLOWED_HOSTS += ["*"]

WATCHMAN_CHECKS = watchman_constants.DEFAULT_CHECKS + (
    'metax_api.checks.elasticsearch_check',
    'metax_api.checks.redis_check',
)
