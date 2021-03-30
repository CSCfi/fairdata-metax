from watchman import constants as watchman_constants

from metax_api.settings.components.common import INSTALLED_APPS, ALLOWED_HOSTS, MIDDLEWARE, DEBUG

INSTALLED_APPS += ["watchman"]

ALLOWED_HOSTS += ["*"]

WATCHMAN_CHECKS = watchman_constants.DEFAULT_CHECKS + (
    "metax_api.checks.elasticsearch_check",
    "metax_api.checks.redis_check",
)

# only used in manual testing script in tests/rabbitmq/consume.py
CONSUMERS = [
    {
        "is_test_user": True,
        "name": "testaaja",
        "password": "testaaja",
        "permissions": {
            "conf": "^testaaja-.*$",
            "read": "^(datasets|testaaja-.*)$",
            "write": "^testaaja-.*$",
        },
        "vhost": "metax",
    },
    {
        "is_test_user": False,
        "name": "etsin",
        "password": "test-etsin",
        "permissions": {
            "conf": "^etsin-.*$",
            "read": "^(datasets|etsin-.*)$",
            "write": "^etsin-.*$",
        },
        "vhost": "metax",
    },
    {
        "is_test_user": False,
        "name": "ttv",
        "password": "test-ttv",
        "permissions": {
            "conf": "^ttv-.*$",
            "read": "^(TTV-datasets|ttv-.*)$",
            "write": "^ttv-.*$",
        },
        "vhost": "metax",
    },
]
if 'debug_toolbar' not in INSTALLED_APPS:
    INSTALLED_APPS += ['debug_toolbar']
if 'debug_toolbar.middleware.DebugToolbarMiddleware' not in MIDDLEWARE:
    MIDDLEWARE = ['debug_toolbar.middleware.DebugToolbarMiddleware'] + MIDDLEWARE
INTERNAL_IPS = [
    '127.0.0.1',
    '0.0.0.0'
]
def show_toolbar(request):
    if DEBUG:
        return True
    else:
        return False
DEBUG_TOOLBAR_CONFIG = {
    "SHOW_TOOLBAR_CALLBACK" : show_toolbar,
}

