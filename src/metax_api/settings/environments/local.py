from watchman import constants as watchman_constants

from metax_api.settings.components.access_control import Role, api_permissions, prepare_perm_values
from metax_api.settings.components.common import ALLOWED_HOSTS, DEBUG, INSTALLED_APPS, MIDDLEWARE

INSTALLED_APPS += ["watchman"]

ALLOWED_HOSTS += ["*"]

WATCHMAN_CHECKS = watchman_constants.DEFAULT_CHECKS + (
    "metax_api.checks.elasticsearch_check",
    "metax_api.checks.redis_check",
)

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
    "SHOW_TOOLBAR_CALLBACK": show_toolbar,
}
api_permissions.rest.apierrors.create += [Role.METAX]

API_ACCESS = prepare_perm_values(api_permissions.to_dict())
