from metax_api.settings import env
from metax_api.settings.components.access_control import Role, api_permissions, prepare_perm_values
from metax_api.settings.components.common import ALLOWED_HOSTS, DEBUG, INSTALLED_APPS, MIDDLEWARE


ALLOWED_HOSTS += ["*"]

if "debug_toolbar" not in INSTALLED_APPS and env("DEBUG_TOOLBAR_ENABLED"):
    INSTALLED_APPS += ["debug_toolbar"]
if "debug_toolbar.middleware.DebugToolbarMiddleware" not in MIDDLEWARE and env("DEBUG_TOOLBAR_ENABLED"):
    MIDDLEWARE = ["debug_toolbar.middleware.DebugToolbarMiddleware"] + MIDDLEWARE

INTERNAL_IPS = ["127.0.0.1", "0.0.0.0"]


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
