import os

from metax_api.settings import env
from metax_api.settings.components import BASE_DIR

DEBUG = env("DEBUG")
SECRET_KEY = env("DJANGO_SECRET_KEY")
ADDITIONAL_USER_PROJECTS_PATH = env("ADDITIONAL_USER_PROJECTS_PATH")
IDA_DATA_CATALOG_IDENTIFIER = "urn:nbn:fi:att:data-catalog-ida"
ATT_DATA_CATALOG_IDENTIFIER = "urn:nbn:fi:att:data-catalog-att"
PAS_DATA_CATALOG_IDENTIFIER = "urn:nbn:fi:att:data-catalog-pas"
LEGACY_DATA_CATALOG_IDENTIFIER = "urn:nbn:fi:att:data-catalog-legacy"
DFT_DATA_CATALOG_IDENTIFIER = "urn:nbn:fi:att:data-catalog-dft"

END_USER_ALLOWED_DATA_CATALOGS = [
    IDA_DATA_CATALOG_IDENTIFIER,
    ATT_DATA_CATALOG_IDENTIFIER,
    LEGACY_DATA_CATALOG_IDENTIFIER,
    DFT_DATA_CATALOG_IDENTIFIER,
    PAS_DATA_CATALOG_IDENTIFIER
]

# catalogs where uniqueness of dataset pids is not enforced.
LEGACY_CATALOGS = [
    LEGACY_DATA_CATALOG_IDENTIFIER,
]
VALIDATE_TOKEN_URL = env("VALIDATE_TOKEN_URL")
CHECKSUM_ALGORITHMS = ["SHA-256", "MD5", "SHA-512"]
ERROR_FILES_PATH = env("ERROR_FILES_PATH")

# Allow only specific hosts to access the app
ALLOWED_HOSTS = ["localhost", "127.0.0.1", "[::1]"]

SECURE_PROXY_SSL_HEADER = ("HTTP_X_FORWARDED_PROTO", "https")
USE_X_FORWARDED_HOST = True
for allowed_host in env("ALLOWED_HOSTS"):
    ALLOWED_HOSTS.append(allowed_host)

SERVER_DOMAIN_NAME = env("SERVER_DOMAIN_NAME")
AUTH_SERVER_LOGOUT_URL = env("AUTH_SERVER_LOGOUT_URL")

# when using the requests-library or similar, should be used to decide when to verify self-signed certs
TLS_VERIFY = False if DEBUG else True

AUTH_USER_MODEL = "metax_api.MetaxUser"
ALLOWED_AUTH_METHODS = ["Basic", "Bearer"]

INSTALLED_APPS = [
    "django.contrib.auth",
    "django.contrib.contenttypes",
    "django.contrib.sessions",
    "rest_framework",
    "metax_api",
]

if DEBUG:
    INSTALLED_APPS.append("django.contrib.staticfiles")

MIDDLEWARE = [
    "metax_api.middleware.RequestLogging",
    # note: not strictly necessary if running in a private network
    # https://docs.djangoproject.com/en/1.11/ref/middleware/#module-django.middleware.security
    "django.middleware.security.SecurityMiddleware",
    "django.contrib.sessions.middleware.SessionMiddleware",
    "django.middleware.common.CommonMiddleware",
    "django.contrib.auth.middleware.AuthenticationMiddleware",
    "django.middleware.clickjacking.XFrameOptionsMiddleware",
    "django.middleware.csrf.CsrfViewMiddleware",
    "metax_api.middleware.IdentifyApiCaller",
    "metax_api.middleware.AddLastModifiedHeaderToResponse",
    "metax_api.middleware.StreamHttpResponse",
]

# security settings
CSRF_COOKIE_SECURE = True
# SECURE_BROWSER_XSS_FILTER = True   # is set in nginx
# SECURE_CONTENT_TYPE_NOSNIFF = True # is set in nginx
# SECURE_SSL_REDIRECT = True         # is set in nginx
# SESSION_COOKIE_SECURE = True       # is set in nginx
# X_FRAME_OPTIONS = 'DENY'           # is set in nginx

REST_FRAMEWORK = {
    # Use Django's standard `django.contrib.auth` permissions,
    # or allow read-only access for unauthenticated users.
    "DEFAULT_PERMISSION_CLASSES": [
        "rest_framework.permissions.DjangoModelPermissionsOrAnonReadOnly"
    ],
    "DEFAULT_FILTER_BACKENDS": ["rest_framework.filters.OrderingFilter"],
    "DEFAULT_PAGINATION_CLASS": "rest_framework.pagination.LimitOffsetPagination",
    "PAGE_SIZE": 10,
}
REST_FRAMEWORK["DEFAULT_PARSER_CLASSES"] = [
    "rest_framework.parsers.JSONParser",
    "metax_api.parsers.XMLParser",
]
REST_FRAMEWORK["DEFAULT_RENDERER_CLASSES"] = [
    "rest_framework.renderers.JSONRenderer",
    "metax_api.renderers.HTMLToJSONRenderer",
    "metax_api.renderers.XMLRenderer",
]
ROOT_URLCONF = "metax_api.urls"

APPEND_SLASH = False

TEMPLATES = [
    {
        "BACKEND": "django.template.backends.django.DjangoTemplates",
        "DIRS": [],
        "APP_DIRS": True,
        "OPTIONS": {
            "context_processors": [
                "django.template.context_processors.debug",
                "django.template.context_processors.request",
                "django.contrib.auth.context_processors.auth",
                "django.contrib.messages.context_processors.messages",
            ],
        },
    },
]
WSGI_APPLICATION = "metax_api.wsgi.application"

DATABASES = {
    "default": {
        "NAME": env("METAX_DATABASE"),
        "USER": env("METAX_DATABASE_USER"),
        "PASSWORD": env("METAX_DATABASE_PASSWORD"),
        "HOST": env("METAX_DATABASE_HOST"),
        "PORT": env("METAX_DATABASE_PORT"),
        # default is 0 == "reconnect to db on every request". placing setting here for visibility
        "CONN_MAX_AGE": 0,
    }
}

DATABASES["default"]["ENGINE"] = "django.db.backends.postgresql"
DATABASES["default"]["ATOMIC_REQUESTS"] = True

# Colorize automated test console output
RAINBOWTESTS_HIGHLIGHT_PATH = str(BASE_DIR)
TEST_RUNNER = "rainbowtests.test.runner.RainbowDiscoverRunner"

# Internationalization
# https://docs.djangoproject.com/en/1.11/topics/i18n/

LANGUAGE_CODE = "en-US"

TIME_ZONE = "Europe/Helsinki"

# A boolean that specifies whether Django’s translation system should
# be enabled. This provides an easy way to turn it off, for performance.
# If this is set to False, Django will make some optimizations so as not
# to load the translation machinery.
USE_I18N = True

# A boolean that specifies if localized formatting of data will
# be enabled by default or not. If this is set to True,
# e.g. Django will display numbers and dates using the format
# of the current locale.
USE_L10N = False

# A boolean that specifies if datetimes will be timezone-aware by default
# or not. If this is set to True, Django will use timezone-aware datetimes
# internally. Otherwise, Django will use naive datetimes in local time.
USE_TZ = True

DATETIME_INPUT_FORMATS = ["%Y-%m-%dT%H:%M:%S.%fZ"]

# Static files (CSS, JavaScript, Images)
# https://docs.djangoproject.com/en/1.11/howto/static-files/

PROJECT_DIR = os.path.dirname(os.path.abspath(__file__))
# same dir as manage.py
STATIC_ROOT = os.path.join(os.path.dirname(PROJECT_DIR), "static")
STATIC_URL = "/static/"

API_VERSIONS_ENABLED = []
if env("ENABLE_V1_ENDPOINTS"):
    API_VERSIONS_ENABLED.append("v1")
if env("ENABLE_V2_ENDPOINTS"):
    API_VERSIONS_ENABLED.append("v2")

# Variables related to api credentials
API_USERS = [
    {"password": "test-metax", "username": "metax"},
    {"password": "test-qvain", "username": "qvain"},
    {"password": "test-ida", "username": "ida"},
    {"password": "test-tpas", "username": "tpas"},
    {"password": "test-etsin", "username": "etsin"},
    {"password": "test-fds", "username": "fds"},
    {"password": "test-download", "username": "download"},
]
