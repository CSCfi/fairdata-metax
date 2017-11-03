"""
Django settings for metax_api project.

Generated by 'django-admin startproject' using Django 1.11.

For more information on this file, see
https://docs.djangoproject.com/en/1.11/topics/settings/

For the full list of settings and their values, see
https://docs.djangoproject.com/en/1.11/ref/settings/
"""

import logging.config
import os
import yaml
from metax_api.utils import executing_test_case, executing_travis

executing_in_travis = executing_travis()

if not executing_in_travis:
    with open('/home/metax-user/app_config') as app_config:
        app_config_dict = yaml.load(app_config)

# Build paths inside the project like this: os.path.join(BASE_DIR, ...)
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


# Quick-start development settings - unsuitable for production
# See https://docs.djangoproject.com/en/1.11/howto/deployment/checklist/

# SECURITY WARNING: keep the secret key used in production secret!
if executing_in_travis:
    SECRET_KEY = '^pqn=v2i)%!w1oh=r!m_=wo_#w3)(@-#8%q_8&9z@slu+#q3+b'
else:
    SECRET_KEY = app_config_dict['DJANGO_SECRET_KEY']

if executing_test_case() or executing_in_travis:
    # used by test cases and travis during test case execution to authenticate with certain api's
    API_TEST_USER = {
        'username': 'testuser',
        'password': 'testuserpassword'
    }

# Consider enabling these
#CSRF_COOKIE_SECURE = True
#SECURE_SSL_REDIRECT = True
#SESSION_COOKIE_SECURE = True

# Allow only specific hosts to access the app
ALLOWED_HOSTS = ['localhost', '127.0.0.1', '[::1]']
if not os.getenv('TRAVIS', None):
    SECURE_PROXY_SSL_HEADER = ('HTTP_X_FORWARDED_PROTO', 'https')
    USE_X_FORWARDED_HOST = True
    for allowed_host in app_config_dict['ALLOWED_HOSTS']:
        ALLOWED_HOSTS.append(allowed_host)

# SECURITY WARNING: don't run with debug turned on in production!
if executing_in_travis:
    DEBUG = True
else:
    DEBUG = app_config_dict['DEBUG']

# Application definition

AUTH_USER_MODEL = 'metax_api.MetaxUser'

INSTALLED_APPS = [
    'django.contrib.auth',
    'django.contrib.contenttypes',
    'django.contrib.sessions',
    'rest_framework',
    'metax_api',
]

if DEBUG:
    INSTALLED_APPS.append('django.contrib.staticfiles')

MIDDLEWARE = [
    # note: not strictly necessary if running in a private network
    # https://docs.djangoproject.com/en/1.11/ref/middleware/#module-django.middleware.security
    'django.middleware.security.SecurityMiddleware',
    'django.contrib.sessions.middleware.SessionMiddleware',
    'django.middleware.common.CommonMiddleware',
    'django.contrib.auth.middleware.AuthenticationMiddleware',
    'metax_api.middleware.IdentifyApiCaller',
]

REST_FRAMEWORK = {
    # Use Django's standard `django.contrib.auth` permissions,
    # or allow read-only access for unauthenticated users.
    'DEFAULT_PERMISSION_CLASSES': [
        'rest_framework.permissions.DjangoModelPermissionsOrAnonReadOnly'
    ],
    'DEFAULT_PAGINATION_CLASS': 'rest_framework.pagination.LimitOffsetPagination',
    'PAGE_SIZE': 10
}

if not DEBUG:
    REST_FRAMEWORK['DEFAULT_RENDERER_CLASSES'] = ['rest_framework.renderers.JSONRenderer']

REST_FRAMEWORK['DEFAULT_PARSER_CLASSES'] = [
    'rest_framework.parsers.JSONParser',
    'metax_api.parsers.XMLParser',
]

REST_FRAMEWORK['DEFAULT_RENDERER_CLASSES'] = [
    'rest_framework.renderers.JSONRenderer',
    'rest_framework.renderers.BrowsableAPIRenderer',
    'metax_api.renderers.XMLRenderer',
]


ROOT_URLCONF = 'metax_api.urls'

APPEND_SLASH = False

if DEBUG:
    TEMPLATES = [
        {
            'BACKEND': 'django.template.backends.django.DjangoTemplates',
            'DIRS': [],
            'APP_DIRS': True,
            'OPTIONS': {
                'context_processors': [
                    'django.template.context_processors.debug',
                    'django.template.context_processors.request',
                    'django.contrib.auth.context_processors.auth',
                    'django.contrib.messages.context_processors.messages',
                ],
            },
        },
    ]

WSGI_APPLICATION = 'metax_api.wsgi.application'

# Database
# https://docs.djangoproject.com/en/1.11/ref/settings/#databases

"""
The following uses the 'TRAVIS' (== True) environment variable on Travis
to detect the session, and changes the default database accordingly.
"""
if executing_in_travis:
    DATABASES = {
        'default': {
            'ENGINE':   'django.db.backends.postgresql_psycopg2',
            'NAME':     'metax_db_test',
            'USER':     'metax_test',
            'PASSWORD': '',
            'HOST':     'localhost'
        }
    }
else:
    DATABASES = {
        'default': {
            'ENGINE': 'django.db.backends.postgresql_psycopg2',
            'NAME': app_config_dict['METAX_DATABASE'],
            'USER': app_config_dict['METAX_DATABASE_USER'],
            'PASSWORD': app_config_dict['METAX_DATABASE_PASSWORD'],
            'HOST': app_config_dict['METAX_DATABASE_HOST'],
            'PORT': '',
            'ATOMIC_REQUESTS': True
        }
    }

"""
Colorize automated test console output
"""
RAINBOWTESTS_HIGHLIGHT_PATH = BASE_DIR
TEST_RUNNER = 'rainbowtests.test.runner.RainbowDiscoverRunner'

"""
Logging rules:
- Django DEBUG enabled: Print everything from logging level DEBUG and up, to
both console, and log file.
- Django DEBUG disabled: Print everything from logging level INFO and up, only
to log file.
"""
LOGGING = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'standard': {
            'format': '%(asctime)s %(name)s %(levelname)s: %(message)s'
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
            'filters': ['require_debug_true'],
            'class': 'logging.StreamHandler',
            'formatter': 'standard',
        },
        'debug': {
            'level': 'DEBUG',
            'class': 'logging.FileHandler',
            'filename': '/var/log/metax-api/metax_api.log',
            'formatter': 'standard',
            'filters': ['require_debug_true'],
        },
        'general': {
            'level': 'INFO',
            'class': 'logging.FileHandler',
            'filename': '/var/log/metax-api/metax_api.log',
            'formatter': 'standard',
            'filters': ['require_debug_false'],
        }
    },
    'loggers': {
        'django': {
            'handlers': ['general', 'console', 'debug'],
        },
        'metax_api': {
            'handlers': ['general', 'console', 'debug'],
        }
    }
}

logger = logging.getLogger('metax_api')
logger.setLevel(logging.DEBUG if DEBUG else logging.INFO)

# Internationalization
# https://docs.djangoproject.com/en/1.11/topics/i18n/

LANGUAGE_CODE = 'en-US'

TIME_ZONE = 'Europe/Helsinki'

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
USE_TZ = False

DATETIME_INPUT_FORMATS = ['%Y-%m-%dT%H:%M:%S.%fZ']

# Static files (CSS, JavaScript, Images)
# https://docs.djangoproject.com/en/1.11/howto/static-files/

PROJECT_DIR = os.path.dirname(os.path.abspath(__file__))
# same dir as manage.py
STATIC_ROOT = os.path.join(os.path.dirname(PROJECT_DIR), 'static')
STATIC_URL = '/static/'

if not executing_in_travis:
    # settings for custom redis-py cache helper in utils/redis.py
    REDIS_SENTINEL = {
        # at least three are required
        'HOSTS':    app_config_dict['REDIS']['HOSTS'],
        'PASSWORD': app_config_dict['REDIS']['PASSWORD'],
        'SERVICE':  app_config_dict['REDIS']['SERVICE'],
        'LOCALHOST_PORT': app_config_dict['REDIS']['LOCALHOST_PORT'],

        # https://github.com/andymccurdy/redis-py/issues/485#issuecomment-44555664
        'SOCKET_TIMEOUT': 0.1,

        # db index reserved for test suites
        'TEST_DB': app_config_dict['REDIS']['TEST_DB'],

        # enables extra logging to console during cache usage
        'DEBUG': False,
    }

if executing_in_travis:
    ELASTICSEARCH = {
        'HOSTS': ['metax-test.csc.fi/es'],
        'USE_SSL': True,
        'ALWAYS_RELOAD_REFERENCE_DATA_ON_RESTART': True,
    }
else:
    ELASTICSEARCH = {
        'HOSTS': app_config_dict['ELASTICSEARCH']['HOSTS'],
        # normally cache is reloaded from elasticsearch only if reference data is missing.
        # for one-off reload / debugging / development, use below flag
        'ALWAYS_RELOAD_REFERENCE_DATA_ON_RESTART': app_config_dict['ALWAYS_RELOAD_REFERENCE_DATA_ON_RESTART'],
    }

if not executing_in_travis:
    RABBITMQ = {
        'HOSTS':    app_config_dict['RABBITMQ']['HOSTS'],
        'PORT':     app_config_dict['RABBITMQ']['PORT'],
        'USER':     app_config_dict['RABBITMQ']['USER'],
        'VHOST':    app_config_dict['RABBITMQ']['VHOST'],
        'PASSWORD': app_config_dict['RABBITMQ']['PASSWORD'],
        'EXCHANGES': [
            {
                'NAME': 'datasets',
                'TYPE': 'direct',
                # make rabbitmq remember queues after restarts
                'DURABLE': True
            }
        ]
    }
