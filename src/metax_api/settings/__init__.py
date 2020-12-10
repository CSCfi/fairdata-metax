"""
This is a django-split-settings main file.
For more information read this:
https://github.com/sobolevn/django-split-settings
To change settings file:
`DJANGO_ENV=production python manage.py runserver`
"""
from os.path import join

import environ
from icecream import ic
from split_settings.tools import include

from metax_api.settings.components import BASE_DIR  # src

# Managing environment via DJANGO_ENV variable:
REFDATA_INDEXER_PATH = join(
    BASE_DIR, "metax_api", "tasks", "refdata", "refdata_indexer"
)
env = environ.Env(
    # set casting, default value
    ADDITIONAL_USER_PROJECTS_PATH=(str, ""),
    ALLOWED_HOSTS=(list, ["metax.csc.local", "20.20.20.20"]),
    DEBUG=(bool, False),
    DJANGO_ENV=(str, "local"),
    ELASTIC_SEARCH_PORT=(int, 9200),
    ELASTIC_SEARCH_USE_SSL=(bool, False),
    ERROR_FILES_PATH=(str, join(BASE_DIR, "log", "errors")),
    ES_CONFIG_DIR=(str, join(REFDATA_INDEXER_PATH, "resources", "es-config/")),
    LOCAL_REF_DATA_FOLDER=(str,join(REFDATA_INDEXER_PATH, "resources", "local-refdata/"),),
    LOGGING_DEBUG_HANDLER_FILE=(str, join(BASE_DIR, "log", "metax_api.log")),
    LOGGING_GENERAL_HANDLER_FILE=(str, join(BASE_DIR, "log", "metax_api.log")),
    LOGGING_JSON_FILE_HANDLER_FILE=(str, join(BASE_DIR, "log", "metax_api.json.log")),
    METAX_DATABASE_HOST=(str, "localhost"),
    METAX_DATABASE_PORT=(str, 5432),
    METAX_ENV=(str, "local_development"),
    METAX_API_ROOT=(str, "https://localhost:8008"),
    ORG_FILE_PATH=(str, join(REFDATA_INDEXER_PATH, "resources", "organizations", "organizations.csv"),),
    RABBIT_MQ_PORT=(int, 5672),
    REDIS_HOST=(str, "localhost"),
    RABBIT_MQ_PASSWORD=(str, "guest"),
    RABBIT_MQ_USER=(str, "guest"),
    REDIS_PORT=(int, 6379),
    REDIS_TEST_DB=(int, 15),
    REDIS_USE_PASSWORD=(bool, False),
    SERVER_DOMAIN_NAME=(str, "metax.csc.local"),
    TRAVIS=(bool, False),
    VALIDATE_TOKEN_URL=(str, "https://127.0.0.1/secure/validate_token"),
    WKT_FILENAME=(str, join(REFDATA_INDEXER_PATH, "resources", "uri_to_wkt.json")),
)
# reading .env file
environ.Env.read_env()

ENV = env("DJANGO_ENV")

base_settings = [
    "components/common.py",
    "components/logging.py",
    "components/redis.py",
    "components/access_control.py",
    "components/elasticsearch.py",
    "components/rabbitmq.py",
    "components/externals.py",
    "components/rems.py",
    "environments/{0}.py".format(ENV),
    # Optionally override some settings:
    # optional('environments/legacy.py'),
]
ic(ENV)

# Include settings:
include(*base_settings)
