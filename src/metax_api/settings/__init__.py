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
REFDATA_INDEXER_PATH = join(BASE_DIR, "metax_api", "tasks", "refdata", "refdata_indexer")
env = environ.Env(
    # set casting, default value
    LOGGING_LEVEL=(str, "INFO"),
    ADDITIONAL_USER_PROJECTS_PATH=(str, "/tmp/metax"),
    ALLOWED_HOSTS=(list, []),
    ALWAYS_RELOAD_REFERENCE_DATA_ON_RESTART=(bool, True),
    API_USERS_PATH=(str, "/etc/fairdata-metax/api_users"),
    CACHE_ROOT=(str, join(BASE_DIR.parent, "cache")),
    DEBUG=(bool, False),
    DEBUG_TOOLBAR_ENABLED=(bool, False),
    DJANGO_ENV=(str, "local"),
    ELASTIC_SEARCH_HOSTS=(list, ["localhost"]),
    ELASTIC_SEARCH_PORT=(int, 9200),
    ELASTIC_SEARCH_USE_SSL=(bool, False),
    ENABLE_API_ERROR_OBJECTS=(bool, False),
    ENABLE_DELETED_OBJECTS_SAVING=(bool, False),
    ENABLE_SIGNALS=(bool, False),
    ENABLE_V1_ENDPOINTS=(bool, True),
    ENABLE_V2_ENDPOINTS=(bool, True),
    ENABLE_DJANGO_WATCHMAN=(bool, True),
    ERROR_FILES_PATH=(str, join("/var", "log", "metax-api", "errors")),
    ES_CONFIG_DIR=(str, join(REFDATA_INDEXER_PATH, "resources", "es-config/")),
    METRICS_API_ADDRESS=(str, None),
    METRICS_API_TOKEN=(str, None),
    LOCAL_REF_DATA_FOLDER=(
        str,
        join(REFDATA_INDEXER_PATH, "resources", "local-refdata/"),
    ),
    LOGGING_PATH=(str, join("/var", "log", "metax-api")),
    METAX_DATABASE_HOST=(str, "localhost"),
    METAX_DATABASE_PORT=(str, 5432),
    METAX_V3_HOST=(str, "http://metax-v3:8002"),
    METAX_V3_INTEGRATION_ENABLED=(bool, False),
    METAX_V3_TOKEN=(str, "token"),
    METAX_V3_PROTOCOL=(str, "https"),
    ORG_FILE_PATH=(
        str,
        join(REFDATA_INDEXER_PATH, "resources", "organizations", "organizations.csv"),
    ),
    OAI_BASE_URL=(str, "https://metax.fd-dev.csc.fi/oai/"),
    OAI_BATCH_SIZE=(int, 25),
    OAI_REPOSITORY_NAME=(str, "Metax"),
    PID_MS_CATALOGS_TO_MIGRATE=(
        list,
        [
            "urn:nbn:fi:att:data-catalog-ida",
            "urn:nbn:fi:att:data-catalog-att",
            "urn:nbn:fi:att:data-catalog-pas",
        ],
    ),
    PID_MS_HOST=(str, "pidms-test.2.rahtiapp.fi"),
    PID_MS_TOKEN=(str, "token"),
    PID_MS_PROTOCOL=(str, "https"),
    RABBIT_MQ_HOSTS=(list, ["localhost"]),
    RABBIT_MQ_PORT=(int, 5672),
    RABBIT_MQ_PASSWORD=(str, "guest"),
    RABBIT_MQ_USER=(str, "guest"),
    RABBIT_MQ_USE_VHOST=(bool, False),
    REDIS_HOST=(str, "localhost"),
    REDIS_PORT=(int, 6379),
    REDIS_TEST_DB=(int, 15),
    REDIS_USE_PASSWORD=(bool, False),
    REMS_ENABLED=(bool, False),
    SERVER_DOMAIN_NAME=(str, "metax.fd-dev.csc.fi"),
    STATIC_ROOT=(str, join(BASE_DIR.parent, "static")),
    VALIDATE_TOKEN_URL=(str, "https://127.0.0.1/secure/validate_token"),
    WKT_FILENAME=(str, join(REFDATA_INDEXER_PATH, "resources", "uri_to_wkt.json")),
    SWAGGER_YAML_PATH=(str, join(BASE_DIR, "metax_api", "swagger")),
    SWAGGER_HTML_PATH=(str, join(BASE_DIR, "metax_api", "templates", "swagger")),
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
    "components/monitoring.py",
    "components/rems.py",
    "components/metrics.py",
    "components/metax_v3.py",
    "components/pid_ms.py",
    "environments/{0}.py".format(ENV),
    # Optionally override some settings:
    # optional('environments/legacy.py'),
]
ic(ENV)
# Include settings:
include(*base_settings)
