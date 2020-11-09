from metax_api.settings.components.common import INSTALLED_APPS

# INSTALLED_APPS += ["django_elasticsearch_dsl"]

ELASTICSEARCH = {
    'HOSTS': ['localhost:9200'],
    'USE_SSL': False,
    'ALWAYS_RELOAD_REFERENCE_DATA_ON_RESTART': False,
}
ELASTICSEARCH['REFERENCE_DATA_RELOAD_INTERVAL'] = 86400