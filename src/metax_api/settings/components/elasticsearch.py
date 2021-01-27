from metax_api.settings.components.common import env

# INSTALLED_APPS += ["django_elasticsearch_dsl"]
ES_HOSTS = [env("ELASTIC_SEARCH_HOSTS")]

ELASTICSEARCH = {
    "HOSTS": ES_HOSTS,
    "USE_SSL": env("ELASTIC_SEARCH_USE_SSL"),
    "ALWAYS_RELOAD_REFERENCE_DATA_ON_RESTART": False,
}
ELASTICSEARCH["REFERENCE_DATA_RELOAD_INTERVAL"] = 86400

ES_CONFIG_DIR = env("ES_CONFIG_DIR")
