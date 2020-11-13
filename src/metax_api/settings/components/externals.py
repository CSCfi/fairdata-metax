from icecream import ic

from metax_api.settings import env
from metax_api.settings.components.common import IDA_DATA_CATALOG_IDENTIFIER, ATT_DATA_CATALOG_IDENTIFIER

OAI = {
    'BASE_URL': env("OAJ_BASE_URL"),
    'BATCH_SIZE': 25,
    'REPOSITORY_NAME': 'Metax',
    'ETSIN_URL_TEMPLATE': 'http://etsin.something.fi/dataset/%s',
    'ADMIN_EMAIL': 'noreply@csc.fi',
    'SET_MAPPINGS': {
        'datasets': [
            IDA_DATA_CATALOG_IDENTIFIER,
            ATT_DATA_CATALOG_IDENTIFIER
        ],
        'ida_datasets': [
            IDA_DATA_CATALOG_IDENTIFIER
        ],
        'att_datasets': [
            ATT_DATA_CATALOG_IDENTIFIER
        ]
    }
}
DATACITE = {
    'USERNAME': env("DATACITE_USERNAME"),
    'PASSWORD': env("DATACITE_PASSWORD"),
    'ETSIN_URL_TEMPLATE': env("DATACITE_ETSIN_URL_TEMPLATE"),
    'PREFIX': env("DACITE_PREFIX"),
    'URL': env("DACITE_URL"),
}
REMS = {
    'ENABLED': env("REMS_ENABLED"),
    'API_KEY': env("REMS_API_KEY"),
    'BASE_URL': env("REMS_BASE_URL"),
    'ETSIN_URL_TEMPLATE': env("REMS_ETSIN_URL_TEMPLATE"),
    'METAX_USER': env("REMS_METAX_USER"),
    'REPORTER_USER': env("REMS_REPORTER_USER"),
    'AUTO_APPROVER': env("REMS_AUTO_APPROVER"),
    'FORM_ID': int(env("REMS_FORM_ID")),
}
ORG_FILE_PATH = env("ORG_FILE_PATH")
WKT_FILENAME = env("WKT_FILENAME")
LOCAL_REF_DATA_FOLDER = env("LOCAL_REF_DATA_FOLDER")
