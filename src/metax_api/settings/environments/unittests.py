from metax_api.settings.components.access_control import Role, api_permissions, prepare_perm_values

API_TEST_USER = {"username": "testuser", "password": "testuserpassword"}
API_METAX_USER = {"username": "metax", "password": "metaxpassword"}
API_METAX_SERVICE_USER = {"username": "metax_service", "password": "metaxservicepassword"}
API_AUTH_TEST_USER = {"username": "api_auth_user", "password": "password"}

API_EXT_USER = {"username": "external", "password": "externalpassword"}

API_TEST_USERS = [API_TEST_USER, API_METAX_USER, API_AUTH_TEST_USER, API_EXT_USER, API_METAX_SERVICE_USER]

ADDITIONAL_USER_PROJECTS_PATH = "/tmp/user_projects.json"

# represents an organizational (such as jyu) catalog in test cases
EXT_DATA_CATALOG_IDENTIFIER = "urn:nbn:fi:att:data-catalog-ext"

api_permissions.rest.apierrors.create += [Role.METAX, Role.TEST_USER]
api_permissions.rest.apierrors.read += [Role.TEST_USER]
api_permissions.rest.apierrors["update"] = [Role.METAX, Role.TEST_USER]
api_permissions.rest.apierrors.delete += [Role.TEST_USER]

api_permissions.rest.contracts.create += [Role.TEST_USER]
api_permissions.rest.contracts.read += [Role.TEST_USER]
api_permissions.rest.contracts["update"] += [Role.TEST_USER]
api_permissions.rest.contracts.delete += [Role.TEST_USER]

api_permissions.rest.datacatalogs.create += [Role.TEST_USER]
api_permissions.rest.datacatalogs["update"] += [Role.TEST_USER]
api_permissions.rest.datacatalogs.delete += [Role.TEST_USER]

api_permissions.rest.datasets.create += [
    Role.API_AUTH_USER,
    Role.EXTERNAL,
    Role.TEST_USER,
]
api_permissions.rest.datasets["update"] += [
    Role.API_AUTH_USER,
    Role.EXTERNAL,
    Role.TEST_USER,
]
api_permissions.rest.datasets.delete += [
    Role.API_AUTH_USER,
    Role.EXTERNAL,
    Role.TEST_USER,
]

api_permissions.rest.directories.read += [Role.TEST_USER]

api_permissions.rest.files.create += [Role.TEST_USER]
api_permissions.rest.files.read += [Role.TEST_USER, Role.API_AUTH_USER]
api_permissions.rest.files["update"] += [Role.TEST_USER]
api_permissions.rest.files.delete += [Role.TEST_USER]

api_permissions.rest.filestorages.create += [Role.TEST_USER]
api_permissions.rest.filestorages.read += [Role.TEST_USER]
api_permissions.rest.filestorages["update"] += [Role.TEST_USER]
api_permissions.rest.filestorages.delete += [Role.TEST_USER]

api_permissions.rpc.files.delete_project.use += [Role.TEST_USER]

API_ACCESS = prepare_perm_values(api_permissions.to_dict())

from metax_api.settings.components.rems import REMS

REMS.update(
    {
        "ENABLED": True,
        "API_KEY": "key",
        "BASE_URL": "https://mock-rems/api",
        "ETSIN_URL_TEMPLATE": "https://etsin.fd-dev.csc.fi/dataset/%s",
        "METAX_USER": "rems-metax@example.com",
        "REPORTER_USER": "rems-reporter@example.com",
        "AUTO_APPROVER": "not-used",
        "FORM_ID": 1,
        "ORGANIZATION": "rems-test-org",
    }
)

from metax_api.settings.components.metax_v3 import METAX_V3
METAX_V3["INTEGRATION_ENABLED"] = False
METAX_V3["PROTOCOL"] = "http"

METRICS_API_ADDRESS = None
METRICS_API_TOKEN = None