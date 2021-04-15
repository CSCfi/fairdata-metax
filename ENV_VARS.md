# Required environmental variables

copy .env.template to .env and fill the required values from below table. Required column tells if you have to have the variable in the .env file

| Name                                    | Required | Default                                                                               | Description                                                                                                |
| --------------------------------------- | -------- | ------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------- |
| ALWAYS_RELOAD_REFERENCE_DATA_ON_RESTART | no       | True
| ADDITIONAL_USER_PROJECTS_PATH           | no       | ""                                                                                    | Defines the file location where additional projects can be given for specific endusers                     |
| ALLOWED_HOSTS                           | no       | []                                                                                    | Defines which IP-addresses are allowed to access metax, DJANGO_ENV=local overrides this                    |
| AUTH_SERVER_LOGOUT_URL                  | yes      |                                                                                       | URL on the auth server where logout button on /secure page will finally redirect the user                  |
| DATACITE_ETSIN_URL_TEMPLATE             | yes      |                                                                                       | Landing page URL for the dataset for Datacite service. Must contain '%s'                                   |
| DATACITE_PASSWORD                       | yes      |                                                                                       |
| DATACITE_PREFIX                         | yes      |                                                                                       |
| DATACITE_URL                            | yes      |                                                                                       |
| DATACITE_USERNAME                       | yes      |                                                                                       |
| DEBUG                                   | no       | False                                                                                 |
| DJANGO_ENV                              | no       | local                                                                                 | Specifies the environment, corresponds with the environments found in src/metax_api/settings/environments/ |
| DJANGO_SECRET_KEY                       | yes      |                                                                                       |
| ELASTIC_SEARCH_HOSTS                    | no       | localhost                                                                             | Elastic Search instance IPs                                                                                |
| ELASTIC_SEARCH_PORT                     | no       | 9200                                                                                  |
| ELASTIC_SEARCH_USE_SSL                  | no       | False                                                                                 | Should Elastic Search queries use https                                                                    |
| ERROR_FILES_PATH                        | no       | src/log/metax-api/errors                                                              | Error file folder                                                                                          |
| ES_CONFIG_DIR                           | no       | src/metax_api/tasks/refdata/refdata_indexer/resources/es-config                       | metax-ops compatibility                                                                                    |
| LOCAL_REF_DATA_FOLDER                   | no       | src/metax_api/tasks/refdata/refdata_indexer/resources/local-refdata                   | metax-ops compatibility                                                                                    |
| LOGGING_DEBUG_HANDLER_FILE              | no       | /var/log/metax-api/metax_api.log                                                      | metax-ops compatibility                                                                                    |
| LOGGING_GENERAL_HANDLER_FILE            | no       | /var/log/metax-api/metax_api.log                                                      | metax-ops compatibility                                                                                    |
| LOGGING_JSON_FILE_HANDLER_FILE          | no       | /var/log/metax-api/metax_api.json.log                                                 | metax-ops compatibility                                                                                    |
| METAX_DATABASE                          | yes      |                                                                                       | Postgres database name                                                                                     |
| METAX_DATABASE_HOST                     | no       | localhost                                                                             | Postgres database host                                                                                     |
| METAX_DATABASE_PASSWORD                 | yes      |                                                                                       | Postgres database password                                                                                 |
| METAX_DATABASE_PORT                     | no       | 5432                                                                                  | Postgres instance exposed port                                                                             |
| METAX_DATABASE_USER                     | yes      |                                                                                       | Postgres user which owns the database                                                                      |
| OAI_BASE_URL                            | no       | https://metax.fd-dev.csc.fi/oai/                                                      | Metax OAI server base url                                                                                  |
| OAI_BATCH_SIZE                          | no       | 25                                                                                    | Batch size of the oai response                                                                             |
| OAI_REPOSITORY_NAME                     | no       | Metax                                                                                 | Repository name of OAI server                                                                              |
| OAI_ETSIN_URL_TEMPLATE                  | yes      |                                                                                       | Landing page URL of the dataset. Must contain '%s'                                                         |
| OAI_ADMIN_EMAIL                         | yes      |                                                                                       |
| ORG_FILE_PATH                           | no       | src/metax_api/tasks/refdata/refdata_indexer/resources/organizations/organizations.csv | metax-ops compatibility                                                                                    |
| RABBIT_MQ_HOSTS                         | no       | localhost                                                                             | RabbitMQ instance IPs                                                                                      |
| RABBIT_MQ_PASSWORD                      | no       | guest                                                                                 |
| RABBIT_MQ_PORT                          | no       | 5672                                                                                  |
| RABBIT_MQ_USER                          | no       | guest                                                                                 |
| RABBIT_MQ_USE_VHOST                     | no       | False
| RABBIT_MQ_VHOST                         | no       |                                                                                       | Required if RABBIT_MQ_USE_VHOST is True                                                                    |
| REDIS_HOST                              | no       | localhost                                                                             | Redis instance IPs                                                                                         |
| REDIS_PASSWORD                          | no       |                                                                                       | Required if REDIS_USE_PASSWORD is True
| REDIS_PORT                              | no       | 6379                                                                                  |
| REDIS_TEST_DB                           | no       | 15                                                                                    | Pick a number, any number                                                                                  |
| REDIS_USE_PASSWORD                      | no       | False                                                                                 |
| REMS_API_KEY                            | no       |                                                                                       | Required if REMS is enabled                                                                                |
| REMS_AUTO_APPROVER                      | no       |                                                                                       | Required if REMS is enabled                                                                                |
| REMS_BASE_URL                           | no       |                                                                                       | Required if REMS is enabled                                                                                |
| REMS_ENABLED                            | no       | False
| REMS_ETSIN_URL_TEMPLATE                 | no       |                                                                                       | Landing page URL of the dataset. Required if REMS is enabled, Must contain '%s'                            |
| REMS_FORM_ID                            | no       |                                                                                       | Required if REMS is enabled                                                                                |
| REMS_METAX_USER                         | no       |                                                                                       | Required if REMS is enabled                                                                                |
| REMS_REPORTER_USER                      | no       |                                                                                       | Required if REMS is enabled                                                                                |
| SERVER_DOMAIN_NAME                      | no       | metax.fd-dev.csc.fi                                                                   |
| ENABLE_V1_ENDPOINTS                     | no       | True
| ENABLE_V2_ENDPOINTS                     | no       | True
| VALIDATE_TOKEN_URL                      | no       | https://127.0.0.1/secure/validate_token                                               | URL where bearer tokens get validated
| WKT_FILENAME                            | no       | src/metax_api/tasks/refdata/refdata_indexer/resources/uri_to_wkt.json                 |