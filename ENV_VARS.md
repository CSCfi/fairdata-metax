# Required environmental variables

copy .env.template to .env and fill the required values from below table. Required column tells if you have to have the variable in the .env file

| Name                           | Required | Default                                                                               | Description                                                                                                |
| ------------------------------ | -------- | ------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------- |
| ADDITIONAL_USER_PROJECTS_PATH  | unknown  | ""                                                                                    | No clue if this is important                                                                               |
| ALLOWED_HOSTS                  | no       |                                                                                       | defines which IP-addresses are allowed to access metax, DJANGO_ENV=local overrides this                    |
| AUTH_SERVER_LOGOUT_URL         | unknown  |                                                                                       | Requires testing if this is needed                                                                         |
| DATACITE_ETSIN_URL_TEMPLATE    | yes      |                                                                                       |
| DATACITE_PASSWORD              | yes      |                                                                                       |
| DATACITE_PREFIX                | yes      |                                                                                       |
| DATACITE_URL                   | yes      |                                                                                       |
| DATACITE_USERNAME              | yes      |                                                                                       |
| DEBUG                          | no       | False                                                                                 |
| DJANGO_ENV                     | no       | local                                                                                 | Specifies the environment, corresponds with the environments found in src/metax_api/settings/environments/ |
| DJANGO_SECRET_KEY              | yes      |                                                                                       |
| ELASTIC_SEARCH_HOSTS           | yes      |                                                                                       | Elastic Search instance IP and port                                                                        |
| ELASTIC_SEARCH_PORT            | no       | 9200                                                                                  | Is not used currently, but should be in the future                                                         |
| ELASTIC_SEARCH_USE_SSL         | yes      |                                                                                       | Should Elastic Search queries use https                                                                    |
| ERROR_FILES_PATH               | no       | src/log/errors                                                                        | Error file folder                                                                                          |
| ES_CONFIG_DIR                  | no       | src/metax_api/tasks/refdata/refdata_indexer/resources/es-config                       | metax-ops compatibility                                                                                    |
| LOCAL_REF_DATA_FOLDER          | no       | src/metax_api/tasks/refdata/refdata_indexer/resources/local-refdata                   | metax-ops compatibility                                                                                    |
| LOGGING_DEBUG_HANDLER_FILE     | no       | src/log/metax_api.log                                                                 | metax-ops compatibility                                                                                    |
| LOGGING_GENERAL_HANDLER_FILE   | no       | src/log/metax_api.log                                                                 | metax-ops compatibility                                                                                    |
| LOGGING_JSON_FILE_HANDLER_FILE | no       | src/log/metax_api.json.log                                                            | metax-ops compatibility                                                                                    |
| METAX_DATABASE                 | yes      |                                                                                       | Postgres database name                                                                                     |
| METAX_DATABASE_PASSWORD        | yes      |                                                                                       | Postgres database password                                                                                 |
| METAX_DATABASE_PORT            | no       | 5432                                                                                  | Postgres instance exposed port                                                                             |
| METAX_DATABASE_USER            | yes      |                                                                                       | Postgres user which owns the database                                                                      |
| OAI_BASE_URL                   | yes      |                                                                                       |
| ORG_FILE_PATH                  | yes      | src/metax_api/tasks/refdata/refdata_indexer/resources/organizations/organizations.csv | metax-ops compatibility                                                                                    |
| RABBIT_MQ_HOSTS                | no       | localhost                                                                             | RabbitMQ instance IPs                                                                                       |
| RABBIT_MQ_PASSWORD             | no       | guest                                                                                 |
| RABBIT_MQ_PORT                 | no       | 5672                                                                                  |
| RABBIT_MQ_USER                 | no       | guest                                                                                 |
| RABBIT_MQ_USE_VHOST            | no       | False
| RABBIT_MQ_VHOST                | no       |                                                                                       | Required if RABBIT_MQ_USE_VHOST is True
| REDIS_HOST                     | yes      |                                                                                       | Redis instance IP                                                                                          |
| REDIS_LOCALHOST_PORT           | unknown  | 6379                                                                                  | Not sure if all references to this are gone                                                                |
| REDIS_PASSWORD                 | no       |                                                                                       |
| REDIS_PORT                     | no       | 6379                                                                                  |
| REDIS_TEST_DB                  | yes      |                                                                                       | Pick a number, any number                                                                                  |
| REDIS_USE_PASSWORD             | no       | false                                                                                 |
| REMS_API_KEY                   | no       |
| REMS_AUTO_APPROVER             | no       |
| REMS_BASE_URL                  | no       |
| REMS_ENABLED                   | no       |
| REMS_ETSIN_URL_TEMPLATE        | yes      |                                                                                       |
| REMS_FORM_ID                   | yes      |                                                                                       |
| REMS_METAX_USER                | no       |
| REMS_REPORTER_USER             | no       |
| SERVER_DOMAIN_NAME             | no       |
| V1_ENABLED                     | no       |                                                                                       |
| V2_ENABLED                     | no       |                                                                                       |
| VALIDATE_TOKEN_URL             | yes      |                                                                                       |
| WKT_FILENAME                   | no       | src/metax_api/tasks/refdata/refdata_indexer/resources/uri_to_wkt.json                 |