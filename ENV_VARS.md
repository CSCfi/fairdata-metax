# Required environmental variables

Rename .env.template as .env and fill following variables manually

| Name                           | Description                              | example                                                                                                       |
| ------------------------------ | ---------------------------------------- | ------------------------------------------------------------------------------------------------------------- |
| DACITE_PREFIX                  |                                          |                                                                                                               |
| DACITE_URL                     |                                          |                                                                                                               |
| DATACITE_ETSIN_URL_TEMPLATE    |                                          |                                                                                                               |
| DATACITE_USERNAME              |                                          |                                                                                                               |
| DJANGO_SECRET_KEY              | replace with proper django secret key    |                                                                                                               |
| ERROR_FILES_PATH               |                                          | /home/user/repo-root/logs/errors                                                                              |
| ES_CONFIG_DIR                  |                                          | /home/user/repo-root/src/metax_api/tasks/refdata/refdata_indexer/resources/es-config/                         |
| LOCAL_REF_DATA_FOLDER          |                                          | /home/user/repo-root/csc/metax/metax-api/src/metax_api/tasks/refdata/refdata_indexer/resources/local-refdata/ |
| LOGGING_DEBUG_HANDLER_FILE     |                                          | /home/user/repo-root/csc/metax/metax-api/logs/metax_api.log                                                   |
| LOGGING_GENERAL_HANDLER_FILE   |                                          | /home/user/repo-root/csc/metax/metax-api/logs/metax_api.log                                                   |
| LOGGING_JSON_FILE_HANDLER_FILE |                                          | /home/user/repo-root/csc/metax/metax-api/logs/metax_api.json.log                                              |
| METAX_DATABASE                 | postgres database, must be created first | metax                                                                                                         |
| METAX_DATABASE_PASSWORD        | postgres owner of the database           |                                                                                                               |
| METAX_DATABASE_PORT            |                                          | 5432                                                                                                          |
| METAX_DATABASE_USER            |                                          | username                                                                                                      |
| OAJ_BASE_URL                   |                                          |                                                                                                               |
| ORG_FILE_PATH                  |                                          | /home/user/repo-root/src/metax_api/tasks/refdata/refdata_indexer/resources/organizations/organizations.csv    |
| RABBIT_MQ_PASSWORD             |                                          | guest                                                                                                         |
| RABBIT_MQ_USER                 |                                          | guest                                                                                                         |
| REDIS_LOCALHOST_PORT           |                                          | 6379                                                                                                          |
| REDIS_PASSWORD                 |                                          |                                                                                                               |
| REMS_ETSIN_URL_TEMPLATE        |                                          |                                                                                                               |
| REMS_FORM_ID                   |                                          |                                                                                                               |
| VALIDATE_TOKEN_URL             |                                          |                                                                                                               |
| WKT_FILENAME                   |                                          | /home/user/repo-root/src/metax_api/tasks/refdata/refdata_indexer/resources/uri_to_wkt.json                    |

