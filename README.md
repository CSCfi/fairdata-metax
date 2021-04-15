This repository contains the code for Metax API service.

## License

Copyright (c) 2018-2020 Ministry of Education and Culture, Finland

Licensed under [GNU GPLv2 License](LICENSE)


## Setting up local development environment

You can also set up the development environment with [Docker-swarm setup](/docs/docker-stack.md) or with [standalone Docker-containers setup](/docs/single-docker-images.md).

### Python dependencies

Install [Poetry](https://python-poetry.org/docs/) for your OS. Navigate to the repository root and run command `poetry install`. this will create and activate new Python virtualenv, installing all necessary Python packages to it.

You can generate traditional requirements.txt file with `poetry export --dev -E simplexquery --without-hashes -f requirements.txt --output requirements.txt`

### Required environmental variables

copy `src/metax_api/settings/.env.template` as `src/metax_api/settings/.env` and fill required variables, you can find examples in ENV_VARS.md

### Create log directory 

`mkdir -p /var/log/metax-api/errors`

### Initial setup commands

Activate your python 3.8 virtualenv, install requirements with `pip install -r requirements.txt`

`cd` into `src` folder and run following commands:

`python manage.py migrate`

start the development server with:

`python manage.py runserver 8008`

Open another terminal and `cd` into `src`, and load the initial data with following commands:

`python manage.py index_refdata`

`python manage.py reload_refdata_cache`

`python manage.py loadinitialdata`

`python manage.py loaddata metax_api/tests/testdata/test_data.json`

Metax api is available from your browser at http://localhost:8008

## Running tests

run the tests with command `DJANGO_ENV=test python manage.py test --parallel --failfast --keepdb -v 0`





