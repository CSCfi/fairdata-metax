This repository contains the code for Metax API service.

# Build status

## Test branch
[![Build Status](https://travis-ci.org/CSCfi/metax-api.svg?branch=test)](https://travis-ci.org/CSCfi/metax-api)

## Stable branch
[![Build Status](https://travis-ci.org/CSCfi/metax-api.svg?branch=stable)](https://travis-ci.org/CSCfi/metax-api)

License
-------
Copyright (c) 2018 Ministry of Education and Culture, Finland

Licensed under [GNU GPLv2 License](LICENSE)

## Setting up local development environment

### Prerequisites

#### Docker-Engine

Install Docker-Engine either following instructions below or looking up your platform specific instructions [from docs.docker.com][1] 

##### Linux

`$ curl -fsSL https://get.docker.com -o get-docker.sh`

`$ sudo sh get-docker.sh`

`$ sudo usermod -aG docker $USER`

Log out and back in to activate non-sudo docker capabilities

##### Mac

https://docs.docker.com/docker-for-mac/install/

#### Portainer (Optional)

We will use portainer container management tool for monitoring various development dependencies. Command below will start portainer on every system startup.

`$ docker volume create portainer_data` (optional for mac)

`$ docker run -d -p 8000:8000 -p 9000:9000 --name=portainer --restart=always -v /var/run/docker.sock:/var/run/docker.sock -v portainer_data:/data portainer/portainer-ce`

Finish the Portainer setup by logging in at http://localhost:9000, create a local endpoint from the Portainer interface. 

#### Docker commands

Run the following docker commands to start services:

##### Redis

`docker run -d -p 6379:6379 --name metax-redis -v metax-redis:/data --restart=unless-stopped redis`

##### Postgres

`docker run -d -p 5432:5432 --name metax-postgres -v metax-postgres:/var/lib/postgresql96/data -e POSTGRES_USER=metax_db_user -e POSTGRES_PASSWORD=YMDLekQMqrVKcs3 -e POSTGRES_DB=metax_db --restart=unless-stopped  postgres:9`

__NOTICE: copy values of `POSTGRES_USER`, `POSTGRES_PASSWORD` and `POSTGRES_DB` into your `.env` files as `METAX_DATABASE_USER`, `METAX_DATABASE_PASSWORD` and `METAX_DATABASE`__

##### Elasticsearch

`docker run -d -p 9200:9200 -p 9300:9300 -v metax-es:/usr/share/elasticsearch/data --name metax-es -e discovery.type=single-node --restart=unless-stopped elasticsearch:7.9.2`

##### RabbitMQ

`docker run -d -p 5671:5671 -p 5672:5672 -v metax-rabbitmq:/var/lib/rabbitmq --name metax-rabbitmq --restart=unless-stopped rabbitmq:latest`

#### mkcerts

Install [mkcerts][2] and run `mkcert -install` and after it the following command:
`mkcert -cert-file cert.pem -key-file key.pem 0.0.0.0 localhost 127.0.0.1 ::1 metax.csc.local 20.20.20.20`
Move the `cert.pem` and `key.pem` to `src/.certs` folder (create the folder if not present).

### Required environmental variables

copy `src/metax_api/settings/.env.template` as `src/metax_api/settings/.env` and fill required variables, you can find examples in ENV_VARS.md

### Run Metax inside a container (Optional)

Check the IP addresses of Redis, RabbitMQ, ElasticSearch and Postgres:9 either from Portainer container list (click the link in the container name to see all attributes) or by going to portainer network tab or by typing `docker container ps` followed by `docker network inspect bridge`

Build new docker image from repository root with this command (change ip-addresses to real ones:

`docker build -t metax-api:latest --build-arg METAX_DATABASE_HOST=xxx.xx.x.x --build-arg REDIS_HOST=xxx.xx.x.x --build-arg RABBITMQ_HOST=xxx.xx.x.x --build-arg ELASTIC_SEARCH_HOST=xxx.xx.x.x:xxxx .`

Run the built container with command:
`docker run -it --name metax-web --mount type=bind,source="$(pwd)"/src,target=/code -p 8008:8008 metax-api:latest`

You should see metax-server starting at port 8008 with hot reload enabled

### Initial setup commands

IF you configured metax-container, access the command line of the container with `docker exec -it metax-web bash`

__NOTICE: Skip activating virtualenv and navigating to src folder if you have metax running on container__

Activate your python 3.6 virtualenv, `cd` into `src` folder and run following commands:

setup the database with migrate command:

`python manage.py migrate`

__NOTICE: Skip following steps if your running metax on container and have terminal open in the container__

start the development server with:
`python manage.py runsslserver --certificate .certs/cert.pem --key .certs/key.pem 8008`

Open another terminal and `cd` into `src`, and load the initial data with following commands: 

__These commands must be run in both setups (container/not-container metax)__

`python manage.py index_refdata`

`python manage.py reload_refdata_cache`

`python manage.py loadinitialdata`

`python manage.py loaddata metax_api/tests/testdata/test_data.json` 

run the tests with command `DJANGO_ENV=test python manage.py test --failfast --keepdb -v 0`

Metax api is available from your browser at https://localhost:8008


[1]: https://docs.docker.com/engine/install/
[2]: https://github.com/FiloSottile/mkcert