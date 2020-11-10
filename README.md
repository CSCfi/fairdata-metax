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

#### Portainer

We will use portainer container management tool for various development dependencies. Command below will start portainer on every system startup.

`$ docker volume create portainer_data`

`$ docker run -d -p 8000:8000 -p 9000:9000 --name=portainer --restart=always -v /var/run/docker.sock:/var/run/docker.sock -v portainer_data:/data portainer/portainer-ce`

Finish the Portainer setup by logging in at http://localhost:9000, create a local endpoint from the Portainer interface. Go to Portainer settings and set App Templates url to: `https://raw.githubusercontent.com/EarthModule/portainer-templates/master/metax-templates.json`

Go to App Templates and start Postgres 9, Redis, ElasticSearch and RabbitMQ images. __NOTICE__: On each template setup click show advanced options and map the image exposed ports to identical host ports.

Attach to postgres container, start the postgres cli and create the database for Metax

#### mkcerts

Install [mkcerts][2] and run `mkcert -install` and after it the following command:
`mkcert -cert-file cert.pem -key-file key.pem 0.0.0.0 localhost 127.0.0.1 ::1 metax.csc.local 20.20.20.20`
Move the `cert.pem` and `key.pem` to `.certs` folder (create the folder if not present) in the repository root.

### Required environmental variables

copy `.env.template` as `.env` and fill required variables, you can find examples in ENV_VARS.md

### Initial setup commands

Activate your python 3.6 virtualenv, `cd` into `src` folder and run following commands:

`python manage.py migrate`

`python manage.py index_refdata`

`python manage.py reload_refdata_cache`

start the development server with:
`python manage.py runsslserver --certificate .certs/cert.pem --key .certs/key.pem 8008`

Open another terminal, `cd` into `src`, and load the initial data with `python manage.py loadinitialdata`

run the tests with command `DJANGO_ENV=test python manage.py test --failfast --keepdb -v 0`


[1]: https://docs.docker.com/engine/install/
[2]: https://github.com/FiloSottile/mkcert