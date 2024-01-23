# Quickstart

1. Install [docker](https://docs.docker.com/engine/install/)
2. Init swarm: `docker swarm init`
3. Add docker-configs from `fairdata-docker` repo
4. Update `/etc/hosts` with `0.0.0.0 metax.fd-dev.csc.fi`
5. Deploy with `docker stack deploy -c docker-compose.yml --resolve-image=always --with-registry-auth metax-dev`
6. Init Metax `docker exec $(docker ps -q -f name=metax-dev_metax-web) python manage.py first_time_setup`

# Local development with Docker-swarm

After installing [Docker prerequisites](docker-prerequisites.md), Initialize the Swarm:

```bash
docker swarm init
```

## Running the stack locally

Append your local `/etc/hosts` file with:

```bash
0.0.0.0 metax.fd-dev.csc.fi
```

The default stack requires docker-configurations that can be found from `fairdata-docker` repository. 
If there is no access to that repo, refer the next section. 
When the required configurations are created, the stack can be deployed from the repository root with:

```bash
docker stack deploy -c docker-compose.yml --resolve-image=always --with-registry-auth metax-dev
```

After all the services has been started, Metax is available from `metax.fd-dev.csc.fi`. This stack contains the common Fairdata nginx proxy.

__NOTE__: Docker for Mac has a bug that prevents the required configurations to be deployed to metax-web container. To work around this, refer the `Required environmental variables` section in [README](/README.md)

## Running the stack without predefined docker-configs

Development setup without pre-required docker-configurations can be run with:

`docker stack deploy -c config-swap-stack.yml --resolve-image=always --with-registry-auth metax-dev`

After all the services has been started, Metax is available from `0.0.0.0:8008`. This stack allows the environment
variables to be changed without re-deploying the whole stack. To add certain docker-config to metax-web container:

```bash
docker service update --config-add source=<CONFIG-NAME>,target=/code/metax_api/settings/.env metax-dev_metax-web
```

To change existing configuration in metax-web container:

```bash
docker service update --config-rm <CONFIG-NAME> --config-add source=<NEW-CONFIG-NAME>,target=/code/metax_api/settings/.env metax-dev_metax-web
```

## Building related images

Docker images needed in the development can be built with the following commands:

```bash
docker build -t fairdata-docker.artifactory.ci.csc.fi/fairdata-metax-web .
docker build -t fairdata-docker.artifactory.ci.csc.fi/fairdata-metax-httpd -f httpd.dockerfile .
docker build -t fairdata-docker.artifactory.ci.csc.fi/fairdata-metax-sphinx -f sphinx.dockerfile .
```

## Pushing images to Artifactory

Ensure that you are logged in to Artifactory:

```bash
docker login fairdata-docker.artifactory.ci.csc.fi
```

Push commands for docker images:

```bash
docker push fairdata-docker.artifactory.ci.csc.fi/fairdata-metax-web
docker push fairdata-docker.artifactory.ci.csc.fi/fairdata-metax-httpd
docker push fairdata-docker.artifactory.ci.csc.fi/fairdata-metax-sphinx
```

## Running Metax management commands

To run Metax management commands, locate the running metax-dev_metax-web container (can be done with `docker container ls`)
and open terminal inside it with:

```bash
docker exec -it <container-name> bash
```

## Developing API documentation

The stack also contains a Sphinx autobuild server for documentation development. More specific instructions can be found from [here](api/README.md).
