# Local development with Docker-swarm

## Building metax-image

After installing [Docker prerequisites](docker-prerequisites.md), build the metax-web docker image with the following command:

`docker build -t fairdata-docker.artifactory.ci.csc.fi/fairdata-metax-web .`

<!-- ## Building httpd-image 

`docker build -t fairdata-docker.artifactory.ci.csc.fi/fairdata-metax-httpd -f containers/apache-image.Dockerfile .` -->

## Pushing metax-image to Artifactory

 `docker push fairdata-docker.artifactory.ci.csc.fi/fairdata-metax-web`

<!-- ## Pushing httpd-image to Artifactory

`docker push fairdata-docker.artifactory.ci.csc.fi/fairdata-metax-httpd` -->


## Running the stack locally

In the repository root, run

`docker stack deploy -c docker-compose.yml --resolve-image=always --with-registry-auth metax-dev`

This stack contains the common Fairdata nginx proxy.

## Running the stack without predefined docker-configs

`docker stack deploy -c config-swap-stack.yml --resolve-image=always --with-registry-auth metax-dev`

## Running Metax management commands

To run Metax management commands, locate the running metax-dev_metax-web container and open terminal inside it with:

`docker exec -it <container-name> bash`

## Adding docker-config to the stack

`docker service update --config-add source=metax-web-stable-config,target=/code/metax_api/settings/.env metax-dev_metax-web`

## Swapping docker-config in the stack

`docker service update --config-rm <docker-config-name> --config-add source=<docker-config-name>,target=/code/metax_api/settings/.env metax-dev_metax-web`
