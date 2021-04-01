# Local development with Docker-swarm

## Building metax-image

After installing [Docker prerequisites](docker-prerequisites.md), build the metax-web docker image with the following command:

`docker build -t fairdata-docker.artifactory.ci.csc.fi/fairdata-metax-web .`

## Building httpd-image 

`docker build -t fairdata-docker.artifactory.ci.csc.fi/fairdata-metax-httpd -f containers/apache-image.Dockerfile .`

## Pushing metax-image to Artifactory

 `docker push fairdata-docker.artifactory.ci.csc.fi/fairdata-metax-web`

## Pushing httpd-image to Artifactory

`docker push fairdata-docker.artifactory.ci.csc.fi/fairdata-metax-httpd`


## Running the stack locally

In the repository root, run

`docker stack deploy -c docker-compose.yml --resolve-image=always --with-registry-auth metax-dev`

## Running the stack without predefined docker-configs

`docker stack deploy -c config-swap-stack.yml --resolve-image=always --with-registry-auth metax-dev`

## Adding nginx to the stack

`docker stack deploy -c docker-compose.yml -c containers/nginx-docker.yml --resolve-image=always --with-registry-auth metax-dev`

## Running all services 

`docker stack deploy --resolve-image=always --with-registry-auth -c docker-compose.yml -c containers/nginx-docker.yml -c containers/apache-docker.yml metax-dev`

## Running Metax management commands

To run  Metax management commands, locate the running metax-dev_metax container and open terminal inside it with:

`docker exec -it <container-name> bash`

## Adding docker-config to the stack

`docker service update --config-add source=metax-web-stable-config,target=/code/metax_api/settings/.env metax-dev_metax`

## Swapping docker-config in the stack

`docker service update --config-rm <docker-config-name> --config-add source=<docker-config-name>,target=/code/metax_api/settings/.env metax-dev_metax`

