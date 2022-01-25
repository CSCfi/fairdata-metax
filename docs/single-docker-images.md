# Docker setup without swarm

__NOTICE If you want to start the services everytime your computer boots, replace `--restart=unless-stopped` with `--restart=always`__

After installing [Docker prerequisites](docker-prerequisites.md),run the following docker commands to start services:

## Redis

`docker run -d -p 6379:6379 --name metax-redis -v metax-redis:/data --restart=unless-stopped redis`

## Postgres

`docker run -d -p 5432:5432 --name metax-postgres -v metax-postgres:/var/lib/postgresql/data -e POSTGRES_USER=metax_db_user -e POSTGRES_PASSWORD=YMDLekQMqrVKcs3 -e POSTGRES_DB=metax_db --restart=unless-stopped  postgres:12`

__NOTICE: copy values of `POSTGRES_USER`, `POSTGRES_PASSWORD` and `POSTGRES_DB` into your `.env` files as `METAX_DATABASE_USER`, `METAX_DATABASE_PASSWORD` and `METAX_DATABASE`__

## Elasticsearch

`docker run -d -p 9200:9200 -p 9300:9300 -v metax-es:/usr/share/elasticsearch/data --name metax-es -e discovery.type=single-node --restart=unless-stopped elasticsearch:7.9.2`

## RabbitMQ

`docker run -d -p 5671:5671 -p 5672:5672 -p 15672:15672 -v metax-rabbitmq:/var/lib/rabbitmq --name metax-rabbitmq --restart=unless-stopped rabbitmq:3-management`

## Metax
Check the IP addresses of Redis, RabbitMQ, ElasticSearch and Postgres:9 either from Portainer container list (click the link in the container name to see all attributes) or by going to portainer network tab or by typing `docker container ps` followed by `docker network inspect bridge`

Build new docker image from repository root with this command

`docker build -t fairdata-metax-web:latest .`

Run the built container with command:

`docker run -it --name fairdata-metax-web --mount type=bind,source="$(pwd)"/src,target=/code -p 8008:8008 --rm -e METAX_DATABASE_USER=<value> -e METAX_DATABASE_PASSWORD=<value> -e METAX_DATABASE=<value> -e REDIS_HOST=<value> -e RABBIT_MQ_HOSTS=<value> -e ELASTIC_SEARCH_HOSTS=<value> -e METAX_DATABASE_HOST=<value> fairdata-metax-web:latest`

## Metax management commands
access the command line of the container with `docker exec -it metax-web bash`
