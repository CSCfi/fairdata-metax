
# Docker setup prerequisites

## Docker-Engine

Install Docker-Engine either following instructions below or looking up your platform specific instructions [from docs.docker.com][https://docs.docker.com/engine/install/] 

### Linux

`$ curl -fsSL https://get.docker.com -o get-docker.sh`

`$ sudo sh get-docker.sh`

`$ sudo usermod -aG docker $USER`

Log out and back in to activate non-sudo docker capabilities

### Mac

https://docs.docker.com/docker-for-mac/install/

## Portainer (Optional)

You can use portainer container management tool for monitoring various development dependencies. Command below will start portainer on every system startup.

`$ docker run -d -p 8000:8000 -p 9000:9000 --name=portainer --restart=always -v /var/run/docker.sock:/var/run/docker.sock -v portainer_data:/data portainer/portainer-ce`

Finish the Portainer setup by logging in at http://localhost:9000, create a local endpoint from the Portainer interface. 

## Dozzle (Optional)

[Dozzle](https://github.com/amir20/dozzle) can be used for reading the container logs:

`docker run --name dozzle -d --volume=/var/run/docker.sock:/var/run/docker.sock -p 8888:8080 amir20/dozzle:latest`

To show stopped/crashed container logs: `settings -> Show stopped containers`
