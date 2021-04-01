
# Docker setup prerequisites

## Docker-Engine

Install Docker-Engine either following instructions below or looking up your platform specific instructions [from docs.docker.com][1] 

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

[1]: https://docs.docker.com/engine/install/