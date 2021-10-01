# Developing documentation

Metax has API documentation implemented with Swagger and Sphinx. Below are instructions how they can be improved.

## Swagger

The openapi/swagger specification version 2 is used to document the Metax REST API.
Rest api descriptions are stored in the repository in /metax_api/swagger/v1/swagger.yaml
 and /metax_api/swagger/v2/swagger.yaml, depending on the interface version. 
From both yaml files, the corresponding html files are generated in the /metax_api/templates/swagger/ directory when Metax starts.

Swagger documentation can be edited directly in PyCharm and VS Code. There are good openapi plugins for both. One good plugin for both is the OpenAPI (Swagger) Editor. Another option is to use [Swagger editor](https://editor.swagger.io).

[VS Code plugin](https://marketplace.visualstudio.com/items?itemName=42Crunch.vscode-openapi&ssr=false#review-details)

[PyCharm plugin](https://plugins.jetbrains.com/plugin/14837-openapi-swagger-editor)

Although OpenApi is allowed to edit in json and yaml formats, in the case of Metax it has to be done in yaml format because otherwise the conversion to html format will not be possible.
A good starting point for studying OpenApi is [OpenApi specification V2](https://swagger.io/specification/v2/)

## Sphinx

The repository provides a Sphinx autobuild server in Docker container for conveniently write and develop the API documentation.
Below are the instructions how to use the server.

### Building the image

The autobuild server can be built with following command from repo root:

`docker build -t fairdata-docker.artifactory.ci.csc.fi/fairdata-metax-sphinx -f sphinx.dockerfile .`

### Running the server in standalone container

The server can be run with the following command, also from repo root:

`docker run -it -v $PWD/docs/api:/sphinx/ -p 8088:8000 fairdata-docker.artifactory.ci.csc.fi/fairdata-metax-sphinx`

### Running the server in stack

The autobuild server is also present in both of the stacks provided in the repo. The default dev env domain name `metax.fd-dev.csc.fi`is used in the documentation
so it should be added to `/etc/hosts` file to enable correct redirection in documentation links. When the domain name is added, the server is available from
`http://metax.fd-dev.csc.fi:8088`. By disabling browser cache redirection errors can be prevented. If all other fail, `0.0.0.0:8088` should work. 

### Additional notes

To conditionally add parts of the documentation, use only -directive. See [This](https://github.com/sphinx-doc/sphinx/issues/1115) for known issue with this
directive and headings.
