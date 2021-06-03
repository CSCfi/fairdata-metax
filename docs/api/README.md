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
### Enabling autobuilds

To install required dependencies run the following:
``pip install sphinx sphinx-autobuild sphinx_rtd_theme``

To start server, run following in metax-api directory:
``sphinx-autobuild -t {envtag} docs/source/ docs/build/``, where {envtag} is one of local_development, test, stable or production. Envtag determines the target environment of the documentation.

Note that the server should be run on the host machine since virtual machine does not build docs automatically.

To conditionally add parts of the documentation, use only -directive. See [This](https://github.com/sphinx-doc/sphinx/issues/1115) for known issue with this directive and headings.
