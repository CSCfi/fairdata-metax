## Enabling autobuilds

To install required dependencies run the following:
``pip install sphinx sphinx-autobuild sphinx_rtd_theme``

To start server, run following in metax-api directory:
``sphinx-autobuild -t {envtag} docs/source/ docs/build/``, where {envtag} is one of local_development, test, stable or production. Envtag determines the target environment of the documentation.

Note that the server should be run on the host machine since virtual machine does not build docs automatically.

To conditionally add parts of the documentation, use only -directive. See [This](https://github.com/sphinx-doc/sphinx/issues/1115) for known issue with this directive and headings.