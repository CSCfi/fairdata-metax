# Metax tests and testdata


## running tests


In order to run tests inside the vagrant vm, as metax-user, execute the following command:

```
runtests
```

The above will execute all tests, and stop execution at first failed test.

In order to run tests selectively inside the vagrant vm, as metax-user, navigate to src/ directory in this repository, and execute:

```
python manage.py test metax_api.tests.api.rest.base.views.datasets --failfast
```

The above executes all tests in directory src/metax_api/tests/api/rest/base/views/datasets, and fails stops execution at first failed test. To run a single testcase from a particular test suite, execute:

```
python manage.py test metax_api.tests.api.rest.base.views.datasets.write.CatalogRecordApiWriteDatasetVersioning.test_changing_files_creates_new_dataset_version
```

The above executes a single specified testcase only. Very useful when developing and testing a new feature, or debugging, since you can start a debugger (e.g. ipdb) inside the testcase code, or anywhere inside the Metax application.


## editing templates


The directory src/metax_api/tests/testdata includes some templates for datasets, files, data catalogs etc, that are used when generating testdata that is used during automated tests, and are also automatically imported to a test env when it is being provisioned. Sometimes, but at this point rarely, the templates should be altered, in case some new features are introduced or significant changes are made to the system. When that happens, the entire testdata should be regenerated. Changing the templates brings a risk of breaking existing tests.


## editing and generating testdata


The directory src/metax_api/tests/testdata contains the script ``generate_test_data.py`` which unsurprisingly generates the test data. The output file is ``test_data.json``, which is loaded into the db at the beginning of each testcase. Editing the templates is one way to edit pre-generated testdata, but modifying the ``generate_test_data.py`` script is another, and in the script more detailed and specific customizations to the testdata can be made. Changing the testdata brings a risk of breaking existing test.

Whenever testdata templates are changed, or testdata in general is re-generated, the resulting ``test_data.json`` file needs to be committed to the codebase.
