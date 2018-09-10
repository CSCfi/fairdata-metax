
Datasets API
=============



General
--------

Datasets, like all objects accessible using the different apis in Metax, have an internal identifier field ``identifier``, which uniquely identifies a record withing Metax.

The standard way to retrieve a single dataset is by sending a request to the api ``GET /datasets/<pid>``, where ``<pid>`` is the record's internal identifier. Datasets can be listed and browsed using the api ``GET /datasets``. Retrieving a dataset or listing datasets can be augmented in various ways by using additional parameters. For details, see swagger's section about datasets.

When retrieving a single record using ``GET /datasets/<pid>``, the root level of the returned object contains various fields...

.. warning:: explain purpose of all fields here ? or somewhere else? make note about the signifigance of field research_dataset, and research_dataset.preferred_identifier.


Retrieving datasets
--------------------

.. warning:: below descriptions should be moved to swagger ?


**Retrieve in a different format**

The api ``GET /datasets/<pid>?dataset_format=someformat`` can be used to retrieve just the field ``research_dataset`` in another supported format.

Currently supported formats are:

* datacite

Using the value 'metax' will return a plain json → xml transformation of the default metax dataset json format.


**Retrieve with file metadata populated**

The api parameter ``GET /datasets/<pid>?file_details`` can be used to populate the objects in ``research_dataset.files`` and ``research_dataset.directories`` with their related file- and directory-specific metadata (which you normally would get using the ``GET /files/<pid>`` and ``GET /directories/<pid>`` apis). This is a convenience parameter for those cases when one wants to retrieve the details of described files and directories anyway.


**Retrieve by preferred_identifier**

API: ``GET /datasets?preferred_identifier=pid``. Searches a dataset by the requested ``preferred_identifier``.


**Getting a list of all identifiers in Metax**

API: ``GET /datasets/identifiers``. Lists field ``catalogrecord.identifier`` from all records.


**Getting a list of all unique preferred_identifiers in Metax**

API: ``GET /datasets/unique_preferred_identifiers``. Lists field ``catalogrecord.research_dataset.preferred_identifier`` from all records.



If-Modified-Since header in dataset API
----------------------------------------

If-Modified-Since header can be used in ``GET /datasets``, ``GET|PUT|PATCH /datasets/<pid>``, or ``GET /datasets/identifiers`` requests. This will return the result(s) only if the resources have been modified after the date specified in the header. In update operations the use of the header works as with other types of resources in Metax API. The format of the header should follow guidelines mentioned in https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/If-Modified-Since

If the requested resource has not been modified after the date specified in the header, the response will be ``304 Not Modified``.




.. _rst-roles-dataset-versioning:

Dataset versioning
-------------------



General
^^^^^^^^


**What does dataset versioning mean?**

In data catalogs that have dataset versioning enabled, certain kind of updates to a record can trigger dataset versioning, i.e. archiving of previous metadata content for later access, or even creating an entirely new record with new identifiers.

In short, when a dataset's metadata is changed, the previous metadata version is archived so it may be accessed or restored later. When a dataset's associated set of files is changed (the actual collection of data the dataset consists of), a new dataset version is created. This creates an entirely new dataset record in Metax, and generates new identifiers accordignly, both Metax internal identifier, and the important ``preferred_identifier`` field.

.. note:: As an end user who is editing the descriptions of their datasets, you generally shouldn't care that new metadata versions are being created. It does not affect your current dataset's identifiers, validity, or ability to access it or refer to it elsewhere. The old metadata is simply being archived so that it may be accessed or restored later. Bear in mind though, that old metadata versions are still as public information as everything else in the current most recent version.


**Terminology**

* Metadata version: Only metadata descriptions differ between metadata versions. The associated set of files is the same between different metadata versions of the same record. Identifiers do not change between metadata versions.
* Dataset version: The associated set of files differ between different dataset versions of the same record. Implicitly, this means also the metadata descriptions have changed. Identifiers change between versions.


**How to enable versioning?**

A data catalog has the setting ``dataset_versioning`` (boolean) which indicates whether or not datasets saved to that catalog should be versioned upon certain changes. In general, versioning is only enabled for ATT catalogs. Versioning cannot be enabled for harvested data catalogs (an error is raised if it is attempted, to prevent accidents). In versioned catalogs, preferred_identifiers can not be set by the user.


**What triggers a version change?**

When updating datasets in versioned catalogs, any change to the contents of the field ``research_dataset`` will result in a new metadata version, and changes in ``research_dataset.files`` or ``research_dataset.directories`` **may** result in a new dataset version being created. The different cases how versioning occurs are:

1) The contents of field ``research_dataset`` is modified in any way, except associated files have not changed:

    * During the update operation, old contents of the field ``research_dataset`` are archived (versioned) into a separate table. Otherwise, the same record that was updated, keeps existing as is, but a new value is generated for the field ``research_dataset.metadata_version_identifier``. This identifier is useful only for accessing old metadata versions.
    * After a successful update, old ``research_dataset`` versions can now be listed using the api ``GET /datasets/<pid>/metadata_versions``, and a specific old research_dataset content can be accessed using the api ``GET /datasets/<pid>/metadata_versions/<metadata_version_identifier>``. The api is read-only.

2) ``research_dataset.files`` or ``research_dataset.directories`` is modified by the user in a way that results in a *different set* of associated files:

    * During the update operation, a new dataset version is created (an entire new CatalogRecord object), with new identifiers generated.
    * The new dataset version record is linked to its previous dataset version record, and vica versa. Look for fields ``previous_dataset_version`` and ``next_dataset_version``.

Out of the two cases above, the second case is more significant, since it generates new identifiers, meaning that possible references to your dataset using the old ``preferred_identifier`` are now pointing to the previous version, which has a different files associated with it.

.. note:: Adding new files for the first time to an existing dataset that has 0 files or directories, will not create a new dataset version. This helps with dataset migration issues, and serves the purpose of "reserving" an identifier for a dataset, when a dataset doesn't yet have any files associated with it. In other words, you can publish a dataset, use its identifiers in your publications, and add files to it later, without making your previous references obsolete.


**When I am updating a dataset, how do I know when a new version has been created?**

In an API update request, when modifying a dataset in a way that causes a new dataset version to be created, the field ``new_version_created`` will be present in the API response json; the field tells that a new version has been created, and its related identifiers to access it. The new version then has to be GETted separately using the identifiers made available.

New metadata versions are not visible in the returned response in any way, except that the value of field ``metadata_version_identifier`` has changed.

.. note:: The field ``new_version_created`` is *not* present normally when GETting a single record or records. *Only* when updating a record (PUT or PATCH request), and a new dataset version has been created!


**How do I know beforehand if a new dataset version is going to be created?**

.. warning:: todo describe difference between describing files and selecting files.



Restrictions in old versions
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^


**Old metadata versions**

Modifying metadata of datasets in old metadata versions is not possible. There is a read-only api to view them. Restoring an old research_dataset metadata version can be achieved by accessing it using the api (``GET /datasets/<pid>/metadata_versions``), and using the content of a specific metadata version as an input in a normal update operation.


**Old dataset versions**

Modifying the set of files in an old dataset version is not possible. Metadata modifications in old dataset versions is still allowed (improve descriptions etc.).



Browsing a dataset's versions
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^


**Browsing metadata versions**

The api ``GET /datasets/<pid>/metadata_versions`` can be used to list metadata versions of a specific dataset. Access details of a specific version using the api ``GET /datasets/<pid>/metadata_versions/<metadata_version_identifier>``.


**Browsing dataset versions**

When retrieving a single dataset record, the following version-related fields are always present if other versions exist:

+--------------------------+-------------------------------------------------------------------------------------+
| Field                    | Purpose                                                                             |
+--------------------------+-------------------------------------------------------------------------------------+
| dataset_version_set      | A list of all other dataset versions of the dataset.                                |
+--------------------------+-------------------------------------------------------------------------------------+
| next_dataset_version     | Link to the next dataset version.                                                   |
+--------------------------+-------------------------------------------------------------------------------------+
| previous_dataset_version | Link to the previous dataset version.                                               |
+--------------------------+-------------------------------------------------------------------------------------+

Using the identifiers provided by the above fields, it's possible to retrieve information about a specific dataset version using the standard datasets api ``GET /datasets/<pid>``.



Uniqueness of datasets
-----------------------


**Non-harvested data catalogs**

In non-harvested data catalogs, the uniqueness of a dataset is generally determined by two fields:

* Identifier of the record object (``catalogrecord.identifier``), the value of which is unique globally, and generated server-side when the dataset is created. This is an internal identifier, used to identify and access a particular record in Metax.
* Identifier of the dataset (``catalogrecord.research_dataset.preferred_identifier``). This is the identifier of "The Dataset", i.e. the actual data and metadata you care about. The value is generated server-side when the dataset is created.


**Harvested data catalogs**

In harvested data catalogs, the value of ``preferred_identifier`` can be provided by the user (the harvester). The value of ``preferred_identifier`` is unique within its data catalog, so there can co-exist for example three datasets, in three different data catalogs, which have the same ``preferred_identifier`` value. When retrieving details of a single record using the API, information about these "alternate records" is included in the field ``alternate_record_set``, which contains a list of Metax internal identifiers of the other records, and is a read-only field.

If the field ``alternate_record_set`` is missing from a record, it means there are no alternate records sharing the same ``preferred_identifier`` in different data catalogs.



Examples
---------

These code examples are from the point of view of an end user. Using the API as an end user requires that the user logs in to ``METAX_DOMAIN/secure`` in order to get a valid access token, which will be used to authenticate with the API. The process for end user authentication is described on the page :doc:`end_users`.

When services interact with Metax, services have the additional responsibility of providing values for fields related to the current user modifying or creating resources, and generally taking care that the user is permitted to do whatever it is that they are doing.



Creating datasets
^^^^^^^^^^^^^^^^^^

Create a dataset with minimum required fields.


.. code-block:: python

    import requests

    dataset_data = {
        "data_catalog": 1,
        "research_dataset": {
            "title": {
                "en": "Test Dataset Title"
            },
            "description": {
                "en": "A descriptive description describing the contents of this dataset. Must be descriptive."
            },
            "creator": [
                {
                    "name": "Teppo Testaaja",
                    "@type": "Person",
                    "member_of": {
                        "name": {
                            "fi": "Mysteeriorganisaatio"
                        },
                        "@type": "Organization"
                    }
                }
            ],
            "curator": [
                {
                    "name": {
                        "und": "School Services, BIZ"
                    },
                    "@type": "Organization",
                    "identifier": "http://purl.org/att/es/organization_data/organization/organization_10076-E700"
                }
            ],
            "language":[{
                "title": { "en": "en" },
                "identifier": "http://lexvo.org/id/iso639-3/aar"
            }],
            "access_rights": {
                "access_type": {
                    "identifier": "http://purl.org/att/es/reference_data/access_type/access_type_open_access"
                },
                "restriction_grounds": {
                    "identifier": "http://purl.org/att/es/reference_data/restriction_grounds/restriction_grounds_1"
                }
            }
        }
    }

    headers = { 'Authorization': 'Bearer abc.def.ghi' }
    response = requests.post('https://metax-test.csc.fi/rest/datasets', json=dataset_data, headers=headers)
    assert response.status_code == 201, response.content
    print(response.json())


The response should look something like below:


.. code-block:: python

    {
        "id": 9152,
        "identifier": "54efa8b4-f03f-4155-9814-7de6aed4adce",
        "data_catalog": {
            "id": 1,
            "identifier": "urn:nbn:fi:att:2955e904-e3dd-4d7e-99f1-3fed446f96d1"
        },
        "dataset_version_set": [
            {
                "identifier": "54efa8b4-f03f-4155-9814-7de6aed4adce",
                "preferred_identifier": "urn:nbn:fi:att:58757004-e9b8-4ac6-834c-f5affaa7ec29",
                "removed": false,
                "date_created": "2018-09-10T12:18:38+03:00"
            }
        ],
        "deprecated": false,
        "metadata_owner_org": "myorganization.fi",
        "metadata_provider_org": "myorganization.fi",
        "metadata_provider_user": "myfairdataid@fairdataid",
        "research_dataset": {
            "title": {
                "en": "Test Dataset Title"
            },

            # <... all the other content that you uploaded ...>

            "preferred_identifier": "urn:nbn:fi:att:58757004-e9b8-4ac6-834c-f5affaa7ec29",
            "metadata_version_identifier": "49de6002-df1c-4090-9af6-d4e970904a5b"
        },
        "preservation_state": 0,
        "removed": True,
        "date_created": "2018-09-10T12:18:38+03:00",
        "user_created": "myfairdataid@fairdataid"
    }


Explanation of all the fields in the received response/newly created dataset:

* ``id`` An internal database identifier in Metax.
* ``identifier`` The unique identifier of the created record in Metax. This is the identifier to use when interacting with the dataset in Metax in any subsequent requests, such as when retrievng, updating, or deleting the dataset.
* ``dataset_version_set`` List of dataset versions associated with this record. Having just created a new record, there is obviously only one record listed.
* ``deprecated`` When files are deleted from IDA, any datasets containing those files are marked as "deprecated", and the value of this field will be set to ``True``. The value of this field may have an effect in other services, when displaying the dataset contents.
* ``metadata_owner_org``, ``metadata_provider_org``, ``metadata_provider_user`` Information about the creator of the metadata, and the associated organization. These are automatically placed according to the information available from the authentication token.
* ``research_dataset`` Now has two new fields generated by Metax:

    * ``preferred_identifier`` The persistent identifier of the dataset. This is the persistent identifier to use when externally referring to the dataset, in publications etc.
    * ``metadata_version_identifier`` The identifier of the specific metadata version. Will be generated by Metax each time the contents of the field ``research_dataset`` changes.

* ``preservation_state`` The PAS status of the record.
* ``removed`` Value will be ``True`` when the record is deleted.
* ``date_created`` Date when record was created.
* ``user_created`` Identifier of the user who created the record.

.. caution:: While in test environments using the internal ``id`` fields will work in place of the string-form unique identifiers (``identifier`` field), and are very handy for that purpose, in production environment they should never be used, since in some situations they can change without notice and may result in errors or accidentally referring to unintended objects, while the longer identifiers will be persistent, and are always safe to use.


**Errors: Required fields missing**


Try to create a dataset with required fields missing. Below example is missing the required field ``data_catalog``.


.. code-block:: python

    import requests

    dataset_data = {
        "research_dataset": {
            "title": {
                "en": "Test Dataset Title"
            },
            "description": {
                "en": "A descriptive description describing the contents of this dataset. Must be descriptive."
            },
            "creator": [
                {
                    "name": "Teppo Testaaja",
                    "@type": "Person",
                    "member_of": {
                        "name": {
                            "fi": "Mysteeriorganisaatio"
                        },
                        "@type": "Organization"
                    }
                }
            ],
            "curator": [
                {
                    "name": {
                        "und": "School Services, BIZ"
                    },
                    "@type": "Organization",
                    "identifier": "http://purl.org/att/es/organization_data/organization/organization_10076-E700"
                }
            ],
            "language":[{
                "title": { "en": "en" },
                "identifier": "http://lexvo.org/id/iso639-3/aar"
            }],
            "access_rights": {
                "access_type": {
                    "identifier": "http://purl.org/att/es/reference_data/access_type/access_type_open_access"
                },
                "restriction_grounds": {
                    "identifier": "http://purl.org/att/es/reference_data/restriction_grounds/restriction_grounds_1"
                }
            }
        }
    }

    headers = { 'Authorization': 'Bearer abc.def.ghi' }
    response = requests.post('https://metax-test.csc.fi/rest/datasets', json=dataset_data, headers=headers)
    assert response.status_code == 400, response.content
    print(response.json())


The error response should look something like this:

.. code-block:: python

    {
        "data_catalog": [
            "This field is required."
        ]
        "error_identifier": "2018-09-10T08:52:24-4c755256"
    }


**Errors: JSON validation error in field research_dataset**


Try to create a dataset when JSON schema validation fails for field ``research_dataset``. In the below example, the required field ``title`` is missing from the JSON blob inside field ``research_dataset``.

.. note::

    The contents of the field ``research_dataset`` are validated directly against the relevant schema from ``GET /schemas``, so probably either the ``ida`` schema or ``att`` schema, depending on if you are going to include files from IDA in your dataset or not. When schema validation fails, the entire output from the validator is returned. For an untrained eye, it can be difficult to find the relevant parts from the output. For that reason, it is strongly recommended that you:

    * Validate the contents of field ``research_dataset`` against the proper schema before you try to upload the dataset to Metax. Whatever JSON schema validator will work, and the error output will probably be easier to inspect compared to the output provided by Metax.
    * Start with a bare minimum working dataset description, and add new fields and descriptions incrementally, validating the contents periodically. This way, it will be a lot easier to backtrack and find any mistakes in the JSON structure.


.. code-block:: python

    import requests

    dataset_data = {
        "data_catalog": 1,
        "research_dataset": {
            "description": {
                "en": "A descriptive description describing the contents of this dataset. Must be descriptive."
            },
            "creator": [
                {
                    "name": "Teppo Testaaja",
                    "@type": "Person",
                    "member_of": {
                        "name": {
                            "fi": "Mysteeriorganisaatio"
                        },
                        "@type": "Organization"
                    }
                }
            ],
            "curator": [
                {
                    "name": {
                        "und": "School Services, BIZ"
                    },
                    "@type": "Organization",
                    "identifier": "http://purl.org/att/es/organization_data/organization/organization_10076-E700"
                }
            ],
            "language":[{
                "title": { "en": "en" },
                "identifier": "http://lexvo.org/id/iso639-3/aar"
            }],
            "access_rights": {
                "access_type": {
                    "identifier": "http://purl.org/att/es/reference_data/access_type/access_type_open_access"
                },
                "restriction_grounds": {
                    "identifier": "http://purl.org/att/es/reference_data/restriction_grounds/restriction_grounds_1"
                }
            }
        }
    }

    headers = { 'Authorization': 'Bearer abc.def.ghi' }
    response = requests.post('https://metax-test.csc.fi/rest/datasets', json=dataset_data, headers=headers)
    assert response.status_code == 400, response.content
    print(response.json())


The error response should look something like this:


.. code-block:: python

    {
        "research_dataset": [
            "'title' is a required property. Json path: []. Schema: { ... <very long output here>"
        ],
        "error_identifier": "2018-09-10T09:04:41-54fb4e22"
    }


Retrieving datasets
^^^^^^^^^^^^^^^^^^^^

Retrieving an existing dataset using a dataset's internal Metax indetifier:

.. code-block:: python

    import requests

    response = requests.get('https://metax-test.csc.fi/rest/datasets/abc123')
    assert response.status_code == 200, response.content
    print(response.json())


The retrieved content should look exactly the same as when creating a dataset. See above.



Updating datasets
^^^^^^^^^^^^^^^^^^

There are two important cases to consider when updating datasets in Metax, and both of them are related to dataset versioning. In the below examples, both cases of updating only dataset metadata, and adding files to a datatset and removing files from a dataset will be covered.

Read more about dataset versioning in :ref:`rst-roles-dataset-versioning`.



Update metadata
~~~~~~~~~~~~~~~~~

Update an existing dataset using a ``PUT`` request:

.. code-block:: python

    import requests

    # first retrieve a dataset that you are the owner of. be sure to authenticate
    # with the API when retrieving the dataset, since some sensitive fields from
    # the dataset are filtered out when retrieved by the general public. otherwise
    # you may accidentally lose some data when you upload the modified dataset!
    headers = { 'Authorization': 'Bearer abc.def.ghi' }
    response = requests.get('https://metax-test.csc.fi/rest/datasets/abc123', headers=headers)
    assert response.status_code == 200, response.content

    modified_data = response.json()
    modified_data['research_dataset']['description']['en'] = 'A More Accurdate Description'

    response = requests.put('https://metax-test.csc.fi/rest/datasets/abc123', json=modified_data, headers=headers)
    assert response.status_code == 200, response.content
    print(response.json())


A successful update operation will return response content that looks just as when creating a dataset. A new record is not created as a result of the update, so the content received from the response *is* the latest greatest version.

The exact same result can be achieved using a ``PATCH`` request, which allows you to only update specific fields. In the below example, we are updating only the field ``research_dataset``. While you can always use either ``PUT`` or ``PATCH`` for update, ``PATCH`` is always less risky in the sense that you will not accidentally modify fields you didn't intend to.


.. code-block:: python

    # ... the beginning is the same as in the above example

    # only updating the field research_dataset
    modified_data = {
        'research_dataset': response.json()['research_dataset']
    }

    modified_data['research_dataset']['description']['en'] = 'A More Accurdate Description'

    # add the HTTP Authorization header, since authentication will be required when executing write operations in the API.
    headers = { 'Authorization': 'Bearer abc.def.ghi' }
    response = requests.patch('https://metax-test.csc.fi/rest/datasets/abc123', json=modified_data, headers=headers)

    # ... the rest is the same as in the above example


The outcome of the update operation should be the same as in the above example.



Update files
~~~~~~~~~~~~~

In the below examples, "adding files", and "adding directories" effectively mean the same things: A bunch of files are being associated with the dataset - either one by one, or the contents of an entire directory at once. So later on in the examples when saying "files have been previously added", or "new files have been added", it basically means that either of the fields``research_dataset.files`` or ``research_dataset.directories`` already may have content inside them, or that new content has been added to either of those fields.


**Add files to a dataset for the first time**


Add files to a dataset, which didn't have any files associated with it when it was first created:


.. code-block:: python

    import requests

    headers = { 'Authorization': 'Bearer abc.def.ghi' }
    response = requests.get('https://metax-test.csc.fi/rest/datasets/abc123', headers=headers)
    assert response.status_code == 200, response.content

    modified_data = response.json()
    modified_data['research_dataset']['files'] = [
        {
            "title": "File Title",
            "identifier": "5105ab9839f63a909893183c14f9e9db",
            "description": "What is this file about",
            "use_category": {
                "identifier": "http://purl.org/att/es/reference_data/use_category/use_category_source",
            }
        }
    ]

    response = requests.put('https://metax-test.csc.fi/rest/datasets/abc123', json=modified_data, headers=headers)
    assert response.status_code == 200, response.content


Since files were added to the dataset for the first time, a new dataset version was not created, and the relevant dataset identifiers have not changed.


**Add files to a dataset, which already has files**


Add files to a dataset, which already has files associated with it, either from when it was first created, or files were later added to it by updating the dataset. The below case assumes the dataset had one existing file in it:


.. code-block:: python

    import requests

    headers = { 'Authorization': 'Bearer abc.def.ghi' }
    response = requests.get('https://metax-test.csc.fi/rest/datasets/abc123', headers=headers)
    assert response.status_code == 200, response.content

    modified_data = response.json()
    assert len(modified_data['research_dataset']['files']) == 1, 'initially the dataset has one file'

    """
    In this example, the contents of the field research_dataset['files'] is excepted to look
    like the following:
    [
        {
            "title": "File Title One",
            "identifier": "5105ab9839f63a909893183c14f9e111",
            "description": "What is this file about",
            "use_category": {
                "identifier": "http://purl.org/att/es/reference_data/use_category/use_category_source",
            }
        }
    ]
    """

    # add one more file to the dataset.
    modified_data['research_dataset']['files'].append({
        "title": "File Title Two",
        "identifier": "5105ab9839f63a909893183c14f9e9db",
        "description": "What is this file about then?",
        "use_category": {
            "identifier": "http://purl.org/att/es/reference_data/use_category/use_category_source",
        }
    })

    response = requests.put('https://metax-test.csc.fi/rest/datasets/abc123', json=modified_data, headers=headers)
    assert response.status_code == 200, response.content

    response_data = response.json()
    # when a new dataset version is created, the below key should always be present in the response.
    assert 'new_version_created' in response_data, 'new version should have been created'

    # the response returned the same version you began to modify, and therefore should only have the same
    # file in it that it had when it was retrieved above:
    assert len(response_data['research_dataset']['files']) == 1, 'the old dataset version should have one file'

    # the new automatically created new dataset version needs to be separately retrieved by
    # using the identifiers provided in the response.
    indetifier_of_new_dataset_version = response_data['new_version_created']['identifier']
    response = requests.get(
        'https://metax-test.csc.fi/rest/datasets/%s' % indetifier_of_new_dataset_version,
        headers=headers
    )
    assert response.status_code == 200, response.content
    response_data = response.json()
    assert len(response_data['research_dataset']['files']) == 2, 'new dataset version should have two files'


**Add a directory to a dataset**


Functionally, adding a directory to a dataset works the exact same way as adding a single file. The effect of adding a directory vs. a single file is a lot greater though, since all the files included in that directory, and its sub-directories, are then associated with the dataset.


.. warning:: explain somewhere the concepts of addings vs describing files. add link to that page here


Below is an example similar to the first example where we added files. The dataset in its initial state does not have any files or directories added to it:


.. code-block:: python

    import requests

    headers = { 'Authorization': 'Bearer abc.def.ghi' }
    response = requests.get('https://metax-test.csc.fi/rest/datasets/abc123', headers=headers)
    assert response.status_code == 200, response.content

    modified_data = response.json()
    modified_data['research_dataset']['directories'] = [
        {
            "title": "Directory Title",
            "identifier": "5105ab9839f63a909893183c14f9e113",
            "description": "What is this directory about",
            "use_category": {
                "identifier": "http://purl.org/att/es/reference_data/use_category/use_category_source",
            }
        }
    ]

    response = requests.put('https://metax-test.csc.fi/rest/datasets/abc123', json=modified_data, headers=headers)
    assert response.status_code == 200, response.content


Again, since files were added to the dataset for the first time, a new dataset version was not created, and the relevant dataset identifiers have not changed.


Deleting datasets
^^^^^^^^^^^^^^^^^^

Delete an existing dataset using a ``DELETE`` request:

.. code-block:: python

    import requests

    headers = { 'Authorization': 'Bearer abc.def.ghi' }
    response = requests.delete('https://metax-test.csc.fi/rest/datasets/abc123', headers=headers)
    assert response.status_code == 204, response.content

    # the dataset is now removed from the general API results
    response = requests.get('https://metax-test.csc.fi/rest/datasets/abc123')
    assert response.status_code == 404, 'metax should return 404 due to dataset not found'

    # removed datasets are still findable using the ?removed=true parameter
    response = requests.get('https://metax-test.csc.fi/rest/datasets/abc123?removed=true')
    assert response.status_code == 200, 'metax should have returned a dataset'


Browsing a dataset's files
^^^^^^^^^^^^^^^^^^^^^^^^^^^

Browsing the files of a dataset works the exact same way as browsing files in general (see :ref:`rst-browsing-files`), except that an additional query parameter ``cr_identifier=<pid>`` should be provided, in order to retrieve only those files and directories, which are present in the specified dataset.

Note that when browsing the files of a dataset, authentication with the API is not required, since if a dataset is retrievable from the API, it means it has been published, and its files are now public information.

Example:


.. code-block:: python

    import requests

    response = requests.delete('https://metax-test.csc.fi/rest/directories/dir123/files?cr_identifier=abc123')
    assert response.status_code == 200, response.content


Browsing a dataset's versions
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

stuff


Dealing with reference data
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

a lot of stuff

