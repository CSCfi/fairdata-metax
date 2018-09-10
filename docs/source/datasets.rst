
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



Creating datasets
^^^^^^^^^^^^^^^^^^

An example of a dataset with minimum required fields in place to create a dataset in Metax (``POST /datasets``).


.. code-block:: python

    {
        "data_catalog": 1,
        "metadata_provider_org": "id-of-organisation",
        "metadata_provider_user": "id-of-user",
        "research_dataset": {
            "title": {
                "en": "Wonderful Title"
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



Retrieving datasets
^^^^^^^^^^^^^^^^^^^^

stuff



Updating datasets
^^^^^^^^^^^^^^^^^^

Update metadata
~~~~~~~~~~~~~~~~~~~~~

stuff


Update files
~~~~~~~~~~~~~

more stuff


Browse a dataset's files
~~~~~~~~~~~~~~~~~~~~~~~~~

stuff


Browse a dataset's versions
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

stuff


Dealing with reference data
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

a lot of stuff



Deleting datasets
^^^^^^^^^^^^^^^^^^

stufff
