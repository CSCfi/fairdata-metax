
Datasets API
=============



General
--------

Datasets, like all objects accessible using the different APIs in Metax, have an internal identifier field ``identifier``, which uniquely identifies a record withing Metax.

The standard way to retrieve a single dataset is by sending a request to the API ``GET /rest/datasets/<pid>``, where ``<pid>`` is the record's internal identifier. The result returned from the API contains various information about the state of the dataset, such as last-modified timestamps, PAS state, and other data. Included is also the field probably of the most interest to end users: The ``research_dataset`` field. ``research_dataset`` contains the actual user-provided metadata descriptions of *The* Dataset.

Datasets can be listed and browsed using the API ``GET /rest/datasets``. Retrieving a dataset or listing datasets can be augmented in various ways by using additional parameters. For details, see swagger's section about datasets.


.. admonition:: todo

    explain purpose of all fields here ? or somewhere else? make note about the signifigance of field research_dataset, and research_dataset.preferred_identifier.



Terminology
^^^^^^^^^^^^

The results returned from the API ``GET /rest/datasets/<pid>`` are also sometimes called "catalog records", or "records". At the top level there are Data Catalogs, and Data Catalogs contain Catalog Records. Catalog records can be considered the "technical" name of a dataset inside Metax.



Data Catalogs
^^^^^^^^^^^^^^

Every dataset belongs in a Data Catalog. Data catalogs house datasets with different origins (harvested vs. Fairdata user provided datasets), slightly different schemas (IDA and ATT catalogs for example), and datasets in some catalogs are automatically versioned. While reading datasets from all catalogs is possible by anybody (save for some data which might be considered as sensitive, such as personal information), adding datasets to catalogs can be restricted: Others allow adding only by known services, but some also by end users.

Data catalogs can be browsed by using the API ``/rest/datacatalogs``. The data catalog data model visualization can be found here https://tietomallit.suomi.fi/model/mdc.

The official Fairdata data catalogs with end user write access are:

+---------+-----------------------------------------------------------------------------------+---------------------------------+
| Catalog | Purpose                                                                           | Identifier                      |
+---------+-----------------------------------------------------------------------------------+---------------------------------+
| IDA     | Store datasets which have files stored in the IDA Fairdata service.               | urn:nbn:fi:att:data-catalog-ida |
+---------+-----------------------------------------------------------------------------------+---------------------------------+
| ATT     | Store datasets which have data stored elsewhere than in the IDA Fairdata service. | urn:nbn:fi:att:data-catalog-att |
+---------+-----------------------------------------------------------------------------------+---------------------------------+



Choosing the right Data Catalog
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Other than the harvested data catalogs managed by Fairdata harvesters, the two most interesting data catalogs are probably the IDA catalog, and the ATT catalog, commonly referred to as "the Fairdata catalogs". Also common for these catalogs is the fact that end users can add datasets to them. For the most parts these two catalogs are behaviourally identical, but they do serve different purposes, and have one critical technical difference.


**IDA catalog**

The IDA catalog hosts datasets, which have their files stored in the Fairdata IDA service. The datasets stored in this catalog use a schema which allow to use the fields ``research_dataset.files`` and ``research_dataset.directories``, which are used to list and describe related files in IDA. On the other hand, the schema is missing the field ``research_dataset.remote_resources``, meaning it does not allow listing files stored in other file storages than IDA.

.. note:: For end users it is important to note, that you will never be "creating" or "storing" new files in Metax or in IDA by using Metax API: Files are always stored by using the IDA service (https://www.fairdata.fi/en/ida/). Once the files have been stored (frozen) using IDA, the metadata of the stored files is automatically sent to Metax. Then, using Metax APIs, the metadata of the files can be browsed, and linked to datasets, and finally published to the world as part of a dataset.


**ATT catalog**

The ATT catalog is the opposite of the IDA catalog: It hosts datasets whose files are stored elsewhere than in the Fairdata IDA service. The datasets in this catalog use a schema which allow using the field ``research_dataset.remote_resources``, while missing the IDA related fields.


**Attaching a dataset to a catalog**

When creating a new dataset and wishing to use for example the ATT catalog, the dataset would be linked to it in the following way:


.. code-block:: python

    import requests

    dataset_data = {
        "data_catalog": "urn:nbn:fi:att:data-catalog-att",
        "research_dataset": {
            # lots of content...
        }
    }

    headers = { 'Authorization': 'Bearer abc.def.ghi' }
    response = requests.post('https://__METAX_ENV_DOMAIN__/rest/datasets', json=dataset_data, headers=headers)
    assert response.status_code == 201, response.content


For more involving examples, see the :ref:`rst-dataset-examples` section for datasets.



Dataset lifecycle in Metax
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

1) A dataset is created and published.
2) A dataset's metadata descriptions may be updated multiple times.
3) A dataset may be explicitly deleted, or implicitly deprecated as a result of someone deleting a dataset's files in IDA.
4) A dataset may have new dataset versions created when files are added or removed.
5) A dataset has been proposed to PAS, and is in a PAS process. Adding or removing files is not possible.
6) Dataset is stored to PAS inside a mountain.



Retrieving datasets
--------------------

.. admonition:: todo

    below descriptions should be moved to swagger ?


**Retrieve in a different format**

The API ``GET /rest/datasets/<pid>?dataset_format=someformat`` can be used to retrieve just the field ``research_dataset`` in another supported format.

Currently supported formats are:

* datacite

Using the value 'metax' will return a plain json → xml transformation of the default metax dataset json format.


**Retrieve with file metadata populated**

The API parameter ``GET /rest/datasets/<pid>?file_details`` can be used to populate the objects in ``research_dataset.files`` and ``research_dataset.directories`` with their related file- and directory-specific metadata (which you normally would get using the ``GET /rest/files/<pid>`` and ``GET /rest/directories/<pid>`` APIs). This is a convenience parameter for those cases when one wants to retrieve the details of described files and directories anyway.


**Retrieve by preferred_identifier**

API: ``GET /rest/datasets?preferred_identifier=pid``. Searches a dataset by the requested ``preferred_identifier``.


**Getting a list of all identifiers in Metax**

API: ``GET /rest/datasets/identifiers``. Lists field ``catalogrecord.identifier`` from all records.


**Getting a list of all unique preferred_identifiers in Metax**

API: ``GET /rest/datasets/unique_preferred_identifiers``. Lists field ``catalogrecord.research_dataset.preferred_identifier`` from all records.



If-Modified-Since header in dataset API
----------------------------------------

If-Modified-Since header can be used in ``GET /rest/datasets``, ``GET|PUT|PATCH /rest/datasets/<pid>``, or ``GET /rest/datasets/identifiers`` requests. This will return the result(s) only if the resources have been modified after the date specified in the header. In update operations the use of the header works as with other types of resources in Metax API. The format of the header should follow guidelines mentioned in https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/If-Modified-Since

If the requested resource has not been modified after the date specified in the header, the response will be ``304 Not Modified``.




.. _rst-dataset-versioning:

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

A data catalog has the setting ``dataset_versioning`` (boolean) which indicates whether or not datasets saved to that catalog should be versioned upon certain changes. In general, versioning is only enabled for IDA catalogs. Versioning cannot be enabled for harvested data catalogs (an error is raised if it is attempted, to prevent accidents). In versioned catalogs, preferred_identifiers can not be set by the user.


**What triggers a version change?**

When updating datasets in versioned catalogs, any change to the contents of the field ``research_dataset`` will result in a new metadata version, and changes in ``research_dataset.files`` or ``research_dataset.directories`` **may** result in a new dataset version being created. The different cases how versioning occurs are:

1) The contents of field ``research_dataset`` is modified in any way, except associated files have not changed:

    * During the update operation, old contents of the field ``research_dataset`` are archived (versioned) into a separate table. Otherwise, the same record that was updated, keeps existing as is, but a new value is generated for the field ``research_dataset.metadata_version_identifier``. This identifier is useful only for accessing old metadata versions.
    * After a successful update, old ``research_dataset`` versions can now be listed using the API ``GET /rest/datasets/<pid>/metadata_versions``, and a specific old research_dataset content can be accessed using the API ``GET /rest/datasets/<pid>/metadata_versions/<metadata_version_identifier>``. The API is read-only.

2) ``research_dataset.files`` or ``research_dataset.directories`` is modified by the user in a way that results in a *different set* of associated files:

    * During the update operation, a new dataset version is created (an entire new CatalogRecord object), with new identifiers generated.
    * The new dataset version record is linked to its previous dataset version record, and vica versa. Look for fields ``previous_dataset_version`` and ``next_dataset_version``.

Out of the two cases above, the second case is more significant, since it generates new identifiers, meaning that possible references to your dataset using the old ``preferred_identifier`` are now pointing to the previous version, which has a different files associated with it.

.. important:: Adding new files for the first time to an existing dataset that has 0 files or directories, will not create a new dataset version. This helps with dataset migration issues, and serves the purpose of "reserving" an identifier for a dataset, when a dataset doesn't yet have any files associated with it. In other words, you can publish a dataset, use its identifiers in your publications, and add files to it later, without making your previous references obsolete.


**When I am updating a dataset, how do I know when a new version has been created?**

In an API update request, when modifying a dataset in a way that causes a new dataset version to be created, the field ``new_version_created`` will be present in the API response json; the field tells that a new version has been created, and its related identifiers to access it. The new version then has to be GETted separately using the identifiers made available.

New metadata versions are not visible in the returned response in any way, except that the value of field ``metadata_version_identifier`` has changed.

.. note:: The field ``new_version_created`` is *not* present normally when GETting a single record or records. *Only* when updating a record (PUT or PATCH request), and a new dataset version has been created!


**How do I know beforehand if a new dataset version is going to be created?**

Take a look at the topic :ref:`rst-describing-and-adding-files`.



Restrictions in old versions
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^


**Old metadata versions**

Modifying metadata of datasets in old metadata versions is not possible. There is a read-only API to view them. Restoring an old research_dataset metadata version can be achieved by accessing it using the API (``GET /rest/datasets/<pid>/metadata_versions``), and using the content of a specific metadata version as an input in a normal update operation.


**Old dataset versions**

Modifying the set of files in an old dataset version is not possible. Metadata modifications in old dataset versions is still allowed (improve descriptions etc.).



Browsing a dataset's versions
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^


**Browsing metadata versions**

The API ``GET /rest/datasets/<pid>/metadata_versions`` can be used to list metadata versions of a specific dataset. Access details of a specific version using the API ``GET /rest/datasets/<pid>/metadata_versions/<metadata_version_identifier>``.


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

Using the identifiers provided by the above fields, it's possible to retrieve information about a specific dataset version using the standard datasets API ``GET /rest/datasets/<pid>``.



Uniqueness of datasets
-----------------------


**Non-harvested data catalogs**

In non-harvested data catalogs, the uniqueness of a dataset is generally determined by two fields:

* Identifier of the record object (``catalogrecord.identifier``), the value of which is unique globally, and generated server-side when the dataset is created. This is an internal identifier, used to identify and access a particular record in Metax.
* Identifier of the dataset (``catalogrecord.research_dataset.preferred_identifier``). This is the identifier of "The Dataset", i.e. the actual data and metadata you care about. The value is generated server-side when the dataset is created.


**Harvested data catalogs**

In harvested data, the value of preferred_identifier can and should be extracted from the harvested dataset’s source data. The harvester is allowed to set the preferred_identifier for the datasets it creates in Metax, so harvest source organization should indicate which field they would like to use as the preferred_identifier.

The value of ``preferred_identifier`` is unique within its data catalog, so there can co-exist for example three datasets, in three different data catalogs, which have the same ``preferred_identifier`` value. When retrieving details of a single record using the API, information about these "alternate records" is included in the field ``alternate_record_set``, which contains a list of Metax internal identifiers of the other records, and is a read-only field.

If the field ``alternate_record_set`` is missing from a record, it means there are no alternate records sharing the same ``preferred_identifier`` in different data catalogs.



.. _rst-describing-and-adding-files:

Describing files vs. adding and removing files
-----------------------------------------------

A distinction needs to be made between *describing* files in a dataset, and *adding or removing* files. As explained in the topic :ref:`rst-dataset-versioning`, just editing a dataset's metadata (including the dataset-specific file metadata in fields ``research_dataset.files`` and ``research_dataset.directories``) does not produce new dataset versions, while *adding* new files will produce new dataset versions, as will *removing* files. Yet, both describing the files, and adding or removing files, happens by inserting objects inside the fields ``research_dataset.files`` and ``research_dataset.directories``, or by removing the same objects when wishing to remove files from a dataset. How to know which is which, and what to expect when updating datasets and dealing with files?


**Adding and describing single files**


As long as we are dealing with only single files, the distinction between describing and adding files does not matter; they are effectively the same thing. Same goes for removing. Either the file is listed in ``research_dataset.files``, or it isn't. But when starting to add or remove directories, the disctintion becomes more necessary.


**Adding and describing directories**


When we add an entire directory to a dataset (into field ``research_dataset.directories``), all the files inside that directory, and its sub-directories, are added to the dataset. No further action is required. If we additionally want to add descriptions for those added files and directories, remarks about their relevance to the dataset, add titles, and so on, we can still achieve that by inserting additional entries of those files inside field ``research_dataset.files``. This operation no longer counts as "adding files" though, since they have already been included in the dataset when the parent directory of the file (or even the root directory of the entire project) was wadded to ``research_dataset.directories``.

The same logic applies when adding descriptions for sub-directories: Adding more directory-entries to ``research_dataset.directories`` does not count as "adding files", as long as a parent directory has already been added there. When you are publishing a new dataset to Metax, or pushing an update, Metax will find the top-most directory that has been added, and use that as the basis when adding files to the dataset. All the other entries only count as "describing metadata".

It is possible though to for example add multiple directories that should all be considered as "top level" parent directories, in which case all those directories are recognized as such, and files from all those directories are still added to the dataset. Likewise, a directory may be added to the dataset, plus some files separately outside of that directory. Metax will recognize the individual files listed in ``research_dataset.files`` do not belong to any of the listed directories, and they will be added separately.


**Removing directories**

As can probably be guessed from the previous paragraphs, removing an entry from ``research_dataset.directories`` does not necessarily count as "removing" files, if there still exists an attached parent directory. In that case, removing the directory would only count as editing metadata descriptions.


**How to exclude files or directories?**

When a directory has been added, excluding files or sub-directories from that directory is not yet supported.



Using an existing dataset as a template
----------------------------------------

If you want to use an existing dataset as a template for a new dataset, you can retrieve a dataset from the API, remove two particular identifying fields from the returned object, and then use the resulting object in a new create request to Metax API. Example:


.. code-block:: python

    import requests

    headers = { 'Authorization': 'Bearer abc.def.ghi' }
    response = requests.get('https://__METAX_ENV_DOMAIN__/rest/datasets/abc123', headers=headers)
    assert response.status_code == 200, response.content
    print('Retrieved a dataset that has identifier: %s' response.data['identifier'])

    new_dataset = response.data
    del new_dataset['identifier']
    del new_dataset['research_dataset']['preferred_identifier']

    response = requests.post('https://__METAX_ENV_DOMAIN__/rest/datasets', json=new_dataset, headers=headers)
    assert response.status_code == 201, response.content
    print('Created a new dataset that has identifier: %s' response.data['identifier'])



.. _rst-datasets-reference-data:

Reference data guide
---------------------

A dataset's metadata descriptions requires the use of reference data in quite many places, and actually even the bare minimum accepted dataset already uses reference data in three different fields.

Below is a table (...python dictionary) that shows which relations and fields of the field ``research_dataset`` require or offer the option to use reference data. For example, ``research_dataset.language`` is a relation, while ``research_dataset.language.identifier`` is a field of that relation. The table is best inspected when holding in the other hand the visualization at https://tietomallit.suomi.fi/model/mrd, which is a visualization of the schema of field ``research_dataset`` (plus the main record object, ``CatalogRecord``, which is actually what the API ``/rest/datasets`` returns).

In the table, on the left hand side is described the relation object which uses reference data (not that one or several of the relations can be an array of objects, instead of a single object), and on the right hand side is "mode", and "url". Mode is either "required" or "optional", where "required" means the relation will only accept values from reference data, and all other values will result in a validation error, while "optional" means reference data can be used if opting to do so, but custom values will also be accepted (such as custom identifiers if you have any). The "url" finally is the url where the reference data can be found in ElasticSearch.


**But first about ResearchAgent, Organization, and Person**


In the schema visualization at https://tietomallit.suomi.fi/model/mrd, there are various relations leading from the object ``ResearchDataset`` to the object ``ResearchAgent``. The visualization is - at current time - unable to visualize "oneOf"-relations of JSON schemas. If opening one of the actual dataset schema files provided by the API ``/rest/schemas``, such as https://__METAX_ENV_DOMAIN__/rest/schemas/ida_dataset, and searching for the string "oneOf" inside that file, you will see that the object ``ResearchAgent`` is actually an instance of either the ``Person`` or the ``Organization`` object. That means, that for example when setting the ``research_dataset.curator`` relation (which is an array), the contents of the ``curator`` field can be either a person, an organization, or a mix of persons and organizations.

This needs to be taken into account when looking which reference data to use, when dealing with ``Person`` or ``Organization`` objects in the schema. In the below table, the person- and organization-related relations have been separated from the rest of the fields that use reference data, and then split, to make it easier to find out which reference data to use depending on what kind of object is being used.


.. code-block:: python

    {
        "research_dataset.theme.identifier":                                { "mode": "required", "url": "https://__METAX_ENV_DOMAIN__/es/reference_data/keyword" },
        "research_dataset.field_of_science.identifier":                     { "mode": "required", "url": "https://__METAX_ENV_DOMAIN__/es/reference_data/field_of_science" },
        "research_dataset.remote_resources.license.identifier":             { "mode": "required", "url": "https://__METAX_ENV_DOMAIN__/es/reference_data/license" },
        "research_dataset.remote_resources.resource_type.identifier":       { "mode": "required", "url": "https://__METAX_ENV_DOMAIN__/es/reference_data/resource_type" },
        "research_dataset.remote_resources.file_type.identifier":           { "mode": "required", "url": "https://__METAX_ENV_DOMAIN__/es/reference_data/file_type" },
        "research_dataset.remote_resources.use_category.identifier":        { "mode": "required", "url": "https://__METAX_ENV_DOMAIN__/es/reference_data/use_category" },
        "research_dataset.remote_resources.media_type":                     { "mode": "optional", "url": "https://__METAX_ENV_DOMAIN__/es/reference_data/mime_type" },
        "research_dataset.language.identifier":                             { "mode": "required", "url": "https://__METAX_ENV_DOMAIN__/es/reference_data/language" },
        "research_dataset.access_rights.access_type.identifier":            { "mode": "required", "url": "https://__METAX_ENV_DOMAIN__/es/reference_data/access_type" },
        "research_dataset.access_rights.restriction_grounds.identifier":    { "mode": "required", "url": "https://__METAX_ENV_DOMAIN__/es/reference_data/restriction_grounds" },
        "research_dataset.access_rights.license.identifier":                { "mode": "required", "url": "https://__METAX_ENV_DOMAIN__/es/reference_data/license" },
        "research_dataset.other_identifier.type.identifier":                { "mode": "required", "url": "https://__METAX_ENV_DOMAIN__/es/reference_data/identifier_type" },
        "research_dataset.spatial.place_uri.identifier":                    { "mode": "required", "url": "https://__METAX_ENV_DOMAIN__/es/reference_data/location" },
        "research_dataset.files.file_type.identifier":                      { "mode": "required", "url": "https://__METAX_ENV_DOMAIN__/es/reference_data/file_type" },
        "research_dataset.files.use_category.identifier":                   { "mode": "required", "url": "https://__METAX_ENV_DOMAIN__/es/reference_data/use_category" },
        "research_dataset.directories.use_category.identifier":             { "mode": "required", "url": "https://__METAX_ENV_DOMAIN__/es/reference_data/use_category" },
        "research_dataset.provenance.spatial.place_uri.identifier":         { "mode": "required", "url": "https://__METAX_ENV_DOMAIN__/es/reference_data/location" },
        "research_dataset.provenance.lifecycle_event.identifier":           { "mode": "required", "url": "https://__METAX_ENV_DOMAIN__/es/reference_data/lifecycle_event" },
        "research_dataset.provenance.preservation_event.identifier":        { "mode": "required", "url": "https://__METAX_ENV_DOMAIN__/es/reference_data/preservation_event" },
        "research_dataset.provenance.event_outcome.identifier":             { "mode": "required", "url": "https://__METAX_ENV_DOMAIN__/es/reference_data/event_outcome" },
        "research_dataset.provenance.used_entity.type.identifier":          { "mode": "required", "url": "https://__METAX_ENV_DOMAIN__/es/reference_data/resource_type" },
        "research_dataset.infrastructure.identifier":                       { "mode": "required", "url": "https://__METAX_ENV_DOMAIN__/es/reference_data/research_infra" },
        "research_dataset.relation.relation_type.identifier":               { "mode": "required", "url": "https://__METAX_ENV_DOMAIN__/es/reference_data/relation_type" },
        "research_dataset.relation.entity.type.identifier":                 { "mode": "required", "url": "https://__METAX_ENV_DOMAIN__/es/reference_data/resource_type" },

        # organizations. note! can be recursive through the organization-object's `is_part_of` relation
        "research_dataset.is_output_of.source_organization.identifier":     { "mode": "required", "url": "https://__METAX_ENV_DOMAIN__/es/organization_data/organization" },
        "research_dataset.is_output_of.has_funding_agency.identifier":      { "mode": "required", "url": "https://__METAX_ENV_DOMAIN__/es/organization_data/organization" },
        "research_dataset.is_output_of.funder_type.identifier.identifier":  { "mode": "required", "url": "https://__METAX_ENV_DOMAIN__/es/organization_data/organization" },
        "research_dataset.other_identifier.provider.identifier":            { "mode": "required", "url": "https://__METAX_ENV_DOMAIN__/es/organization_data/organization" },
        "research_dataset.contributor.contributor_role.identifier":         { "mode": "optional", "url": "https://__METAX_ENV_DOMAIN__/es/reference_data/contributor_role" },
        "research_dataset.publisher.contributor_role.identifier":           { "mode": "optional", "url": "https://__METAX_ENV_DOMAIN__/es/reference_data/contributor_role" },
        "research_dataset.curator.contributor_role.identifier":             { "mode": "optional", "url": "https://__METAX_ENV_DOMAIN__/es/reference_data/contributor_role" },
        "research_dataset.creator.contributor_role.identifier":             { "mode": "optional", "url": "https://__METAX_ENV_DOMAIN__/es/reference_data/contributor_role" },
        "research_dataset.rights_holder.contributor_role.identifier":       { "mode": "optional", "url": "https://__METAX_ENV_DOMAIN__/es/reference_data/contributor_role" },
        "research_dataset.provenance.was_associated_with.contributor_role.identifier": { "mode": "optional", "url": "https://__METAX_ENV_DOMAIN__/es/reference_data/contributor_role" }

        # persons
        "research_dataset.contributor.member_of.identifier":          { "mode": "optional", "url": "https://__METAX_ENV_DOMAIN__/es/organization_data/organization" },
        "research_dataset.contributor.contributor_role.identifier":   { "mode": "optional", "url": "https://__METAX_ENV_DOMAIN__/es/reference_data/contributor_role" },
        "research_dataset.contributor.contributor_type.identifier":   { "mode": "optional", "url": "https://__METAX_ENV_DOMAIN__/es/reference_data/contributor_type" },
        "research_dataset.publisher.member_of.identifier":            { "mode": "optional", "url": "https://__METAX_ENV_DOMAIN__/es/organization_data/organization" },
        "research_dataset.publisher.contributor_role.identifier":     { "mode": "optional", "url": "https://__METAX_ENV_DOMAIN__/es/reference_data/contributor_role" },
        "research_dataset.publisher.contributor_type.identifier":     { "mode": "optional", "url": "https://__METAX_ENV_DOMAIN__/es/reference_data/contributor_type" },
        "research_dataset.curator.member_of.identifier":              { "mode": "optional", "url": "https://__METAX_ENV_DOMAIN__/es/organization_data/organization" },
        "research_dataset.curator.contributor_role.identifier":       { "mode": "optional", "url": "https://__METAX_ENV_DOMAIN__/es/reference_data/contributor_role" },
        "research_dataset.curator.contributor_type.identifier":       { "mode": "optional", "url": "https://__METAX_ENV_DOMAIN__/es/reference_data/contributor_type" },
        "research_dataset.creator.member_of.identifier":              { "mode": "optional", "url": "https://__METAX_ENV_DOMAIN__/es/organization_data/organization" },
        "research_dataset.creator.contributor_role.identifier":       { "mode": "optional", "url": "https://__METAX_ENV_DOMAIN__/es/reference_data/contributor_role" },
        "research_dataset.creator.contributor_type.identifier":       { "mode": "optional", "url": "https://__METAX_ENV_DOMAIN__/es/reference_data/contributor_type" },
        "research_dataset.rights_holder.member_of.identifier":        { "mode": "optional", "url": "https://__METAX_ENV_DOMAIN__/es/organization_data/organization" },
        "research_dataset.rights_holder.contributor_role.identifier": { "mode": "optional", "url": "https://__METAX_ENV_DOMAIN__/es/reference_data/contributor_role" },
        "research_dataset.rights_holder.contributor_type.identifier": { "mode": "optional", "url": "https://__METAX_ENV_DOMAIN__/es/reference_data/contributor_type" },
        "research_dataset.provenance.was_associated_with.member_of.identifier":        { "mode": "optional", "url": "https://__METAX_ENV_DOMAIN__/es/organization_data/organization" },
        "research_dataset.provenance.was_associated_with.contributor_role.identifier": { "mode": "optional", "url": "https://__METAX_ENV_DOMAIN__/es/reference_data/contributor_role" },
        "research_dataset.provenance.was_associated_with.contributor_type.identifier": { "mode": "optional", "url": "https://__METAX_ENV_DOMAIN__/es/reference_data/contributor_type" }
    }



.. _rst-dataset-examples:

Examples
---------

These code examples are from the point of view of an end user. Using the API as an end user requires that the user logs in to ``https://__METAX_ENV_DOMAIN__/secure`` in order to get a valid access token, which will be used to authenticate with the API. The process for end user authentication is described on the page :doc:`end_users`.

When services interact with Metax, services have the additional responsibility of providing values for fields related to the current user modifying or creating resources, and generally taking care that the user is permitted to do whatever it is that they are doing.



Retrieve minimal valid dataset template
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The API ``GET /rpc/datasets/get_minimal_dataset_template`` returns a valid minimal dataset, that can be used as-is to create a dataset into Metax.


.. code-block:: python

    import requests

    response = requests.get('https://__METAX_ENV_DOMAIN__/rpc/datasets/get_minimal_dataset_template?type=enduser')
    assert response.status_code == 200, response.content

    # dataset_data can now be used in a POST request to create a new dataset!
    dataset_data = response.json()

    headers = { 'Authorization': 'Bearer abc.def.ghi' }
    response = requests.post('https://__METAX_ENV_DOMAIN__/rest/datasets', json=dataset_data, headers=headers)
    assert response.status_code == 201, response.content
    print(response.json())


.. important:: The other code examples below contain the full dataset in written form to give you an idea what the dataset contents really look like. While these textual examples can sometimes get outdated, the dataset template from the API is always kept up-to-date, and would serve as a good starting point for your own dataset.



Creating datasets
^^^^^^^^^^^^^^^^^^

Create a dataset with minimum required fields.


.. code-block:: python

    import requests

    dataset_data = {
        "data_catalog": "urn:nbn:fi:att:data-catalog-att",
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
                    "identifier": "http://uri.suomi.fi/codelist/fairdata/organization/code/01901"
                }
            ],
            "language":[{
                "title": { "en": "en" },
                "identifier": "http://lexvo.org/id/iso639-3/aar"
            }],
            "access_rights": {
                "access_type": {
                    "identifier": "http://uri.suomi.fi/codelist/fairdata/access_type/code/open"
                }
            }
        }
    }

    headers = { 'Authorization': 'Bearer abc.def.ghi' }
    response = requests.post('https://__METAX_ENV_DOMAIN__/rest/datasets', json=dataset_data, headers=headers)
    assert response.status_code == 201, response.content
    print(response.json())


The response should look something like below:


.. code-block:: python

    {
        "id": 9152,
        "identifier": "54efa8b4-f03f-4155-9814-7de6aed4adce",
        "data_catalog": {
            "id": 1,
            "identifier": "urn:nbn:fi:att:data-catalog-att"
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
* ``deprecated`` When files are deleted or unfrozen from IDA, any datasets containing those files are marked as "deprecated", and the value of this field will be set to ``True``. The value of this field may have an effect in other services, when displaying the dataset contents.
* ``metadata_owner_org``, ``metadata_provider_org``, ``metadata_provider_user`` Information about the creator of the metadata, and the associated organization. These are automatically placed according to the information available from the authentication token.
* ``research_dataset`` Now has two new fields generated by Metax:

    * ``preferred_identifier`` The persistent identifier of the dataset. This is the persistent identifier to use when externally referring to the dataset, in publications etc.
    * ``metadata_version_identifier`` The identifier of the specific metadata version. Will be generated by Metax each time the contents of the field ``research_dataset`` changes.

* ``preservation_state`` The PAS status of the record.
* ``removed`` Value will be ``True`` when the record is deleted.
* ``date_created`` Date when record was created.
* ``user_created`` Identifier of the user who created the record.

.. caution:: While in test environments using the internal ``id`` fields will work in place of the string-form unique identifiers (``identifier`` field), and are very handy for that purpose, in production environment they should never be used, since in some situations they can change without notice and may result in errors or accidentally referring to unintended objects, while the longer identifiers will be persistent, and are always safe to use. Example how to use the internal ``id`` field to retrieve a dataset: https://__METAX_ENV_DOMAIN__/rest/datasets/12 (note: assuming there exists a record with the id: 12)


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
                    "identifier": "http://uri.suomi.fi/codelist/fairdata/organization/code/01901"
                }
            ],
            "language":[{
                "title": { "en": "en" },
                "identifier": "http://lexvo.org/id/iso639-3/aar"
            }],
            "access_rights": {
                "access_type": {
                    "identifier": "http://uri.suomi.fi/codelist/fairdata/access_type/code/open"
                }
            }
        }
    }

    headers = { 'Authorization': 'Bearer abc.def.ghi' }
    response = requests.post('https://__METAX_ENV_DOMAIN__/rest/datasets', json=dataset_data, headers=headers)
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

.. important::

    The contents of the field ``research_dataset`` are validated directly against the relevant schema from ``GET /rest/schemas``, so probably either the ``ida`` schema or ``att`` schema, depending on if you are going to include files from IDA in your dataset or not. When schema validation fails, the entire output from the validator is returned. For an untrained eye, it can be difficult to find the relevant parts from the output. For that reason, it is strongly recommended that you:

    * Validate the contents of field ``research_dataset`` against the proper schema before you try to upload the dataset to Metax. Whatever JSON schema validator will work, and the error output will probably be easier to inspect compared to the output provided by Metax.
    * Start with a bare minimum working dataset description, and add new fields and descriptions incrementally, validating the contents periodically. This way, it will be a lot easier to backtrack and find any mistakes in the JSON structure.


.. code-block:: python

    import requests

    dataset_data = {
        "data_catalog": "urn:nbn:fi:att:data-catalog-att",
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
                    "identifier": "http://uri.suomi.fi/codelist/fairdata/organization/code/01901"
                }
            ],
            "language":[{
                "title": { "en": "en" },
                "identifier": "http://lexvo.org/id/iso639-3/aar"
            }],
            "access_rights": {
                "access_type": {
                    "identifier": "http://uri.suomi.fi/codelist/fairdata/access_type/code/open"
                }
            }
        }
    }

    headers = { 'Authorization': 'Bearer abc.def.ghi' }
    response = requests.post('https://__METAX_ENV_DOMAIN__/rest/datasets', json=dataset_data, headers=headers)
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

Retrieving an existing dataset using a dataset's internal Metax identifier:

.. code-block:: python

    import requests

    response = requests.get('https://__METAX_ENV_DOMAIN__/rest/datasets/abc123')
    assert response.status_code == 200, response.content
    print(response.json())


The retrieved content should look exactly the same as when creating a dataset. See above.



Updating datasets
^^^^^^^^^^^^^^^^^^

There are two important cases to consider when updating datasets in Metax, and both of them are related to dataset versioning. In the below examples, both cases of updating only dataset metadata, and adding files to a datatset and removing files from a dataset will be covered.

Read more about dataset versioning in :ref:`rst-dataset-versioning`.



Update metadata
~~~~~~~~~~~~~~~~~

Update an existing dataset using a ``PUT`` request:

.. code-block:: python

    import requests

    # first retrieve a dataset that you are the owner of
    headers = { 'Authorization': 'Bearer abc.def.ghi' }
    response = requests.get('https://__METAX_ENV_DOMAIN__/rest/datasets/abc123', headers=headers)
    assert response.status_code == 200, response.content

    modified_data = response.json()
    modified_data['research_dataset']['description']['en'] = 'A More Accurdate Description'

    response = requests.put('https://__METAX_ENV_DOMAIN__/rest/datasets/abc123', json=modified_data, headers=headers)
    assert response.status_code == 200, response.content
    print(response.json())


A successful update operation will return response content that looks just as when creating a dataset. A new record is not created as a result of the update, so the content received from the response *is* the latest greatest version.

.. caution:: When updating a dataset, be sure to authenticate with the API when retrieving the dataset, since some sensitive fields from the dataset are filtered out when retrieved without authentication (or by the general public). Otherwise you may accidentally lose some data when you upload the modified dataset!

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
    response = requests.patch('https://__METAX_ENV_DOMAIN__/rest/datasets/abc123', json=modified_data, headers=headers)

    # ... the rest is the same as in the above example


The outcome of the update operation should be the same as in the above example.



Update files
~~~~~~~~~~~~~

In the below examples, "adding files", and "adding directories" effectively mean the same things: A bunch of files are being associated with the dataset - either one by one, or the contents of an entire directory at once. So later on in the examples when saying "files have been previously added", or "new files have been added", it basically means that either of the fields ``research_dataset.files`` or ``research_dataset.directories`` already may have content inside them, or that new content has been added to either of those fields.


**Add files to a dataset for the first time**


Add files to a dataset, which didn't have any files associated with it when it was first created:


.. code-block:: python

    import requests

    headers = { 'Authorization': 'Bearer abc.def.ghi' }
    response = requests.get('https://__METAX_ENV_DOMAIN__/rest/datasets/abc123', headers=headers)
    assert response.status_code == 200, response.content

    modified_data = response.json()
    modified_data['research_dataset']['files'] = [
        {
            "title": "File Title",
            "identifier": "5105ab9839f63a909893183c14f9e9db",
            "description": "What is this file about",
            "use_category": {
                "identifier": "http://uri.suomi.fi/codelist/fairdata/use_category/code/source",
            }
        }
    ]

    response = requests.put('https://__METAX_ENV_DOMAIN__/rest/datasets/abc123', json=modified_data, headers=headers)
    assert response.status_code == 200, response.content


Since files were added to the dataset for the first time, a new dataset version was not created, and the relevant dataset identifiers have not changed. Note: In the above example, the field ``use_category`` contains a rather long url-form value. This field only accepts pre-defined values from a specific reference data. Read more about :doc:`reference_data`.


**Add files to a dataset, which already has files**


Add files to a dataset, which already has files associated with it, either from when it was first created, or files were later added to it by updating the dataset. The below case assumes the dataset had one existing file in it:


.. code-block:: python

    import requests

    headers = { 'Authorization': 'Bearer abc.def.ghi' }
    response = requests.get('https://__METAX_ENV_DOMAIN__/rest/datasets/abc123', headers=headers)
    assert response.status_code == 200, response.content

    modified_data = response.json()
    assert len(modified_data['research_dataset']['files']) == 1, 'initially the dataset has one file'

    """
    In this example, the contents of the field research_dataset['files'] is expected to look
    like the following:
    [
        {
            "title": "File Title One",
            "identifier": "5105ab9839f63a909893183c14f9e111",
            "description": "What is this file about",
            "use_category": {
                "identifier": "http://uri.suomi.fi/codelist/fairdata/use_category/code/source",
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
            "identifier": "http://uri.suomi.fi/codelist/fairdata/use_category/code/source",
        }
    })

    response = requests.put('https://__METAX_ENV_DOMAIN__/rest/datasets/abc123', json=modified_data, headers=headers)
    assert response.status_code == 200, response.content

    response_data = response.json()
    # when a new dataset version is created, the below key should always be present in the response.
    assert 'new_version_created' in response_data, 'new version should have been created'

    # the response returned the same version you began to modify, and therefore should only have the same
    # file in it that it had when it was retrieved above:
    assert len(response_data['research_dataset']['files']) == 1, 'the old dataset version should have one file'

    # the new automatically created new dataset version needs to be separately retrieved by
    # using the identifiers provided in the response.
    identifier_of_new_dataset_version = response_data['new_version_created']['identifier']
    response = requests.get(
        'https://__METAX_ENV_DOMAIN__/rest/datasets/%s' % identifier_of_new_dataset_version,
        headers=headers
    )
    assert response.status_code == 200, response.content
    response_data = response.json()
    assert len(response_data['research_dataset']['files']) == 2, 'new dataset version should have two files'


**Add a directory to a dataset**


Functionally, adding a directory to a dataset works the exact same way as adding a single file. The effect of adding a directory vs. a single file is a lot greater though, since all the files included in that directory, and its sub-directories, are then associated with the dataset.

Below is an example similar to the first example where we added files. The dataset in its initial state does not have any files or directories added to it:


.. code-block:: python

    import requests

    headers = { 'Authorization': 'Bearer abc.def.ghi' }
    response = requests.get('https://__METAX_ENV_DOMAIN__/rest/datasets/abc123', headers=headers)
    assert response.status_code == 200, response.content

    modified_data = response.json()
    modified_data['research_dataset']['directories'] = [
        {
            "title": "Directory Title",
            "identifier": "5105ab9839f63a909893183c14f9e113",
            "description": "What is this directory about",
            "use_category": {
                "identifier": "http://uri.suomi.fi/codelist/fairdata/use_category/code/source",
            }
        }
    ]

    response = requests.put('https://__METAX_ENV_DOMAIN__/rest/datasets/abc123', json=modified_data, headers=headers)
    assert response.status_code == 200, response.content


Again, since files were added to the dataset for the first time, a new dataset version was not created, and the relevant dataset identifiers have not changed.


Deleting datasets
^^^^^^^^^^^^^^^^^^

Delete an existing dataset using a ``DELETE`` request:

.. code-block:: python

    import requests

    headers = { 'Authorization': 'Bearer abc.def.ghi' }
    response = requests.delete('https://__METAX_ENV_DOMAIN__/rest/datasets/abc123', headers=headers)
    assert response.status_code == 204, response.content

    # the dataset is now removed from the general API results
    response = requests.get('https://__METAX_ENV_DOMAIN__/rest/datasets/abc123')
    assert response.status_code == 404, 'metax should return 404 due to dataset not found'

    # removed datasets are still findable using the ?removed=true parameter
    response = requests.get('https://__METAX_ENV_DOMAIN__/rest/datasets/abc123?removed=true')
    assert response.status_code == 200, 'metax should have returned a dataset'


Browsing a dataset's files
^^^^^^^^^^^^^^^^^^^^^^^^^^^

File metadata of a dataset can be browsed in two ways.

First way is to retrieve a flat list of file metadata of all the files included in the dataset. Be advised though: The below API endpoint does not utilize paging! If the number of files is very large, the amount of data being downloaded by default can be very large! Therefore, it is highly recommended to use the query parameter ``file_fields=field_1,field_2,field_3...`` to only retrieve the information you are interested in:


.. code-block:: python

    import requests

    # retrieve all file metadata
    response = requests.get('https://__METAX_ENV_DOMAIN__/rest/datasets/abc123/files')
    assert response.status_code == 200, response.content

    # retrieve only specified fields from file metadata
    response = requests.get('https://__METAX_ENV_DOMAIN__/rest/datasets/abc123/files?file_fields=identifier,file_path')
    assert response.status_code == 200, response.content


The second way is by using the same API as is used to generally browse the files of a project (see :ref:`rst-browsing-files`). Browsing the files of a dataset works the same way, except that an additional query parameter ``cr_identifier=<dataset_identifer>`` should be provided, in order to retrieve only those files and directories, which are included in the specified dataset.

Example:


.. code-block:: python

    import requests

    response = requests.get('https://__METAX_ENV_DOMAIN__/rest/directories/dir123/files?cr_identifier=abc123')
    assert response.status_code == 200, response.content


.. hint:: Etsin, a Fairdata service, provides a nice graphical UI for browsing files of published datasets.


.. note:: When browsing the files of a dataset, authentication with the API is not required, since if a dataset is retrievable from the API, it means it has been published, and its files are now public information.


Using reference data
^^^^^^^^^^^^^^^^^^^^^

Modifying field ``research_dataset`` to contain data that depends on reference data. Below example assumes an existing bare minimum dataset, to which a directory of files is being added. The directory-object has a mandatory field called ``use_category``, which requires using a value from reference data in its ``identifier`` field:


.. code-block:: python

    import requests

    headers = { 'Authorization': 'Bearer abc.def.ghi' }
    response = requests.get('https://__METAX_ENV_DOMAIN__/rest/datasets/abc123', headers=headers)
    assert response.status_code == 200, response.content

    modified_data = response.json()
    modified_data['research_dataset']['directories'] = [
        {
            "title": "Directory Title",
            "identifier": "5105ab9839f63a909893183c14f9e113",
            "description": "What is this directory about",
            "use_category": {
                # the value to the below field is from reference data
                "identifier": "http://uri.suomi.fi/codelist/fairdata/use_category/code/source",
            }
        }
    ]

    response = requests.put('https://__METAX_ENV_DOMAIN__/rest/datasets/abc123', json=modified_data, headers=headers)
    assert response.status_code == 200, response.content

When the dataset is updated, some fields inside the field ``use_category`` will have been populated by Metax according to the used reference data. The value used in the example above is the value of ``uri`` field from one of the objects in the following list: https://__METAX_ENV_DOMAIN__/es/reference_data/use_category/_search?pretty.

For more information about reference data, see :doc:`reference_data`.
