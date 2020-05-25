
Files API
==========


General
--------


The ``/rest/files`` API supports creating, retrieving, updating, and deleting files. The file and directory data model visualization can be found here https://tietomallit.suomi.fi/model/mfs.

Write-operations to the ``/rest/files`` API is generally limited only to Fairdata services. In practice, new file metadata only appears to Metax as a result of freezing files in the Fairdata IDA service, or during some PAS processes.

End users will only be able to browse file metadata of projects where they are a member, end edit a limited set of metadata fields. Details about browsing files using the Metax API can be found later in this document :ref:`here <rst-browsing-files>`, and in swagger.



File Storages and Projects
---------------------------

A file metadata entry in Metax always belongs to a File Storage, and to a Project. A file storage can be for example the Fairdata IDA service.

A Project in this documentation generally refers to a project in IDA. For end users, browsing files in Metax, and associating files from a certain project with a dataset, and publishing the dataset, requires membership in that project. Read more about the IDA service, and how to become an IDA user at https://www.fairdata.fi/en/ida/.



File hierarchy
---------------

When sending a list of files to ``POST /rest/files``, a file/directory hierarchy is automatically created based on the file paths, and files are assigned to their parent directories. When retrieving a file (or a directory), its parent directory information is stored in the field ``parent_directory``.



End User Editable File Fields
------------------------------

End Users may edit the following file metadata fields using the API:

* ``file_characteristics``



Browsing files in Metax
------------------------

Note that browsing files using Metax API requires authentication in order to verify project membership. Once a file has been used in a published dataset, the file metadata will be free to browse for everybody (by using the approriate API).

This is just a quick overview, below code examples include some use of them, and other details can be found in swagger.


**Browse all file metadata of frozen IDA files (requires authentication)**


* ``GET /rest/directories/<pid>`` Get details of a single directory. Returned object does not contain files and sub-directories.
* ``GET /rest/directories/<pid>/files`` Get contents of a directory. Returns only directories and files included in the directory, not the directory itself (the directory designated by ``<pid>``). Returns immediate child directories and files, does not return files recursively from all sub directories.
* ``GET /rest/directories/root?project=project_identifier`` Retrieve the root directory of a project. Contains the directory itself, and the sub-directories and files contained by the directory.


**Browse file metadata in published datasets (no authentication)**


* ``GET /rest/directories/<pid>/files?cr_identifier=myidentifier`` Returns the files and directories that have been used in a specific published dataset (referred to by the parameter ``?cr_identifier=myidentifier``).
* ``GET /rest/datasets/<pid>/files`` Retrieve a flat list of all files associated with the dataset.

In the public browse API's, a dataset's access restrictions or embargoes may apply, and only limited metadata may be returned. Authentication for these public API's is optional, but by authenticating access restrictions may be lifted, for example due to ownership of the published dataset, etc.


.. _rst-files-reference-data:

Reference data guide
---------------------

File metadata only utilizes reference data when describing the field ``file.file_characteristics``, and more specifically, fields ``file_format`` and ``format_version`` inside that field. The related reference data can be browsed here https://__METAX_ENV_DOMAIN__/es/reference_data/file_format_version/_search?pretty=true.

For additional reference, the file schema visualization can be found here https://tietomallit.suomi.fi/model/mfs.


**Selecting file_format and format_version from reference data**


First choose a value for field ``file.file_characteristics.file_format`` from the list of ``input_file_format`` values in the reference data. Then, select a value for field ``file.file_characteristics.format_version`` from one of the ``output_format_version`` values that matches with the previously selected ``input_file_format``.

Example:

Let's browse the file_format_version reference data at https://__METAX_ENV_DOMAIN__/es/reference_data/file_format_version/_search?pretty=true. Let' pick an entry where the reference data field ``input_file_format`` equals "application/vnd.oasis.opendocument.text". This will be the value for field ``file.file_characteristics.file_format``. In order to pick a version for the selected format, browse the reference data again, but only searching results where ``input_file_format`` is the same as we chose previously. There will be (possibly) multiple results. The reference data query for that is:

``https://__METAX_ENV_DOMAIN__/es/reference_data/file_format_version/_search?pretty=true&q=input_file_format:application\/vnd\.oasis\.opendocument\.text``

Where the relevant addition is ``&q=input_file_format:application\/vnd\.oasis\.opendocument\.text``, where the ``"/"`` and ``"."`` characters have been escaped by a leading ``"\"`` character. Not nearly as many results now! The different version numbers can be seen in the field ``output_format_version``. Pick on that fancies you, and use that as the value for field ``file.file_characteristics.format_version``.



Examples
---------



Creating files
^^^^^^^^^^^^^^^

Example payload to create a file in Metax (``POST /rest/files``).

.. important::

    The possibility to create new file metadata entries in Metax is reserved for selected Fairdata services only.

.. code-block:: python

    {
        "identifier": "abc123",
        "file_name": "file.pdf",
        "file_path": "/some/file/path/file.pdf",
        "file_uploaded": "2017-09-27T12:38:18.700Z",
        "file_modified": "2017-09-27T12:38:18.700Z",
        "file_frozen": "2017-09-27T12:38:18.700Z",
        "file_format": "string",
        "byte_size": 1024,
        "file_storage": "urn:nbn:fi:att:file-storage-ida",
        "project_identifier": "string",
        "checksum": {
            "value": "string",
            "algorithm": "md5",
            "checked": "2017-09-27T12:38:18.701Z"
        },
        "open_access": false,
        "user_created": "string",
        "service_created": "string"
    }



.. _rst-browsing-files:

Browsing files
^^^^^^^^^^^^^^^

To begin browsing the files of a project, you will need one of the following information to be able to reach a directory to get started:

* The directory's identifier (access any directory)
* ...or the directory's project (access the root directory in a project)
* ...or the directory's project and path (access any directory in a project)

First, lets look what the contents of a single directory might look like in the first place.


.. code-block:: python

    import requests
    response = requests.get('https://__METAX_ENV_DOMAIN__/rest/directories/5105ab9839f63a909893183c14f9e119')
    print(response.json())


Contents could look something like below:


.. code-block:: python

    {
        "id": 441,
        "byte_size": 442778,
        "directory_modified": "2017-06-27T13:07:22+03:00",
        "directory_name": "init",
        "directory_path": "/project550/research/2018/data/init",
        "file_count": 264,
        "identifier": "5105ab9839f63a909893183c14f9e119",
        "parent_directory": {
            "id": 398,
            "identifier": "5105ab98398475109893183c14f9e119"
        },
        "project_identifier": "project550",
        "date_modified": "2017-06-27T13:07:22+03:00",
        "date_created": "2017-05-23T13:07:22+03:00",
        "service_created": "metax"
    }


When browsing files using the ``/rest/directories`` API, the ``identifier`` field will help in browsing directories further down the directory tree, while the field ``parent_directory`` can be used to browse directories up.


**List contents by directory identifier**


.. code-block:: python

    import requests

    response = requests.get('https://__METAX_ENV_DOMAIN__/rest/directories/dir123/files')
    assert response.status_code == 200, response.content


The responses from the directory browsing API generally look like the following, where either of the ``directories`` or ``files`` fields may be empty:


.. code-block:: python


    {
        "directories": [
            { directory object ... },
            { directory object ... },
        ],
        "files": [
            { file object ... },
            { file object ... },
            { file object ... },
            { file object ... },
        ]
    }


The ``/rest/directories`` API can be further augmented by using various query parameters. Refer to Swagger doc for details.


**Find project root directory**


Shows contents of the directory, as if ``GET /rest/directories/<pid>/files`` was used.


.. code-block:: python

    import requests
    response = requests.get('https://__METAX_ENV_DOMAIN__/rest/directories/root?project=<project_identifier>')


**Find directory by project and path**


Shows contents of the directory, as if ``GET /rest/directories/<pid>/files`` was used.


.. code-block:: python

    import requests
    response = requests.get('https://__METAX_ENV_DOMAIN__/rest/directories/files?project=<projcet_identifier>&path=/path/to/dir')
