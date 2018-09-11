
Files API
==========


General
--------


The ``/rest/files`` API supports creating, retrieving, updating, and deleting files.

* Creating files is currently possible only by utilizing the IDA service.
* Retrieving/browsing files is possible by utilizing services such as Qvain and Etsin. End Users can browse files of projects where they are a member. Details about browsing files using the Metax API can be found later in this document, and in swagger.
* Updating files is currently possible only by utilzing the PAS service.
* Deleting files is currently possible only by utilizing the IDA service.


File hierarchy
---------------

When sending a list of files to ``POST /rest/files``, a file/directory hierarchy is automatically created based on the file paths, and files are assigned to their parent directories. When retrieving a file (or a directory), its parent directory information is stored in the field ``parent_directory``.



Browsing files in Metax
------------------------


**APIs of interest for browsing files**

* ``GET /rest/directories/<pid>`` Get details of a single directory. Returned object does not contain files and sub-directories.
* ``GET /rest/directories/<pid>/files`` Get contents of a directory. Returns only directories and files included in the directory, not the directory itself (the directory designated by ``<pid>``). Returns immediate child directories and files, does not return files recursively from all sub directories.
* ``GET /rest/directories/<pid>/files?cr_identifier=myidentifier`` Same as above, but only returns the files and dirs that existed at the time of assigning the dir in that dataset
* ``GET /rest/directories/root?project=project_name`` Retrieve the root directory of a project. Contains the directory itself, and the sub-directories and files contained by the directory.
* ``GET /rest/datasets/<pid>/files`` Retrieve a flat list of all files associated with the dataset.

This is just a quick overview, below code examples include some use of them, and other details can be found in swagger.


Examples
---------



Creating files
^^^^^^^^^^^^^^^

Example payload to create a file in Metax (``POST /rest/files``).

.. code-block:: python

    {
        "identifier": "some:unique:identifier:1",
        "file_name": "file.pdf",
        "file_path": "/some/file/path",
        "replication_path": "string",
        "file_uploaded": "2017-09-27T12:38:18.700Z",
        "file_modified": "2017-09-27T12:38:18.700Z",
        "file_frozen": "2017-09-27T12:38:18.700Z",
        "file_format": "string",
        "byte_size": 1024,
        "file_storage": 1,
        "project_identifier": "string",
        "checksum": {
            "value": "string",
            "algorithm": "md5",
            "checked": "2017-09-27T12:38:18.701Z"
        },
        "open_access": true,
        "user_modified": "string",
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
    response = requests.get('https://metax-test.csc.fi/rest/directories/5105ab9839f63a909893183c14f9e119')
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

    response = requests.delete('https://metax-test.csc.fi/rest/directories/dir123/files')
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
    response = requests.get('https://metax-test.csc.fi/rest/directories/root?project=<project_identifier>')


**Find directory by project and path**


Shows contents of the directory, as if ``GET /rest/directories/<pid>/files`` was used.


.. code-block:: python

    import requests
    response = requests.get('https://metax-test.csc.fi/rest/directories/files?project=<projcet_identifier>&path=/path/to/dir')
