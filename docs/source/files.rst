
Files API
==========


General
--------


The ``/files`` api supports creating, retrieving, updating, and deleting files.

* Creating files is currently possible only by utilizing the IDA service.
* Retrieving/browsing files is possible by utilizing services such as Qvain and Etsin. End Users can browse files of projects where they are a member. Details about browsing files using the Metax API can be found later in this document, and in swagger.
* Updating files is currently possible only by utilzing the PAS service.
* Deleting files is currently possible only by utilizing the IDA service.


File hierarchy
---------------

When sending a list of files to ``POST /files``, a file/directory hierarchy is automatically created based on the file paths, and files are assigned to their parent directories. When retrieving a file (or a directory), its parent directory information is stored in the field ``parent_directory``.



Browsing files in Metax
------------------------


**APIs of interest for browsing files**

* ``GET /directories/<pid>`` Get details of a single directory. Returned object does not contain files and sub-directories.
* ``GET /directories/<pid>/files`` Get contents of a directory. Returns only directories and files included in the directory, not the directory itself (the directory designated by ``<pid>``). Returns immediate child directories and files, does not return files recursively from all sub directories.
* ``GET /directories/<pid>/files?cr_identifier=myidentifier`` Same as above, but only returns the files and dirs that existed at the time of assigning the dir in that dataset
* ``GET /directories/root?project=project_name`` Retrieve the root directory of a project. Contains the directory itself, and the sub-directories and files contained by the directory.
* ``GET /datasets/<pid>/files`` Retrieve a flat list of all files associated with the dataset.

This is just a quick overview, details for the apis can be found in swagger.


Examples
---------


Payload to create a file in Metax (``POST /files``).

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