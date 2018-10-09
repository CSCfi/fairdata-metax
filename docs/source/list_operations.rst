
List operations
================



Retrieve operations (GET)
--------------------------

``GET`` to resource root, such as ``/rest/datasets`` or ``/rest/files``, retrieves a list of objects, as per usual REST convention. By default paging is forced to avoid time-consuming accidents. To disable paging, optional query parameter ``no_pagination=true`` can be passed to instead retrieve all records in a single request.



Pagination
^^^^^^^^^^^

Paging using query parameters limit and offset, example:

* https://__METAX_ENV_DOMAIN__/rest/datasets?limit=2&offset=3

Since we are using Django REST framework, the response provided for us looks like the following:

.. code-block:: python

    {
        "count": 12, # total count of objects available in the db
        "next": "http://__METAX_ENV_DOMAIN__/rest/datasets?limit=2&offset=5", # convenience link for next page
        "previous": "http://__METAX_ENV_DOMAIN__/rest/datasets?limit=2&offset=1", # convenience link for previous page
        "results": [ # the list of objects on the queried page
            object,
            ...
        ]
    }



Streaming
^^^^^^^^^^

An additional query parameter ``stream=true`` can be passed to stream the responses instead of downloading in a single file. In order to use streaming, ``no_pagination=true`` must also be passed. This option can only be used with list-apis.



Create operations (POST)
-------------------------

``POST`` requests generally accept both individual objects or a list of objects to create from for bulk creation. The response contains the created object or errors in case of failure:

.. code-block:: python

    # POST create single, response bodies

    # 201 created, the created object
    {
        'field': 'value',
        'other': 'etc'
    }

    # 400 bad request, an error with some field:
    {
        'preferred_identifier': ['already exists']
    }

    # 400 bad request, in case of a more general error:
    {
        'detail': ['error description']
    }


When a list was given, a list of successfully created objects is returned, and a list of errors for the failed ones:

.. code-block:: python

    # POST create list response bodies

    # 201 created, all successful
    {
        'success': [
            { 'object': object },
            { 'object': object }
        ],
        'failed': []
    }

    # 201 created, some successful, some failed. If even one operation was a success, the general status code is 201.
    {
        'success': [
            { 'object': object },
            { 'object': object }
        ],
        'failed': [
            { 'object': object, 'errors': {'some_field': ['message'], 'other_field': ['message']} },
            { 'object': object, 'errors': {'some_field': ['message'] } }
        ]
    }

    # 400 bad request, all failed. Only if all operations fail, the general status code will be 400.
    {
        'success': [],
        'failed': [
            { 'object': object, 'errors': {'some_field': ['message'], 'other_field': ['message']} },
            { 'object': object, 'errors': {'some_field': ['message'] } }
        ]
    }


In list create, if even one object was successfully created, return code will be 201. Only if all create operations have failed, return code will be 400.



Bulk update (PUT and PATCH)
----------------------------

Resource root urls such as ``/rest/datasets`` also accept ``PUT`` and ``PATCH`` requests for bulk update. Provide the parameter objects as usual, except wrapped inside a list.

For ``PATCH`` bulk update, the parameter object must also contain some field that can be used to identify the object being updated, because the url does not contain the identifier like it does when updating a single object. The field to use for that is, of course, the ``identifier`` field. For example when bulk updating files:

.. code-block:: python

    # PATCH list update, request body
    [
        {
            'identifier': 'some:identifier',
            'field_being_updated': 'value'
        },
        {
            'identifier': 'some:identifier2',
            'field_being_updated': 'value2'
        }
    ]


Return values are similar to bulk create, i.e. the response contains the keys success and failed, with updated objects and possible error descriptions found inside.



Bulk delete
------------

Only the API ``/rest/files`` currently supports bulk delete, and is reserver for Fairdata service use only.



Atomic Bulk Operations
-----------------------

Bulk create and update operations take an optional boolean parameter ``atomic``, which can be set to ensure that either all operations succeed in the request, or none at all. When using the parameter, if the request fails, there is an extra field ``detail`` in the response result (in addition to the usual fields ``success``, and ``failed``) which reminds that the failure occurred due to the ``atomic`` flag.

Example: Trying to create 10000 files by sending request to ``POST /rest/files``, and 10 file creations fail for whatever reason. By providing ``?atomic=true``, no files at all are created.
