
Reference Data
===============

For some API's - such as ``/rest/datasets`` and ``/rest/datacatalogs`` - when creating or updating objects, values of some fields are checked against pre-defined reference data for valid values. The complete reference data, as provided by ElasticSearch, can be browsed here https://metax-test.csc.fi/es/.

When a field's value is validated, the ``uri`` field and the shorthand ``code`` field are accepted values. For examples what the values of those fields might look like, take a look at this reference data list for ``use_category``: https://metax-test.csc.fi/es/reference_data/use_category/_search?pretty. When a value is valid, the label for that object (usually known as ``pref_label`` or ``title`` in the object) is also copied from the reference data to the object being validated, overwriting its ``label`` value if it existed.

Any errors in reference data validation are returned from the API and displayed as all other errors.

The reference data index is updated nightly.



Querying Reference Data
------------------------

To get started right away, here are some examples for how to get something out of the reference data. For more advanced queries, such as search from a specific index and/or specific type, refer to ElasticSearch documentation for how to build search queries.

The below examples can be tried out by pointing your web browser to the presented urls. If you are reading the reference data for machine-reading purposes, you can leave out the ``pretty=true`` query parameter for unnecessary formatting of the output.

* Get data from a specific index https://metax-test.csc.fi/es/reference_data/field_of_science/_search?pretty

* Get data from a specific index, increase retrieved results size https://metax-test.csc.fi/es/reference_data/field_of_science/_search?pretty&size=100

    * Note, ``size=10000`` is max for this kind of request, so if there are more search results, refer to ElasticSearch scroll API

Additionally, with a little bit of help from UNIX tools, a handy command to list all available indexes and types:

* ``curl -X GET https://metax-test.csc.fi/es/_mapping | jq 'to_entries | .[] | {(.key): .value.mappings | keys}'``



When is use of reference data required in Metax?
-------------------------------------------------

Thats a very good question

.. warning:: explain
