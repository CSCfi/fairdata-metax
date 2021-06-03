
Reference Data
===============

"Reference Data" is a fancy name for "a list of known values". When creating new objects in Metax, or updating existing objects, values for some fields are checked against an approriate category in this pre-defined list of known values. For example, when setting the language of a dataset, "my cat" is not a valid value. Instead, a valid value must be chosen from the language reference data listed at https://__METAX_ENV_DOMAIN__/es/reference_data/language/_search?pretty=true (the link only shows a few values, since the list of languages is pretty huge, but it gives you a glimpse of what reference data in practice looks like).

Some other examples for reference data use include describing the purpose of associated files in a dataset (`Use Category <https://__METAX_ENV_DOMAIN__/es/reference_data/use_category/_search?pretty=true>`_), the manner how contributors were involved in a research (`Contributor Role <https://__METAX_ENV_DOMAIN__/es/reference_data/contributor_role/_search?pretty=true>`_), or telling which fields of science the research fits into (`Field of Science <https://__METAX_ENV_DOMAIN__/es/reference_data/field_of_science/_search?pretty=true>`_). The docs for each API has a section dedicated for reference data, which explains when use of reference data is required. For example for datasets, that section can be found here :ref:`rst-datasets-reference-data-v1`.

The Metax API documentation contains some examples for querying the reference data index, but since the index is a ElasticSearch database, it is best to refer to the official ElasticSearch documentation for how to build more complex search queries.



Reference Data Indexes
-----------------------

Below table contains a full list of the different indexes stored in the reference data.

+----------------------------------------------------------------------------------------+
| Url to browse reference data                                                           |
+----------------------------------------------------------------------------------------+
| https://__METAX_ENV_DOMAIN__/es/reference_data/access_type/_search?pretty=true         | 
+----------------------------------------------------------------------------------------+
| https://__METAX_ENV_DOMAIN__/es/reference_data/contributor_role/_search?pretty=true    |
+----------------------------------------------------------------------------------------+
| https://__METAX_ENV_DOMAIN__/es/reference_data/contributor_type/_search?pretty=true    |
+----------------------------------------------------------------------------------------+
| https://__METAX_ENV_DOMAIN__/es/reference_data/event_outcome/_search?pretty=true       |
+----------------------------------------------------------------------------------------+
| https://__METAX_ENV_DOMAIN__/es/reference_data/field_of_science/_search?pretty=true    |
+----------------------------------------------------------------------------------------+
| https://__METAX_ENV_DOMAIN__/es/reference_data/file_format_version/_search?pretty=true |
+----------------------------------------------------------------------------------------+
| https://__METAX_ENV_DOMAIN__/es/reference_data/file_type/_search?pretty=true           |
+----------------------------------------------------------------------------------------+
| https://__METAX_ENV_DOMAIN__/es/reference_data/funder_type/_search?pretty=true         |
+----------------------------------------------------------------------------------------+
| https://__METAX_ENV_DOMAIN__/es/reference_data/identifier_type/_search?pretty=true     |
+----------------------------------------------------------------------------------------+
| https://__METAX_ENV_DOMAIN__/es/reference_data/keyword/_search?pretty=true             |
+----------------------------------------------------------------------------------------+
| https://__METAX_ENV_DOMAIN__/es/reference_data/language/_search?pretty=true            |
+----------------------------------------------------------------------------------------+
| https://__METAX_ENV_DOMAIN__/es/reference_data/license/_search?pretty=true             |
+----------------------------------------------------------------------------------------+
| https://__METAX_ENV_DOMAIN__/es/reference_data/lifecycle_event/_search?pretty=true     |
+----------------------------------------------------------------------------------------+
| https://__METAX_ENV_DOMAIN__/es/reference_data/location/_search?pretty=true            |
+----------------------------------------------------------------------------------------+
| https://__METAX_ENV_DOMAIN__/es/reference_data/mime_type/_search?pretty=true           |
+----------------------------------------------------------------------------------------+
| https://__METAX_ENV_DOMAIN__/es/reference_data/preservation_event/_search?pretty=true  |
+----------------------------------------------------------------------------------------+
| https://__METAX_ENV_DOMAIN__/es/reference_data/relation_type/_search?pretty=true       |
+----------------------------------------------------------------------------------------+
| https://__METAX_ENV_DOMAIN__/es/reference_data/research_infra/_search?pretty=true      |
+----------------------------------------------------------------------------------------+
| https://__METAX_ENV_DOMAIN__/es/reference_data/resource_type/_search?pretty=true       |
+----------------------------------------------------------------------------------------+
| https://__METAX_ENV_DOMAIN__/es/reference_data/restriction_grounds/_search?pretty=true |
+----------------------------------------------------------------------------------------+
| https://__METAX_ENV_DOMAIN__/es/reference_data/use_category/_search?pretty=true        |
+----------------------------------------------------------------------------------------+
| https://__METAX_ENV_DOMAIN__/es/organization_data/organization/_search?pretty=true     |
+----------------------------------------------------------------------------------------+



How to use reference data when uploading data to Metax?
--------------------------------------------------------

Let's pick a language from the available reference data, and use it in the ``language`` field of a dataset. Browse the language reference data at https://__METAX_ENV_DOMAIN__/es/reference_data/language/_search?pretty=true, and search for "English language". You probably won't find it on the first page of the reference data, but that's ok. In the language reference data, the result for the English language looks like the following:


.. code-block:: python

    {
        "_index" : "reference_data",
        "_type" : "language",
        "_id" : "language_eng",
        "_score" : 1.0,
        "_source" : {
            "id" : "language_eng",
            "code" : "eng",
            "type" : "language",
            "uri" : "http://lexvo.org/id/iso639-3/eng",
            "wkt" : "",
            "input_file_format" : "",
            "output_format_version" : "",
            "label" : {
                "ti" : "እንግሊዝኛ",
                "el" : "Αγγλική γλώσσα",
                # ...
                # <potentially a LONG list of translations listed here>
                # ...
                "fi" : "Englannin kieli",
                # ...
                # ...
                # ...
                "lbe" : "Ингилис маз",
                "shi" : "ⵜⴰⵏⴳⵍⵉⵣⵜ",
                "und" : "Englannin kieli"
            },
            "parent_ids" : [ ],
            "child_ids" : [ ],
            "has_children" : false,
            "same_as" : [ ],
            "internal_code" : "",
            "scheme" : "http://lexvo.org/id/"
        }
    }


In above, the values that should be used to refer to that particular object in the reference data, are the fields ``uri`` and ``code``. So, to use the English language in the field ``language`` in a dataset, we would do the following:


.. code-block:: python

    # ... other fields of research_dataset ...
    "language": [
            {
                # using the value of field ``uri`` here
                "identifier": "http://lexvo.org/id/iso639-3/eng"

                # also valid would be to use value of field ``code``:
                # "identifier": "eng"
            }
        ],
    # ... other fields of research_dataset...


After uploading the dataset to Metax, Metax will validate the provided value in language ``identifier``, and automatically populate the rest of the fields according to what was specified in that reference data object. When Metax returns a response, the language block will look like the following (note: only a selected few translations are picked to be populated, since there can be a really huge amount of translations for some language names):

.. code-block:: python

    # ... other fields of research_dataset ...
    "language": [
            {
                "title": {
                    "en": "English language",
                    "fi": "Englannin kieli",
                    "sv": "engelska",
                    "und": "Englannin kieli"
                },
                "identifier": "http://lexvo.org/id/iso639-3/eng"
            }
        ],
    # ... other fields of research_dataset ...


Most often, the field that will get populated from the reference data will be the label, usually known as ``pref_label`` or ``title`` in the object. If those fields had any values in place when uploading the data to Metax, it will get overwritten.



.. _rst-reference-data-query-examples-v1:

Querying Reference Data
------------------------

To get started right away, here are some examples for how to get something out of the reference data. For more advanced queries, such as search from a specific index and/or specific type, refer to official ElasticSearch documentation for how to build search queries.

The below examples can be tried out by pointing your web browser to the presented urls. If you are reading the reference data for machine-reading purposes, you can leave out the ``pretty=true`` query parameter for unnecessary formatting of the output.

* Get data from a specific index https://__METAX_ENV_DOMAIN__/es/reference_data/field_of_science/_search?pretty

* Get data from a specific index, increase retrieved results size https://__METAX_ENV_DOMAIN__/es/reference_data/field_of_science/_search?pretty&size=100

    * Note, ``size=10000`` is max for this kind of request, so if there are more search results, refer to ElasticSearch scroll API

Additionally, with a little bit of help from UNIX tools, a handy command to list all available indexes and types:

* ``curl -X GET https://__METAX_ENV_DOMAIN__/es/_mapping | jq 'to_entries | .[] | {(.key): .value.mappings | keys}'``



Reference data validation errors
---------------------------------

Any errors in reference data validation are returned from the API and displayed as all other errors.



How often is the reference data updated?
-----------------------------------------

The reference data index is updated nightly.

