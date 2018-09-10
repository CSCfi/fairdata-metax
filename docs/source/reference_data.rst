
Reference Data
===============

For some API's (currently datasets and data catalogs), values of some fields are checked against specific reference data for valid values. The fields whose values are validated, are listed later on this page. The reference data currently on the test server can be browsed using ElasticSearch here https://metax-test.csc.fi/es/

When a field's value is validated, the uri and the shorthand code are accepted values. When a value is valid, the label for that object (usually known as ``pref_label`` or ``title`` in the object) is also copied from the reference data, to the object being validated, overwriting its ``label`` value if it existed.

Any errors in reference data validation are displayed as all other errors.
