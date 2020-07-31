
Metax API
==========

This documentation contains information about how to use the APIs in Metax. If you more or less know what you are doing and want to get your hands dirty right away, head over to :doc:`quick_start`. Otherwise, keep reading.

Metax is a metadata storage for Finnish research data. The core of the data is based on metadata from other Fairdata services (such as IDA and PAS), but also user-created metadata (by using Qvain, or other means), and metadata harvested from external sources. Read more at https://www.fairdata.fi/en/ and https://www.fairdata.fi/en/metax/.

Metax does not have a graphical UI. Most of the API's provided by Metax API are accessible only to other Fairdata services, and the main method for interacting with Metax should be by using those services. For advanced end users, Metax API can be accessed directly by using special tokens for authentication. See :doc:`end_users` for more information.

Any Python code examples in the API documentation are written using Python version 3, and most examples utilize the excellent ``requests`` library.


**Metax APIs**

Below is a rough outline what type of APIs Metax is currently providing. The rest of the documentation will cover how to interact with them.

+--------------------------------------+---------------------------------------------------------------------------------------------------------------------------------------------+
| API                                  | Description                                                                                                                                 |
+--------------------------------------+---------------------------------------------------------------------------------------------------------------------------------------------+
| https://__METAX_ENV_DOMAIN__/rest/v2 | The main API to interact with most resources inside Metax, such as datasets, files, and data catalogs.                                      |
+--------------------------------------+---------------------------------------------------------------------------------------------------------------------------------------------+
| https://__METAX_ENV_DOMAIN__/rpc/v2  | A Remote Procedure Call -API, to execute various actions in Metax, or retrieve data that otherwise does not fit into the REST api.          |
+--------------------------------------+---------------------------------------------------------------------------------------------------------------------------------------------+
| https://__METAX_ENV_DOMAIN__/oaipmh  | Implements the OAI-PMH specification. The specification defines a way to harvest filtered sets of data (datasets) in an incremental manner. |
+--------------------------------------+---------------------------------------------------------------------------------------------------------------------------------------------+
