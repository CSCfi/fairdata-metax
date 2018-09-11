
Metax API
==========

Metax is a metadata storage for Finnish research data. The core of the data is based on metadata from other Fairdata services (such as IDA and PAS), but also user-created metadata (by using Qvain, or other means), and metadata harvested from external sources. Read more at https://www.fairdata.fi and https://www.fairdata.fi/metax/.

Metax does not have a graphical UI. Most of the API's provided by Metax API are accessible only to other Fairdata services, and the main method for interacting with Metax should be by using those services. For advanced end users, Metax API can be accessed directly by using special tokens for authentication. See :doc:`end_users` for more information.

Any Python code examples in the API documentation are written using Python version 3, and most examples utilize the excellent ``requests`` library.

.. warning:: todo overview of all the different APIs and their purposes?
