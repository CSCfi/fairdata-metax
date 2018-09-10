
End User Access
========================



Authentication
---------------

End Users are able to interact with certain Metax apis by utilizing Bearer Tokens for request authentication.

You can gain a Token by directing your web browser to the ``/secure`` endpoint, where you will be prompted to authenticate. Once authenticated, you are shown an access token in an encoded form, and information how long it will be valid. Copy the encoded token to a HTTP Authorization header, so that the end result looks like the following:

    Bearer eyJraWQiOiJ.0ZXN0a2V5I.iwiYWxnIjoi

Real tokens in encoded form are very long, but above is an example what the HTTP Authorization header should look like, if the token value was very short. Now the HTTP Authorization header can be used in a request to Metax API. Below is an example how the header should be used when utilizing Python and the requests-library.

.. code-block:: python

    import requests

    headers = { 'Authorization': 'Bearer eyJraWQiOiJ.0ZXN0a2V5I.iwiYWxnIjoi' }

    # retrieve a file by its identifier. using the /files api, its possible to only retrieve metadata
    # of files where you are a member the file's project.
    response = requests.get('https://metax-test.csc.fi/rest/files/abc123def', headers=headers)

    print(response.json())

