
Quick Start
============



Fairdata Services
------------------

If you don't already have API credentials to Metax API, get in contact with Metax developers. After that, see the examples sections in topics :doc:`datasets`, :doc:`files`, or whatever API it is that you are going to interact with.



End Users
----------

No chit-chat get a dataset published into Metax and see it in the Fairdata service Etsin:

1) Copy the following Python code into a file:


.. code-block:: python

    import requests

    token = 'paste your token here'

    response = requests.get('https://__METAX_ENV_DOMAIN__/rpc/datasets/get_minimal_dataset_template?type=enduser')
    assert response.status_code == 200, response.content

    dataset_data = response.json()
    headers = { 'Authorization': 'Bearer %s' % token }
    response = requests.post('https://__METAX_ENV_DOMAIN__/rest/datasets', json=dataset_data, headers=headers)
    assert response.status_code == 201, response.content

    print('I have created a dataset, and its identifier is: %s' % response.json()['identifier'])


2) Log into https://__METAX_ENV_DOMAIN__/secure
3) Copy your token from the presented web page and use it in the script as the value for variable ``token``.
4) Execute the script.
5) You should now have a published dataset, and you should be able to find it in the Fairdata service Etsin by using the identifier printed by the script in the following url: ``https://etsin-test.fairdata.fi/dataset/<identifier>``

In reality you will probably want to create a dataset with a little bit more interesting data in it, but using the example dataset from API ``/rpc/datasets/get_minimal_dataset_template`` is a good starting point for your modifications. For more involved examples, see the examples sections in topics :doc:`datasets` and :doc:`files`, or start browsing the various sections of the documentation to get to know what's what.
