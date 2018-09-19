
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

    dataset_data = {
        "data_catalog": "urn:nbn:fi:att:data-catalog-att",
        "research_dataset": {
            "title": {
                "en": "Test Dataset Title"
            },
            "description": {
                "en": "A descriptive description describing the contents of this dataset. Must be descriptive."
            },
            "creator": [
                {
                    "name": "Teppo Testaaja",
                    "@type": "Person",
                    "member_of": {
                        "name": {
                            "fi": "Mysteeriorganisaatio"
                        },
                        "@type": "Organization"
                    }
                }
            ],
            "curator": [
                {
                    "name": {
                        "und": "School Services, BIZ"
                    },
                    "@type": "Organization",
                    "identifier": "http://purl.org/att/es/organization_data/organization/organization_10076-E700"
                }
            ],
            "language":[{
                "title": { "en": "en" },
                "identifier": "http://lexvo.org/id/iso639-3/aar"
            }],
            "access_rights": {
                "access_type": {
                    "identifier": "http://purl.org/att/es/reference_data/access_type/access_type_open_access"
                },
                "restriction_grounds": {
                    "identifier": "http://purl.org/att/es/reference_data/restriction_grounds/restriction_grounds_1"
                }
            }
        }
    }

    token = 'paste your token here'

    headers = { 'Authorization': 'Bearer %s' % token }
    response = requests.post('https://metax-test.csc.fi/rest/datasets', json=dataset_data, headers=headers)
    assert response.status_code == 201, response.content
    print('I have created a dataset, and its identifier is: %s' % response.json()['identifier'])


2) Log into https://metax-test.csc.fi/secure
3) Copy your token from the presented web page and use it in the script as the value for variable ``token``.
4) Execute the script.
5) You should now have a published dataset, and you should be able to find it in the Fairdata service Etsin by using the identifier printed by the script in the following url: ``https://etsin-test.fairdata.fi/dataset/<identifier>``

For more involved examples, see the examples sections in topics :doc:`datasets` and :doc:`files`, or start browsing the various sections of the documentation to get to know what's what.
