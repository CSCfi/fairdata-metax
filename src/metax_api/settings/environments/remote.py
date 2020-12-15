from metax_api.settings import env

API_USERS = [
    {
        "password": env("METAX_USER_PASSWORD"),
        "username": "metax"
    },
    {
        "password": env("QVAIN_USER_PASSWORD"),
        "username": "qvain"
    },
    {
        "password": env("IDA_USER_PASSWORD"),
        "username": "ida"
    },
    {
        "password": env("TPAS_USER_PASSWORD"),
        "username": "tpas"
    },
    {
        "password": env("ETSIN_USER_PASSWORD"),
        "username": "etsin"
    },
    {
        "password": env("FDS_USER_PASSWORD"),
        "username": "fds"
    },
    {
        "password": env("QVAIN_LIGHT_USER_PASSWORD"),
        "username": "qvain-light"
    },
    {
        "password": env("QVAIN_JORI_USER_PASSWORD"),
        "username": "qvain-jori"
    },
    {
        "password": env("TTV_USER_PASSWORD"),
        "username": "ttv"
    },
    {
        "password": env("DOWNLOAD_USER_PASSWORD"),
        "username": "download"
    }
]
CONSUMERS=[
    {
        "is_test_user": True,
        "name": "testaaja",
        "password": env("TESTAAJA_CONSUMER_PASSWORD"),
        "permissions": {
            "conf": "^testaaja-.*$",
            "read": "^(datasets|testaaja-.*)$",
            "write": "^testaaja-.*$"
        },
        "vhost": "metax"
    },
    {
        "is_test_user": False,
        "name": "etsin",
        "password": env("ETSIN_CONSUMER_PASSWORD"),
        "permissions": {
            "conf": "^etsin-.*$",
            "read": "^(datasets|etsin-.*)$",
            "write": "^etsin-.*$"
        },
        "vhost": "metax"
    },
    {
        "is_test_user": False,
        "name": "ttv",
        "password": env("TTV_CONSUMER_PASSWORD"),
        "permissions": {
            "conf": "^ttv-.*$",
            "read": "^(TTV-datasets|ttv-.*)$",
            "write": "^ttv-.*$"
        },
        "vhost": "ttv"
    }
]
END_USER_ALLOWED_DATA_CATALOGS= [
    "urn:nbn:fi:att:data-catalog-ida",
    "urn:nbn:fi:att:data-catalog-att",
    "urn:nbn:fi:att:data-catalog-legacy",
    "urn:nbn:fi:att:data-catalog-pas",
    "urn:nbn:fi:att:data-catalog-dft"
]

API_ACCESS = {
    "rest": {
        "apierrors": {
            "delete": [
                "metax"
            ],
            "read": [
                "all"
            ]
        },
        "contracts": {
            "create": [
                "all"
            ],
            "delete": [
                "all"
            ],
            "read": [
                "all"
            ],
            "update": [
                "all"
            ]
        },
        "datacatalogs": {
            "create": [
                "all"
            ],
            "delete": [
                "all"
            ],
            "read": [
                "all"
            ],
            "update": [
                "all"
            ]
        },
        "datasets": {
            "create": [
                "all"
            ],
            "delete": [
                "all"
            ],
            "read": [
                "all"
            ],
            "update": [
                "all"
            ]
        },
        "directories": {
            "create": [],
            "delete": [],
            "read": [
                "all"
            ],
            "update": []
        },
        "files": {
            "create": [
                "all"
            ],
            "delete": [
                "all"
            ],
            "read": [
                "all"
            ],
            "update": [
                "all"
            ]
        },
        "filestorages": {
            "create": [
                "all"
            ],
            "delete": [
                "all"
            ],
            "read": [
                "all"
            ],
            "update": [
                "all"
            ]
        },
        "schemas": {
            "create": [],
            "delete": [],
            "read": [
                "all"
            ],
            "update": []
        }
    },
    "rpc": {
        "datasets": {
            "change_cumulative_state": {
                "use": [
                    "all"
                ]
            },
            "create_draft": {
                "use": [
                    "all"
                ]
            },
            "create_new_version": {
                "use": [
                    "all"
                ]
            },
            "fix_deprecated": {
                "use": [
                    "all"
                ]
            },
            "get_minimal_dataset_template": {
                "use": [
                    "all"
                ]
            },
            "merge_draft": {
                "use": [
                    "all"
                ]
            },
            "publish_dataset": {
                "use": [
                    "all"
                ]
            },
            "refresh_directory_content": {
                "use": [
                    "all"
                ]
            },
            "set_preservation_identifier": {
                "use": [
                    "all"
                ]
            }
        },
        "elasticsearchs": {
            "map_refdata": {
                "use": [
                    "all"
                ]
            }
        },
        "files": {
            "delete_project": {
                "use": [
                    "all"
                ]
            },
            "flush_project": {
                "use": [
                    "all"
                ]
            }
        },
        "statistics": {
            "all_datasets_cumulative": {
                "use": [
                    "all"
                ]
            },
            "catalog_datasets_cumulative": {
                "use": [
                    "all"
                ]
            },
            "count_datasets": {
                "use": [
                    "all"
                ]
            },
            "deprecated_datasets_cumulative": {
                "use": [
                    "all"
                ]
            },
            "end_user_datasets_cumulative": {
                "use": [
                    "all"
                ]
            },
            "harvested_datasets_cumulative": {
                "use": [
                    "all"
                ]
            },
            "organization_datasets_cumulative": {
                "use": [
                    "all"
                ]
            },
            "unused_files": {
                "use": [
                    "all"
                ]
            }
        }
    }
}