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
    },
    {
        "password": env("JYU_USER_PASSWORD"),
        "username": "jyu"
    }
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