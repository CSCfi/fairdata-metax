from metax_api.settings.environments.remote import *

API_ACCESS = {
    "rest": {
        "apierrors": {
            "delete": [
                "metax"
            ],
            "read": [
                "metax"
            ]
        },
        "contracts": {
            "create": [
                "metax",
                "tpas"
            ],
            "delete": [
                "metax",
                "tpas"
            ],
            "read": [
                "metax",
                "tpas"
            ],
            "update": [
                "metax",
                "tpas"
            ]
        },
        "datacatalogs": {
            "create": [
                "metax",
                "etsin"
            ],

            "delete": [
                "metax",
                "etsin"
            ],
            "read": [
                "metax",
                "all"
            ],
            "update": [
                "metax",
                "etsin"
            ]
        },
        "datasets": {
            "create": [
                "metax",
                "ida",
                "qvain",
                "qvain-light",
                "etsin",
                "tpas",
                "jyu",
                "endusers"
            ],
            "delete": [
                "metax",
                "ida",
                "qvain",
                "qvain-light",
                "etsin",
                "tpas",
                "jyu",
                "endusers"
            ],

            "read": [
                "all"
            ],
            "update": [
                "metax",
                "ida",
                "qvain",
                "qvain-light",
                "etsin",
                "tpas",
                "jyu",
                "endusers"
            ]
        },
        "directories": {
            "read": [
                "metax",
                "ida",
                "qvain",
                "qvain-light",
                "etsin",
                "tpas",
                "fds",
                "endusers"
            ]
        },
        "files": {
            "create": [
                "metax",
                "ida",
                "tpas"
            ],
            "delete": [
                "metax",
                "ida",
                "tpas"
            ],

            "read": [
                "metax",
                "ida",
                "fds",
                "tpas",
                "qvain",
                "qvain-light",
                "endusers"
            ],
            "update": [
                "metax",
                "ida",
                "tpas",
                "qvain",
                "qvain-light",
                "endusers"
            ]
        },
        "filestorages": {
            "create": [
                "metax"
            ],
            "delete": [
                "metax"
            ],
            "read": [
                "metax"
            ],
            "update": [
                "metax"
                "metax",
                "ida"
            ],
            "delete": [
                "metax",
                "ida"
            ],
            "read": [
                "metax",
                "ida"
            ],
            "update": [
                "metax",
                "ida"
            ]
        },
        "schemas": {
            "read": [
                "all"
            ]
        }
    },
    "rpc": {
        "datasets": {
            "change_cumulative_state": {
                "use": [
                    "metax",
                    "qvain",
                    "qvain-light",
                    "endusers"
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
                    "metax",
                    "qvain",
                    "qvain-light",
                    "endusers"
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
                    "metax",
                    "qvain",
                    "qvain-light",
                    "endusers"
                ]
            },
            "set_preservation_identifier": {
                "use": [
                    "metax",
                    "tpas"
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
                    "metax",
                    "ida",
                    "tpas"
                ]
            },
            "flush_project": {
                "use": [
                    "metax",
                    "ida",
                    "tpas"
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