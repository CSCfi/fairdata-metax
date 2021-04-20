import json

from metax_api.settings import env

with open(env("API_USERS_PATH")) as users_file:
    API_USERS = json.load(users_file)

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