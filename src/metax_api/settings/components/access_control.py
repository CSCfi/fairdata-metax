# specify read and write access of services per api, or if an api is readable by world
API_ACCESS = {
    "rest": {
        "apierrors": {
            "create": [
                "metax"
            ],
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
                "qvain",
                "etsin",
                "tpas",
                "endusers"
            ],
            "delete": [
                "metax",
                "qvain",
                "etsin",
                "tpas",
                "endusers"
            ],
            "read": [
                "all"
            ],
            "update": [
                "metax",
                "qvain",
                "etsin",
                "tpas",
                "endusers"
            ]
        },
        "directories": {
            "create": [],
            "delete": [],
            "read": [
                "metax",
                "qvain",
                "etsin",
                "tpas",
                "fds",
                "endusers"
            ],
            "update": []
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
                "endusers"
            ],
            "update": [
                "metax",
                "ida",
                "tpas",
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