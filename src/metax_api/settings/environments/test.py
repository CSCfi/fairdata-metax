API_TEST_USER = {"username": "testuser", "password": "testuserpassword"}
API_METAX_USER = {"username": "metax", "password": "metaxpassword"}
API_AUTH_TEST_USER = {"username": "api_auth_user", "password": "password"}

API_EXT_USER = {"username": "external", "password": "externalpassword"}

API_TEST_USERS = [API_TEST_USER, API_METAX_USER, API_AUTH_TEST_USER, API_EXT_USER]

API_ACCESS = {
    "rest": {
        "apierrors": {
            "read": ["testuser", "metax"],
            "create": ["testuser", "metax"],
            "update": ["testuser", "metax"],
            "delete": ["testuser", "metax"],
        },
        "contracts": {
            "read": ["testuser", "metax"],
            "create": ["testuser", "metax"],
            "update": ["testuser", "metax"],
            "delete": ["testuser", "metax"],
        },
        "datacatalogs": {
            "read": ["all"],
            "create": ["testuser", "metax"],
            "update": ["testuser", "metax"],
            "delete": ["testuser", "metax"],
        },
        "datasets": {
            "read": ["all"],
            "create": ["testuser", "metax", "api_auth_user", "endusers", "external"],
            "update": ["testuser", "metax", "api_auth_user", "endusers", "external"],
            "delete": ["testuser", "metax", "api_auth_user", "endusers", "external"],
        },
        "directories": {
            "read": ["testuser", "metax", "endusers"],
        },
        "files": {
            "read": ["testuser", "metax", "api_auth_user", "endusers"],
            "create": ["testuser", "metax"],
            "update": ["testuser", "metax", "endusers"],
            "delete": ["testuser", "metax"],
        },
        "filestorages": {
            "read": ["testuser", "metax"],
            "create": ["testuser", "metax"],
            "update": ["testuser", "metax"],
            "delete": ["testuser", "metax"],
        },
        "schemas": {
            "read": ["all"],
        },
    },
    "rpc": {
        "datasets": {
            "change_cumulative_state": {"use": ["all"]},
            "get_minimal_dataset_template": {"use": ["all"]},
            "refresh_directory_content": {"use": ["all"]},
            "fix_deprecated": {"use": ["all"]},
            "set_preservation_identifier": {"use": ["metax", "tpas"]},
            "create_new_version": {"use": ["all"]},
            "publish_dataset": {"use": ["all"]},
            "create_draft": {"use": ["all"]},
            "merge_draft": {"use": ["all"]},
        },
        "files": {"delete_project": {"use": ["testuser", "metax"]}},
        "statistics": {
            "count_datasets": {"use": ["all"]},
            "all_datasets_cumulative": {"use": ["all"]},
            "catalog_datasets_cumulative": {"use": ["all"]},
            "end_user_datasets_cumulative": {"use": ["all"]},
            "harvested_datasets_cumulative": {"use": ["all"]},
            "deprecated_datasets_cumulative": {"use": ["all"]},
            "organization_datasets_cumulative": {"use": ["all"]},
            "unused_files": {"use": ["all"]},
        },
    },
}
for api, perms in API_ACCESS["rest"].items():
    perms["read"] = ["all"]
    perms["create"] = ["all"]
    perms["update"] = ["all"]
    perms["delete"] = ["all"]

for api, functions in API_ACCESS["rpc"].items():
    for function, perms in functions.items():
        perms["use"] = ["all"]

ADDITIONAL_USER_PROJECTS_PATH = "/tmp/user_projects.json"
