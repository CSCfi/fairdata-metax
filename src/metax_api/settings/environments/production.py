from metax_api.settings.environments.stable import api_permissions, prepare_perm_values
from metax_api.settings.environments.staging import API_USERS  # noqa: F401

api_permissions.rpc.datasets.flush_user_data.use.clear()

api_permissions.rpc.files.flush_project.use.clear()

API_ACCESS = prepare_perm_values(api_permissions.to_dict())
