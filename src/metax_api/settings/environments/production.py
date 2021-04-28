from metax_api.settings.environments.stable import api_permissions, prepare_perm_values

api_permissions.rpc.files.flush_project.use.clear()

API_ACCESS = prepare_perm_values(api_permissions.to_dict())