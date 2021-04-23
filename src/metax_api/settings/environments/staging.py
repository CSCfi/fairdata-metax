from metax_api.settings.components.access_control import Role, api_permissions, prepare_perm_values

api_permissions.rest.apierrors.read = [Role.ALL]

api_permissions.rest.contracts.create = [Role.ALL]
api_permissions.rest.contracts.read = [Role.ALL]
api_permissions.rest.contracts["update"] = [Role.ALL]
api_permissions.rest.contracts.delete = [Role.ALL]

api_permissions.rest.datacatalogs.create = [Role.ALL]
api_permissions.rest.datacatalogs["update"] = [Role.ALL]
api_permissions.rest.datacatalogs.delete = [Role.ALL]

api_permissions.rest.datasets.create = [Role.ALL]
api_permissions.rest.datasets["update"] = [Role.ALL]
api_permissions.rest.datasets.delete = [Role.ALL]

api_permissions.rest.directories.read = [Role.ALL]

api_permissions.rest.files.create = [Role.ALL]
api_permissions.rest.files.read = [Role.ALL]
api_permissions.rest.files["update"] = [Role.ALL]
api_permissions.rest.files.delete = [Role.ALL]

api_permissions.rest.filestorages.create = [Role.ALL]
api_permissions.rest.filestorages.read = [Role.ALL]
api_permissions.rest.filestorages["update"] = [Role.ALL]
api_permissions.rest.filestorages.delete = [Role.ALL]

api_permissions.rpc.datasets.set_preservation_identifier.use = [Role.ALL]

api_permissions.rpc.files.delete_project.use = [Role.ALL]
api_permissions.rpc.files.flush_project.use = [Role.ALL]

API_ACCESS = prepare_perm_values(api_permissions.to_dict())
