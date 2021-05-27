
from metax_api.settings.components.access_control import Role, api_permissions, prepare_perm_values
from metax_api.settings.environments.staging import API_USERS  # noqa: F401

api_permissions.rest.datasets.create += [Role.IDA, Role.QVAIN_LIGHT, Role.JYU]
api_permissions.rest.datasets["update"] += [Role.IDA, Role.QVAIN_LIGHT, Role.JYU]
api_permissions.rest.datasets.delete += [Role.IDA, Role.QVAIN_LIGHT, Role.JYU]

api_permissions.rest.directories.read += [Role.IDA, Role.QVAIN_LIGHT]

api_permissions.rest.files.read += [Role.QVAIN, Role.QVAIN_LIGHT]
api_permissions.rest.files["update"] += [Role.QVAIN, Role.QVAIN_LIGHT]

api_permissions.rpc.datasets.change_cumulative_state.use = [Role.METAX, Role.QVAIN, Role.QVAIN_LIGHT, Role.END_USERS]
api_permissions.rpc.datasets.fix_deprecated.use = [Role.METAX, Role.QVAIN, Role.QVAIN_LIGHT, Role.END_USERS]
api_permissions.rpc.dataset.refresh_directory_content.use = [Role.METAX, Role.QVAIN, Role.QVAIN_LIGHT, Role.END_USERS]

API_ACCESS = prepare_perm_values(api_permissions)