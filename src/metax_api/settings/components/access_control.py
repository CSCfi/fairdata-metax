# specify read and write access of services per api, or if an api is readable by world

from enum import Enum

from box import Box

api_permissions = Box(
    {
        "rest": {
            "apierrors": {},
            "datacatalogs": {},
            "datasets": {},
            "directories": {},
            "editorpermissions": {},
            "files": {},
            "filestorages": {},
            "schemas": {},
        },
        "rpc": {"datasets": {}, "elasticsearchs": {}, "files": {}, "statistics": {}},
    },
    default_box_attr={},
    default_box=True,
)


# Associated datacatalog permissions are defined
# in /src/metax_api/initialdata/datacatalogs.json
# -----------------------------------------------
class Role(Enum):

    # 1 Baseline
    # These must be referenced in configurations
    # ------------------------------------------
    ETSIN = "etsin"
    FDS = "fds"
    IDA = "ida"
    METAX = "metax"
    QVAIN = "qvain"
    QVAIN_LIGHT = "qvain-light"
    TPAS = "tpas"

    # 2 Customer
    # These must be referenced in configurations
    # ------------------------------------------
    AALTO = "aalto"
    EUDAT = "eudat"
    JYU = "jyu"
    REPORTRONIC = "reportronic"
    SD = "sd"
    METAX_SERVICE = "metax_service"

    # 3 Utility
    # These do not need configuration definitions
    # -------------------------------------------
    ALL = "all"
    API_AUTH_USER = "api_auth_user"
    END_USERS = "endusers"
    EXTERNAL = "external"
    TEST_USER = "testuser"

    def __ge__(self, other):
        if self.__class__ is other.__class__:
            return self.value >= other.value

    def __gt__(self, other):
        if self.__class__ is other.__class__:
            return self.value > other.value

    def __le__(self, other):
        if self.__class__ is other.__class__:
            return self.value <= other.value

    def __lt__(self, other):
        if self.__class__ is other.__class__:
            return self.value < other.value


api_permissions.rest.apierrors.create = []
api_permissions.rest.apierrors.read = [Role.METAX]
api_permissions.rest.apierrors.delete = [Role.METAX]

api_permissions.rest.contracts.create = [Role.METAX, Role.TPAS, Role.METAX_SERVICE]
api_permissions.rest.contracts.read = [Role.METAX, Role.TPAS, Role.METAX_SERVICE]
api_permissions.rest.contracts["update"] = [Role.METAX, Role.TPAS, Role.METAX_SERVICE]
api_permissions.rest.contracts.delete = [Role.METAX, Role.TPAS, Role.METAX_SERVICE]

api_permissions.rest.datacatalogs.create = [Role.METAX, Role.ETSIN]
api_permissions.rest.datacatalogs.read = [Role.ALL]
api_permissions.rest.datacatalogs["update"] = [Role.METAX, Role.ETSIN]
api_permissions.rest.datacatalogs.delete = [Role.METAX, Role.ETSIN]

api_permissions.rest.datasets.create = [
    Role.METAX,
    Role.END_USERS,
    Role.TPAS,
    Role.QVAIN,
    Role.ETSIN,
    Role.EUDAT,
    Role.JYU,
    Role.METAX_SERVICE,
]
api_permissions.rest.datasets.read = [Role.ALL]
api_permissions.rest.datasets["update"] = [
    Role.METAX,
    Role.END_USERS,
    Role.TPAS,
    Role.QVAIN,
    Role.ETSIN,
    Role.METAX_SERVICE,
]
api_permissions.rest.datasets.delete = [
    Role.METAX,
    Role.END_USERS,
    Role.TPAS,
    Role.QVAIN,
    Role.ETSIN,
    Role.METAX_SERVICE,
]

api_permissions.rest.editorpermissions.create = [
    Role.METAX,
    Role.END_USERS,
    Role.TPAS,
    Role.QVAIN,
    Role.QVAIN_LIGHT,
    Role.ETSIN,
]
api_permissions.rest.editorpermissions.read = [Role.ALL]
api_permissions.rest.editorpermissions["update"] = [
    Role.METAX,
    Role.END_USERS,
    Role.TPAS,
    Role.QVAIN,
    Role.QVAIN_LIGHT,
    Role.ETSIN,
]
api_permissions.rest.editorpermissions.delete = [
    Role.METAX,
    Role.END_USERS,
    Role.TPAS,
    Role.QVAIN,
    Role.QVAIN_LIGHT,
    Role.ETSIN,
]

api_permissions.rest.directories.read = [
    Role.METAX,
    Role.QVAIN,
    Role.ETSIN,
    Role.TPAS,
    Role.FDS,
    Role.END_USERS,
]


api_permissions.rest.directories.delete = [Role.METAX, Role.IDA, Role.TPAS]

api_permissions.rest.files.create = [Role.METAX, Role.IDA, Role.TPAS, Role.METAX_SERVICE]
api_permissions.rest.files.read = [
    Role.METAX,
    Role.IDA,
    Role.FDS,
    Role.TPAS,
    Role.END_USERS,
    Role.QVAIN,
    Role.QVAIN_LIGHT,
    Role.METAX_SERVICE,
]
api_permissions.rest.files["update"] = [
    Role.METAX,
    Role.IDA,
    Role.TPAS,
    Role.FDS,
    Role.END_USERS,
    Role.QVAIN,
    Role.QVAIN_LIGHT,
    Role.METAX_SERVICE,
]
api_permissions.rest.files.delete = [Role.METAX, Role.IDA, Role.TPAS, Role.METAX_SERVICE]

api_permissions.rest.filestorages.create = [Role.METAX]
api_permissions.rest.filestorages.read = [Role.METAX]
api_permissions.rest.filestorages["update"] = [Role.METAX]
api_permissions.rest.filestorages.delete = [Role.METAX]

api_permissions.rest.schemas.read = [Role.ALL]

api_permissions.rpc.datasets.change_cumulative_state.use = [Role.ALL]
api_permissions.rpc.datasets.create_draft.use = [Role.ALL]
api_permissions.rpc.datasets.create_new_version.use = [Role.ALL]
api_permissions.rpc.datasets.fix_deprecated.use = [Role.ALL]
api_permissions.rpc.datasets.flush_user_data.use = [Role.METAX, Role.IDA, Role.TPAS]
api_permissions.rpc.datasets.get_minimal_dataset_template.use = [Role.ALL]
api_permissions.rpc.datasets.merge_draft.use = [Role.ALL]
api_permissions.rpc.datasets.publish_dataset.use = [Role.ALL]
api_permissions.rpc.datasets.refresh_directory_content.use = [Role.ALL]
api_permissions.rpc.datasets.set_preservation_identifier.use = [Role.METAX, Role.TPAS]

api_permissions.rpc.elasticsearchs.map_refdata.use = [Role.ALL]

api_permissions.rpc.files.delete_project.use = [Role.METAX, Role.IDA, Role.TPAS]
api_permissions.rpc.files.flush_project.use = [Role.METAX, Role.IDA, Role.TPAS]

api_permissions.rpc.statistics.all_datasets_cumulative.use = [Role.ALL]
api_permissions.rpc.statistics.catalog_datasets_cumulative.use = [Role.ALL]
api_permissions.rpc.statistics.count_datasets.use = [Role.ALL]
api_permissions.rpc.statistics.count_files.use = [Role.ALL]
api_permissions.rpc.statistics.deprecated_datasets_cumulative.use = [Role.ALL]
api_permissions.rpc.statistics.end_user_datasets_cumulative.use = [Role.ALL]
api_permissions.rpc.statistics.harvested_datasets_cumulative.use = [Role.ALL]
api_permissions.rpc.statistics.organization_datasets_cumulative.use = [Role.ALL]
api_permissions.rpc.statistics.unused_files.use = [Role.ALL]
api_permissions.rpc.statistics.projects_summary.use = [Role.ALL]
api_permissions.rpc.statistics.organizations_summary.use = [Role.ALL]


def prepare_perm_values(d):
    new_d = d
    if hasattr(d, "items"):
        for k, v in d.items():
            if isinstance(v, dict):
                prepare_perm_values(v)
            elif isinstance(v, list):
                v.sort()
                str_list = []
                for i in v:
                    if isinstance(i, Role):
                        str_list.append(i.value)
                    else:
                        str_list.append(str(i))
                new_d[k] = str_list
    return new_d


API_ACCESS = prepare_perm_values(api_permissions.to_dict())
