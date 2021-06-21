# This file is part of the Metax API service
#
# Copyright 2017-2018 Ministry of Education and Culture, Finland
#
# :author: CSC - IT Center for Science Ltd., Espoo Finland <servicedesk@csc.fi>
# :license: MIT

import os
import sys
from copy import deepcopy
from json import dump as json_dump, load as json_load

from jsonschema import validate as json_validate

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils import generate_test_identifier, get_json_schema

"""
A script to generate test data. Loaded for automated tests, and as content for
test environments.
"""

# how many file rows to generate
file_max_rows = 120

# how many filestorage rows to generate
file_storage_max_rows = 2

ida_data_catalog_max_rows = 4

att_data_catalog_max_rows = 4

contract_max_rows = 5

ida_catalog_record_max_rows = 10

att_catalog_record_max_rows = 10

files_per_dataset = 2

catalog_records_per_contract = 2

# spread these evenly among the cr's
catalog_records_owner_ids = [
    "053bffbcc41edad4853bea91fc42ea18",
    "053d18ecb29e752cb7a35cd77b34f5fd",
    "05593961536b76fa825281ccaedd4d4f",
    "055ea4dade5ab2145954f56d4b51cef0",
    "055ea531a6cac569425bed94459266ee",
]

# very slow with a large number of rows. we'll always validate the first loop tho
validate_json = False

# Location of schema files
schema_path = os.path.dirname(__file__) + "../api.rest.base/schemas"

# identifier model type
cr_type = 1  # catalog record
dc_type = 2  # data catalog


def generate_file_storages(file_storage_max_rows):
    print("generating file storages...")
    test_file_storage_list = []

    with open("file_storage_test_data_template.json") as json_file:
        row_template = json_load(json_file)

    title = row_template["file_storage_json"]["title"]
    identifier = "pid:urn:storage" + row_template["file_storage_json"]["identifier"]

    for i in range(1, file_storage_max_rows + 1):
        new = {
            "fields": {
                "date_modified": "2017-06-23T10:07:22Z",
                "date_created": "2017-05-23T10:07:22Z",
                "file_storage_json": {
                    "title": title % str(i),
                    "identifier": identifier % str(i),
                    "url": "https://metax.fd-test.csc.fi/rest/filestorages/%d" % i,
                },
            },
            "model": "metax_api.filestorage",
            "pk": i,
        }
        test_file_storage_list.append(new)

    return test_file_storage_list


def generate_files(test_file_storage_list, validate_json):
    print("generating files...")

    with open("file_test_data_template.json") as json_file:
        row_template = json_load(json_file)

    json_template = row_template["file_characteristics"].copy()
    file_name = row_template["file_name"]
    json_title = json_template["title"]
    json_description = json_template["description"]

    directories = []
    file_test_data_list = []
    directory_test_data_list = []
    json_schema = get_json_schema("file")
    file_storage = test_file_storage_list[0]["pk"]

    for i in range(1, file_max_rows + 1):
        if i <= 20:
            project_identifier = "project_x"
            project_root_folder = "project_x_FROZEN"
        else:
            project_identifier = "research_project_112"
            project_root_folder = "prj_112_root"

        loop = str(i)
        new = {
            "fields": row_template.copy(),
            "model": "metax_api.file",
        }

        file_path = row_template["file_path"]

        # assing files to different directories to have something to browse
        if 1 <= i < 6:
            file_path = file_path.replace(
                "/some/path/", "/{0}/Experiment_X/".format(project_root_folder)
            )
        elif 6 <= i < 11:
            file_path = file_path.replace(
                "/some/path/", "/{0}/Experiment_X/Phase_1/".format(project_root_folder)
            )
        elif 11 <= i <= 20:
            file_path = file_path.replace(
                "/some/path/",
                "/{0}/Experiment_X/Phase_1/2017/01/".format(project_root_folder),
            )
        if i == 21:
            file_path = file_path.replace(
                "/some/path/", "/{0}/science_data_A/".format(project_root_folder)
            )
        if 22 <= i < 25:
            file_path = file_path.replace(
                "/some/path/",
                "/{0}/science_data_A/phase_1/2018/01/".format(project_root_folder),
            )
        if i == 25:
            file_path = file_path.replace(
                "/some/path/", "/{0}/science_data_B/".format(project_root_folder)
            )
        if i == 26:
            file_path = file_path.replace(
                "/some/path/", "/{0}/other/items/".format(project_root_folder)
            )
        if 27 <= i < 30:
            file_path = file_path.replace(
                "/some/path/", "/{0}/random_folder/".format(project_root_folder)
            )
        if 30 <= i < 35:
            file_path = file_path.replace(
                "/some/path/", "/{0}/science_data_C/".format(project_root_folder)
            )
        elif 35 <= i < 40:
            file_path = file_path.replace(
                "/some/path/",
                "/{0}/science_data_C/phase_1/".format(project_root_folder),
            )
        elif 40 <= i < 50:
            file_path = file_path.replace(
                "/some/path/",
                "/{0}/science_data_C/phase_1/2017/01/".format(project_root_folder),
            )
        elif 50 <= i < 70:
            file_path = file_path.replace(
                "/some/path/",
                "/{0}/science_data_C/phase_1/2017/02/".format(project_root_folder),
            )
        elif 70 <= i <= file_max_rows:
            file_path = file_path.replace(
                "/some/path/",
                "/{0}/science_data_C/phase_2/2017/10/".format(project_root_folder),
            )

        directory_id = get_parent_directory_for_path(
            directories, file_path, directory_test_data_list, project_identifier
        )

        new["fields"]["parent_directory"] = directory_id
        new["fields"]["project_identifier"] = project_identifier
        new["fields"]["file_name"] = file_name % loop
        new["fields"]["file_path"] = file_path % loop
        new["fields"]["identifier"] = "pid:urn:" + loop
        new["fields"]["file_characteristics"]["title"] = json_title % loop
        new["fields"]["file_characteristics"]["description"] = json_description % loop
        new["fields"]["file_storage"] = file_storage
        new["fields"]["byte_size"] = i * 100
        new["pk"] = i

        if validate_json or i == 1:
            json_validate(new["fields"]["file_characteristics"], json_schema)

        file_test_data_list.append(new)

    return file_test_data_list, directory_test_data_list


def get_parent_directory_for_path(
    directories, file_path, directory_test_data_list, project_identifier
):
    dir_name = os.path.dirname(file_path)
    for d in directories:
        if (
            d["fields"]["directory_path"] == dir_name
            and d["fields"]["project_identifier"] == project_identifier
        ):
            return d["pk"]
    return create_parent_directory_for_path(
        directories, dir_name, directory_test_data_list, project_identifier
    )


def create_parent_directory_for_path(
    directories, file_path, directory_test_data_list, project_identifier
):
    """
    Recursively creates the requested directories for file_path
    """
    with open("directory_test_data_template.json") as json_file:
        row_template = json_load(json_file)

    if file_path == "/":
        directory_id = None
    else:
        # the directory where a file or dir belongs to, must be retrieved or created first
        directory_id = get_parent_directory_for_path(
            directories, file_path, directory_test_data_list, project_identifier
        )

    # all parent dirs have been created - now create the dir that was originally asked for

    new_id = len(directories) + 1

    new = {
        "fields": row_template.copy(),
        "model": "metax_api.directory",
        "pk": new_id,
    }

    # note: it is possible that parent_directory is null (top-level directories)
    new["fields"]["parent_directory"] = directory_id
    new["fields"]["directory_name"] = os.path.basename(file_path)
    new["fields"]["directory_path"] = file_path
    new["fields"]["identifier"] = new["fields"]["identifier"] % new_id
    new["fields"]["project_identifier"] = project_identifier

    directory_test_data_list.append(new)
    directories.append(new)

    return new_id


def save_test_data(
    file_storage_list,
    file_list,
    directory_list,
    data_catalogs_list,
    contract_list,
    catalog_record_list,
    dataset_version_sets,
):
    with open("test_data.json", "w") as f:
        print("dumping test data as json to metax_api/tests/test_data.json...")
        json_dump(
            file_storage_list
            + directory_list
            + file_list
            + data_catalogs_list
            + contract_list
            + dataset_version_sets
            + catalog_record_list,
            f,
            indent=4,
            sort_keys=True,
        )


def generate_data_catalogs(start_idx, data_catalog_max_rows, validate_json, type):
    print("generating %s data catalogs..." % type)
    test_data_catalog_list = []
    json_schema = get_json_schema("datacatalog")

    with open("data_catalog_test_data_template.json") as json_file:
        row_template = json_load(json_file)

    for i in range(start_idx, start_idx + data_catalog_max_rows):

        new = {
            "fields": deepcopy(row_template),
            "model": "metax_api.datacatalog",
            "pk": i,
        }
        new["fields"]["date_modified"] = "2017-06-15T10:07:22Z"
        new["fields"]["date_created"] = "2017-05-15T10:07:22Z"
        new["fields"]["catalog_json"]["identifier"] = generate_test_identifier(dc_type, i)

        if type == "ida":
            new["fields"]["catalog_json"]["research_dataset_schema"] = "ida"
        elif type == "att":
            new["fields"]["catalog_json"]["research_dataset_schema"] = "att"

        if i in (start_idx, start_idx + 1):
            # lets pretend that the first two data catalogs will support versioning,
            # they are "fairdata catalogs"
            dataset_versioning = True
            new["fields"]["catalog_json"]["harvested"] = False
        else:
            dataset_versioning = False

            # rest of the catalogs are harvested
            new["fields"]["catalog_json"]["harvested"] = True

        new["fields"]["catalog_json"]["dataset_versioning"] = dataset_versioning

        test_data_catalog_list.append(new)

        if validate_json or i == start_idx:
            json_validate(new["fields"]["catalog_json"], json_schema)

    return test_data_catalog_list


def generate_contracts(contract_max_rows, validate_json):
    print("generating contracts...")
    test_contract_list = []
    json_schema = get_json_schema("contract")

    with open("contract_test_data_template.json") as json_file:
        row_template = json_load(json_file)

    # sample contract provided by PAS
    new = {
        "fields": deepcopy(row_template[0]),
        "model": "metax_api.contract",
        "pk": 1,
    }
    json_validate(new["fields"]["contract_json"], json_schema)
    test_contract_list.append(new)

    for i in range(2, contract_max_rows + 1):

        new = {
            "fields": deepcopy(row_template[1]),
            "model": "metax_api.contract",
            "pk": i,
        }

        new["fields"]["contract_json"]["identifier"] = "optional:contract:identifier%d" % i
        new["fields"]["contract_json"]["title"] = "Title of Contract %d" % i
        new["fields"]["contract_json"]["organization"]["organization_identifier"] = "1234567-%d" % i
        new["fields"]["date_modified"] = "2017-06-15T10:07:22Z"
        new["fields"]["date_created"] = "2017-05-15T10:07:22Z"
        test_contract_list.append(new)

        if validate_json or i == 1:
            json_validate(new["fields"]["contract_json"], json_schema)

    return test_contract_list


def generate_catalog_records(
    basic_catalog_record_max_rows,
    data_catalogs_list,
    contract_list,
    file_list,
    validate_json,
    type,
    test_data_list=[],
    dataset_version_sets=[],
):
    print("generating %s catalog records..." % type)

    with open("catalog_record_test_data_template.json") as json_file:
        row_template = json_load(json_file)

    files_start_idx = 1
    data_catalog_id = data_catalogs_list[0]["pk"]
    owner_idx = 0
    start_idx = len(test_data_list) + 1

    for i in range(start_idx, start_idx + basic_catalog_record_max_rows):
        json_schema = None

        if type == "ida":
            json_schema = get_json_schema("ida_dataset")
        elif type == "att":
            json_schema = get_json_schema("att_dataset")

        new = {
            "fields": row_template.copy(),
            "model": "metax_api.catalogrecord",
            "pk": i,
        }

        if data_catalog_id in (1, 2):  # versioned catalogs only
            dataset_version_set = {
                "fields": {},
                "model": "metax_api.datasetversionset",
                "pk": i,
            }
            new["fields"]["dataset_version_set"] = dataset_version_set["pk"]
            dataset_version_sets.append(dataset_version_set)

        # comment this line. i dare you.
        # for real tho, required to prevent some strange behaving references to old data
        new["fields"]["research_dataset"] = row_template["research_dataset"].copy()
        new["fields"]["data_catalog"] = data_catalog_id
        new["fields"]["research_dataset"]["metadata_version_identifier"] = generate_test_identifier(
            cr_type, i, urn=False
        )
        new["fields"]["research_dataset"]["preferred_identifier"] = generate_test_identifier(
            cr_type, i
        )
        new["fields"]["identifier"] = generate_test_identifier("cr", i, urn=False)
        new["fields"]["date_modified"] = "2017-06-23T10:07:22Z"
        new["fields"]["date_created"] = "2017-05-23T10:07:22Z"
        new["fields"]["files"] = []

        # add files

        if type == "ida":
            new["fields"]["files"] = []
            dataset_files = []
            total_files_byte_size = 0
            file_divider = 4

            for j in range(files_start_idx, files_start_idx + files_per_dataset):

                total_files_byte_size += file_list[j - 1]["fields"]["byte_size"]

                # note - this field will go in the m2m table in the db when importing generated testdata...
                new["fields"]["files"].append(file_list[j - 1]["pk"])

                # ... while every API operation will look at research_dataset.files.identifier
                # to lookup the file - be careful the identifier below matches with the m2m id set above
                dataset_files.append(
                    {
                        "identifier": file_list[j - 1]["fields"]["identifier"],
                        "title": "File metadata title %d" % j,
                    }
                )

                if j < file_divider:
                    # first fifth of files
                    dataset_files[-1]["file_type"] = {
                        "identifier": "http://uri.suomi.fi/codelist/fairdata/file_type/code/text",
                        "pref_label": {
                            "fi": "Teksti",
                            "en": "Text",
                            "und": "Teksti"
                        },
                        "in_scheme": "http://uri.suomi.fi/codelist/fairdata/file_type"
                    }
                    dataset_files[-1]["use_category"] = {
                        "identifier": "http://uri.suomi.fi/codelist/fairdata/use_category/code/source",
                        "pref_label": {
                            "fi": "Lähdeaineisto",
                            "en": "Source material",
                            "und": "Lähdeaineisto"
                        },
                        "in_scheme": "http://uri.suomi.fi/codelist/fairdata/use_category"
                    }

                elif file_divider <= j < (file_divider * 2):
                    # second fifth of files
                    dataset_files[-1]["file_type"] = {
                        "identifier": "http://uri.suomi.fi/codelist/fairdata/file_type/code/video",
                        "pref_label": {
                            "fi": "Video",
                            "en": "Video",
                            "und": "Video"
                        },
                        "in_scheme": "http://uri.suomi.fi/codelist/fairdata/file_type"
                    }
                    dataset_files[-1]["use_category"] = {
                        "identifier": "http://uri.suomi.fi/codelist/fairdata/use_category/code/outcome",
                        "pref_label": {
                            "fi": "Tulosaineisto",
                            "en": "Outcome material",
                            "und": "Tulosaineisto"
                        },
                        "in_scheme": "http://uri.suomi.fi/codelist/fairdata/use_category"
                    }
                elif (file_divider * 2) <= j < (file_divider * 3):
                    # third fifth of files
                    dataset_files[-1]["file_type"] = {
                        "identifier": "http://uri.suomi.fi/codelist/fairdata/file_type/code/image",
                        "pref_label": {
                            "fi": "Kuva",
                            "en": "Image",
                            "und": "Kuva"
                        },
                        "in_scheme": "http://uri.suomi.fi/codelist/fairdata/file_type"
                    }
                    dataset_files[-1]["use_category"] = {
                        "identifier": "http://uri.suomi.fi/codelist/fairdata/use_category/code/publication",
                        "pref_label": {
                            "fi": "Julkaisu",
                            "en": "Publication",
                            "und": "Julkaisu"
                        },
                        "in_scheme": "http://uri.suomi.fi/codelist/fairdata/use_category"
                    }
                elif (file_divider * 3) <= j < (file_divider * 4):
                    # fourth fifth of files
                    dataset_files[-1]["file_type"] = {
                        "identifier": "http://uri.suomi.fi/codelist/fairdata/file_type/code/source_code",
                        "pref_label": {
                            "fi": "Lähdekoodi",
                            "en": "Source code",
                            "und": "Lähdekoodi"
                        },
                        "in_scheme": "http://uri.suomi.fi/codelist/fairdata/file_type"
                    }
                    dataset_files[-1]["use_category"] = {
                        "identifier": "http://uri.suomi.fi/codelist/fairdata/use_category/code/documentation",
                        "pref_label": {
                            "fi": "Dokumentaatio",
                            "en": "Documentation",
                            "und": "Dokumentaatio"
                        },
                        "in_scheme": "http://uri.suomi.fi/codelist/fairdata/use_category"
                    }
                else:
                    # the rest of files
                    dataset_files[-1]["use_category"] = {"identifier": "configuration"}

            new["fields"]["research_dataset"]["files"] = dataset_files
            new["fields"]["research_dataset"]["total_files_byte_size"] = total_files_byte_size
            files_start_idx += files_per_dataset

        elif type == "att":
            new["fields"]["research_dataset"]["remote_resources"] = [
                {
                    "title": "Remote resource {0}".format(str(i)),
                    "modified": "2014-01-12T17:11:54Z",
                    "use_category": {"identifier": "outcome"},
                    "checksum": {
                        "algorithm": "SHA-256",
                        "checksum_value": "u5y6f4y68765ngf6ry8n",
                    },
                    "byte_size": i * 512,
                },
                {
                    "title": "Other remote resource {0}".format(str(i)),
                    "modified": "2013-01-12T11:11:54Z",
                    "use_category": {"identifier": "source"},
                    "checksum": {
                        "algorithm": "SHA-512",
                        "checksum_value": "u3k4kn7n1g56l6rq5a5s",
                    },
                    "byte_size": i * 1024,
                },
            ]
            total_remote_resources_byte_size = 0
            for rr in new["fields"]["research_dataset"]["remote_resources"]:
                total_remote_resources_byte_size += rr.get("byte_size", 0)
            new["fields"]["research_dataset"][
                "total_remote_resources_byte_size"
            ] = total_remote_resources_byte_size

        if validate_json or i == start_idx:
            json_validate(new["fields"]["research_dataset"], json_schema)

        test_data_list.append(new)

    # set some preservation_states and dependent values
    # see CatalogRecord model PRESERVATION_STATE_... for value definitions
    for i, pres_state_value in enumerate((0, 10, 10, 40, 50, 70, 70, 90, 130, 140)):

        test_data_list[i]["fields"]["preservation_state"] = pres_state_value

        if i > 0:
            test_data_list[i]["fields"]["contract"] = 1

        if i in (90, 140):
            # packaging, dissemination
            test_data_list[i]["fields"]["mets_object_identifier"] = ["a", "b", "c"]

        test_data_list[i]["fields"]["research_dataset"]["curator"] = [
            {
                "@type": "Person",
                "name": "Rahikainen",
                "identifier": "id:of:curator:rahikainen",
                "member_of": {
                    "@type": "Organization",
                    "name": {"fi": "MysteeriOrganisaatio"},
                },
            }
        ]

    # set different owner
    for i in range(start_idx + 5, len(test_data_list)):
        test_data_list[i]["fields"]["research_dataset"]["curator"] = [
            {
                "@type": "Person",
                "name": "Jarski",
                "identifier": "id:of:curator:jarski",
                "member_of": {
                    "@type": "Organization",
                    "name": {"fi": "MysteeriOrganisaatio"},
                },
            }
        ]

    # if preservation_state is other than 0, means it has been modified at some point,
    # so set timestamp
    for i in range(start_idx - 1, len(test_data_list)):
        row = test_data_list[i]
        if row["fields"]["preservation_state"] != 0:
            row["fields"]["preservation_state_modified"] = "2017-05-23T10:07:22.559656Z"

    # add a couple of catalog records with fuller research_dataset fields belonging to both ida and att data catalog
    total_files_byte_size = 0
    if type == "ida":
        template = "catalog_record_test_data_template_full_ida.json"
    elif type == "att":
        template = "catalog_record_test_data_template_full_att.json"

    with open(template) as json_file:
        row_template_full = json_load(json_file)

    for j in [0, 1, 2]:
        new = {
            "fields": deepcopy(row_template_full),
            "model": "metax_api.catalogrecord",
            "pk": len(test_data_list) + 1,
        }

        if data_catalog_id in (1, 2):  # versioned catalogs only
            dataset_version_set = {
                "fields": {},
                "model": "metax_api.datasetversionset",
                "pk": len(test_data_list) + 1,
            }
            new["fields"]["dataset_version_set"] = dataset_version_set["pk"]
            dataset_version_sets.append(dataset_version_set)

        # for the relation in the db. includes dir id 3, which includes all 20 files

        new["fields"]["data_catalog"] = data_catalog_id
        new["fields"]["date_modified"] = "2017-09-23T10:07:22Z"
        new["fields"]["date_created"] = "2017-05-23T10:07:22Z"
        new["fields"]["editor"] = {
            "owner_id": catalog_records_owner_ids[j],
            "creator_id": catalog_records_owner_ids[owner_idx],
        }

        new["fields"]["research_dataset"]["metadata_version_identifier"] = generate_test_identifier(
            cr_type, len(test_data_list) + 1, urn=False
        )
        new["fields"]["research_dataset"]["preferred_identifier"] = generate_test_identifier(
            cr_type, len(test_data_list) + 1
        )
        new["fields"]["identifier"] = generate_test_identifier(
            "cr", len(test_data_list) + 1, urn=False
        )

        if type == "ida":
            if j in [0, 1]:
                new["fields"]["files"] = [i for i in range(1, 21)]
                file_identifier_0 = file_list[0]["fields"]["identifier"]
                file_identifier_1 = file_list[1]["fields"]["identifier"]
                total_files_byte_size = sum(f["fields"]["byte_size"] for f in file_list[0:19])
                new["fields"]["research_dataset"]["total_files_byte_size"] = total_files_byte_size
                new["fields"]["research_dataset"]["files"][0]["identifier"] = file_identifier_0
                new["fields"]["research_dataset"]["files"][1]["identifier"] = file_identifier_1
            elif j == 2:
                db_files = []
                directories = []
                files = []

                db_files = [22, 23, 24, 25, 26, 27, 28]
                db_files.extend(list(range(35, 116)))

                files = [
                    {
                        "identifier": "pid:urn:27",
                        "title": "file title 27",
                        "description": "file description 27",
                        "file_type": {
                            "identifier": "video",
                            "definition": {
                                "en": "A statement or formal explanation of the meaning of a concept."
                            },
                            "in_scheme": "http://uri.of.filetype.concept/scheme",
                        },
                        "use_category": {"identifier": "configuration"},
                    },
                    {
                        "identifier": "pid:urn:28",
                        "title": "file title 28",
                        "description": "file description 28",
                        "file_type": {
                            "identifier": "software",
                            "definition": {
                                "en": "A statement or formal explanation of the meaning of a concept."
                            },
                            "in_scheme": "http://uri.of.filetype.concept/scheme",
                        },
                        "use_category": {"identifier": "publication"},
                    },
                ]

                directories = [
                    {
                        "identifier": "pid:urn:dir:18",
                        "title": "Phase 1 of science data C",
                        "description": "Description of the directory",
                        "use_category": {"identifier": "outcome"},
                    },
                    {
                        "identifier": "pid:urn:dir:22",
                        "title": "Phase 2 of science data C",
                        "description": "Description of the directory",
                        "use_category": {"identifier": "outcome"},
                    },
                    {
                        "identifier": "pid:urn:dir:12",
                        "title": "Phase 1 01/2018 of Science data A",
                        "description": "Description of the directory",
                        "use_category": {"identifier": "outcome"},
                    },
                    {
                        "identifier": "pid:urn:dir:13",
                        "title": "Science data B",
                        "description": "Description of the directory",
                        "use_category": {"identifier": "source"},
                    },
                    {
                        "identifier": "pid:urn:dir:14",
                        "title": "Other stuff",
                        "description": "Description of the directory",
                        "use_category": {"identifier": "method"},
                    },
                ]

                total_files_byte_size += sum(
                    file_list[file_pk - 1]["fields"]["byte_size"] for file_pk in db_files
                )

                new["fields"]["files"] = db_files
                new["fields"]["research_dataset"]["files"] = files
                new["fields"]["research_dataset"]["directories"] = directories
                new["fields"]["research_dataset"]["total_files_byte_size"] = total_files_byte_size
        elif type == "att":
            total_remote_resources_byte_size = 0
            if "remote_resources" in new["fields"]["research_dataset"]:
                for rr in new["fields"]["research_dataset"]["remote_resources"]:
                    total_remote_resources_byte_size += rr.get("byte_size", 0)
                new["fields"]["research_dataset"][
                    "total_remote_resources_byte_size"
                ] = total_remote_resources_byte_size

        json_validate(new["fields"]["research_dataset"], json_schema)
        test_data_list.append(new)

    return test_data_list, dataset_version_sets


def generate_alt_catalog_records(test_data_list):
    #
    # create a couple of alternate records for record with id 10
    #
    # note, these alt records wont have an editor-field set, since they presumably
    # originated to metax from somewhere else than qvain (were harvested).
    #
    print("generating alternate catalog records...")
    alternate_record_set = {
        "fields": {},
        "model": "metax_api.alternaterecordset",
        "pk": 1,
    }

    # first record belongs to alt record set
    test_data_list[9]["fields"]["alternate_record_set"] = 1

    # create one other record
    alt_rec = deepcopy(test_data_list[9])
    alt_rec["pk"] = test_data_list[-1]["pk"] + 1
    alt_rec["fields"]["research_dataset"]["preferred_identifier"] = test_data_list[9]["fields"][
        "research_dataset"
    ]["preferred_identifier"]
    alt_rec["fields"]["research_dataset"]["metadata_version_identifier"] += "-alt-1"
    alt_rec["fields"]["identifier"] = generate_test_identifier(
        "cr", len(test_data_list) + 1, urn=False
    )
    alt_rec["fields"]["data_catalog"] = 2
    alt_rec["fields"]["alternate_record_set"] = 1
    alt_rec["fields"].pop("dataset_version_set", None)
    test_data_list.append(alt_rec)

    # create second other record
    alt_rec = deepcopy(test_data_list[9])
    alt_rec["pk"] = test_data_list[-1]["pk"] + 1
    alt_rec["fields"]["research_dataset"]["preferred_identifier"] = test_data_list[9]["fields"][
        "research_dataset"
    ]["preferred_identifier"]
    alt_rec["fields"]["research_dataset"]["metadata_version_identifier"] += "-alt-2"
    alt_rec["fields"]["identifier"] = generate_test_identifier(
        "cr", len(test_data_list) + 1, urn=False
    )
    alt_rec["fields"]["data_catalog"] = 3
    alt_rec["fields"]["alternate_record_set"] = 1
    alt_rec["fields"].pop("dataset_version_set", None)
    test_data_list.append(alt_rec)

    # alternate record set must exist before importing catalog records, so prepend it
    test_data_list.insert(0, alternate_record_set)
    return test_data_list


def set_qvain_info_to_records(catalog_record_list):
    """
    For data catalog ids 1,2 set qvain info, since they are supposedly fairdata catalogs.
    """
    owner_idx = 0
    total_qvain_users = len(catalog_records_owner_ids)
    for cr in catalog_record_list:
        if cr["model"] != "metax_api.catalogrecord":
            # there are other type of objects in the list (at least datasetversionset)
            continue
        if cr["fields"]["data_catalog"] not in (1, 2):
            continue
        cr["fields"]["editor"] = {
            "owner_id": catalog_records_owner_ids[owner_idx],
            "creator_id": catalog_records_owner_ids[owner_idx],
            "identifier": "qvain",
            "record_id": "955e904-e3dd-4d7e-99f1-3fed446f9%03d"
            % cr["pk"],  # 3 leading zeroes to preserve length
        }
        owner_idx += 1
        if owner_idx >= total_qvain_users:
            owner_idx = 0


if __name__ == "__main__":
    print("begin generating test data...")

    contract_list = generate_contracts(contract_max_rows, validate_json)
    file_storage_list = generate_file_storages(file_storage_max_rows)
    file_list, directory_list = generate_files(file_storage_list, validate_json)

    ida_data_catalogs_list = generate_data_catalogs(
        1, ida_data_catalog_max_rows, validate_json, "ida"
    )
    att_data_catalogs_list = generate_data_catalogs(
        ida_data_catalog_max_rows + 1, att_data_catalog_max_rows, validate_json, "att"
    )

    catalog_record_list, dataset_version_sets = generate_catalog_records(
        ida_catalog_record_max_rows,
        ida_data_catalogs_list,
        contract_list,
        file_list,
        validate_json,
        "ida",
    )

    catalog_record_list, dataset_version_sets = generate_catalog_records(
        att_catalog_record_max_rows,
        att_data_catalogs_list,
        contract_list,
        [],
        validate_json,
        "att",
        catalog_record_list,
        dataset_version_sets,
    )

    catalog_record_list = generate_alt_catalog_records(catalog_record_list)

    set_qvain_info_to_records(catalog_record_list)

    save_test_data(
        file_storage_list,
        directory_list,
        file_list,
        ida_data_catalogs_list + att_data_catalogs_list,
        contract_list,
        catalog_record_list,
        dataset_version_sets,
    )

    print("done")
