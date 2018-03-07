import json
import os
import sys
import time
from copy import deepcopy
from json import dump as json_dump
from json import dumps as json_dumps
from json import load as json_load
from uuid import uuid4

import requests
import urllib3
from jsonschema import validate as json_validate

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils import get_json_schema, generate_test_identifier

"""
Execute this file to generate rows to metax_api_file table. Uses
file_test_data_template.json as template, and slightly modifies fields each loop.

When sending generated files immediately using requests for the first time, make sure to
load data from a json-generated list first, because file storages are only generated when
usind mode=json for now. After that, sending requests will work too because then they can
use the file_storage from the json-generated file. If you are flushing the db at times,
remember to load file storages from json-file again.

Data input options:

- Generate rows as a json list to file file_test_data_list.json. Note that manage.py wont
  handle very large files very well, i.e. dont try to load half a million rows in one file.
  Execute the following to load the json data to django db:
  python manage.py loaddata metax_api/tests/file_test_data_list.json

- Send generated rows directly as http POST requests to your api.
  Optionally send either one by one, or in bulk in one request, or in bulk in batches.
  Note that nginx max request size restriction applies.

todo:
- run with proper cmd line parameters ?
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
    '053bffbcc41edad4853bea91fc42ea18',
    '053d18ecb29e752cb7a35cd77b34f5fd',
    '05593961536b76fa825281ccaedd4d4f',
    '055ea4dade5ab2145954f56d4b51cef0',
    '055ea531a6cac569425bed94459266ee',
]

# mode: json for json-file, request for request per row, request_list for bulk post
mode = 'json'

# how many rows to send in one request when using request_list.
# sends requests in batches until all rows sent. value 0 means send all at once
batch_size = 0

# very slow with a large number of rows. we'll always validate the first loop tho
validate_json = False

# where to send POST requests if using it
url = 'https://metax.csc.local/rest/files/'

# print tasty debug information from POST requests
DEBUG = False

#  to disable self-signed certificate warnings
urllib3.disable_warnings()

# Location of schema files
schema_path = os.path.dirname(__file__) + '../api.rest.base/schemas'

# identifier model type
cr_type = 1  # catalog record
dc_type = 2  # data catalog


def generate_file_storages(mode, file_storage_max_rows):
    test_file_storage_list = []

    if mode == 'json':

        with open('file_storage_test_data_template.json') as json_file:
            row_template = json_load(json_file)

        title = row_template['file_storage_json']['title']
        identifier = "pid:urn:storage" + row_template['file_storage_json']['identifier']

        for i in range(1, file_storage_max_rows + 1):
            new = {
                'fields': {
                    'date_modified': '2017-06-23T10:07:22Z',
                    'date_created': '2017-05-23T10:07:22Z',
                    'file_storage_json': {
                        'title': title % str(i),
                        'identifier': identifier % str(i),
                        'url': url,
                    }
                },
                'model': "metax_api.filestorage",
                'pk': i
            }
            test_file_storage_list.append(new)

    return test_file_storage_list


def generate_files(mode, test_file_storage_list, validate_json, url):
    print('generating files%s...' % ('' if mode in ('json', 'request_list') else ' and uploading'))

    with open('file_test_data_template.json') as json_file:
        row_template = json_load(json_file)

    json_template = row_template['file_characteristics'].copy()
    file_name = row_template['file_name']
    json_title = json_template['title']
    json_description = json_template['description']

    directories = []
    file_test_data_list = []
    directory_test_data_list = []
    json_schema = get_json_schema('file')
    total_time_elapsed = 0

    if mode == "json":
        # use the file storage id from the file storage created previously during this script execution
        file_storage = test_file_storage_list[0]['pk']
    else:
        # with POST requests, new file storages are not generated. instead, it is expected that they have
        # been already loaded in from previous test data
        with open('file_test_data_list.json') as old_test_data_file:
            file_test_data = json_load(old_test_data_file)
            file_storage = file_test_data[0]['pk']

    for i in range(1, file_max_rows + 1):
        if i <= 20:
            project_identifier = 'project_x'
            project_root_folder = 'project_x_FROZEN'
        else:
            project_identifier = 'research_project_112'
            project_root_folder = 'prj_112_root'

        loop = str(i)
        if mode == 'json':

            new = {
                'fields': row_template.copy(),
                'model': 'metax_api.file',
            }

            file_path = row_template['file_path']

            # assing files to different directories to have something to browse
            if 1 <= i < 6:
                file_path = file_path.replace('/some/path/', '/{0}/Experiment_X/'.
                                              format(project_root_folder))
            elif 6 <= i < 11:
                file_path = file_path.replace('/some/path/', '/{0}/Experiment_X/Phase_1/'.
                                              format(project_root_folder))
            elif 11 <= i <= 20:
                file_path = file_path.replace('/some/path/', '/{0}/Experiment_X/Phase_1/2017/01/'.
                                              format(project_root_folder))
            if i == 21:
                file_path = file_path.replace('/some/path/', '/{0}/science_data_A/'.
                                              format(project_root_folder))
            if 22 <= i < 25:
                file_path = file_path.replace('/some/path/', '/{0}/science_data_A/phase_1/2018/01/'.
                                              format(project_root_folder))
            if i == 25:
                file_path = file_path.replace('/some/path/', '/{0}/science_data_B/'.
                                              format(project_root_folder))
            if i == 26:
                file_path = file_path.replace('/some/path/', '/{0}/other/items/'.
                                              format(project_root_folder))
            if 27 <= i < 30:
                file_path = file_path.replace('/some/path/', '/{0}/random_folder/'.
                                              format(project_root_folder))
            if 30 <= i < 35:
                file_path = file_path.replace('/some/path/', '/{0}/science_data_C/'.
                                              format(project_root_folder))
            elif 35 <= i < 40:
                file_path = file_path.replace('/some/path/', '/{0}/science_data_C/phase_1/'.
                                              format(project_root_folder))
            elif 40 <= i < 50:
                file_path = file_path.replace('/some/path/', '/{0}/science_data_C/phase_1/2017/01/'.
                                              format(project_root_folder))
            elif 50 <= i < 70:
                file_path = file_path.replace('/some/path/', '/{0}/science_data_C/phase_1/2017/02/'.
                                              format(project_root_folder))
            elif 70 <= i <= file_max_rows:
                file_path = file_path.replace('/some/path/', '/{0}/science_data_C/phase_2/2017/10/'.
                                              format(project_root_folder))

            directory_id = get_parent_directory_for_path(directories, file_path, directory_test_data_list,
                                                         project_identifier)

            new['fields']['parent_directory'] = directory_id
            new['fields']['project_identifier'] = project_identifier
            new['fields']['file_name'] = file_name % loop
            new['fields']['file_path'] = file_path % loop
            new['fields']['identifier'] = "pid:urn:" + loop
            new['fields']['file_characteristics']['title'] = json_title % loop
            new['fields']['file_characteristics']['description'] = json_description % loop
            new['fields']['file_storage'] = file_storage
            new['fields']['byte_size'] = i * 100
            new['pk'] = i

            if validate_json or i == 1:
                json_validate(new['fields']['file_characteristics'], json_schema)

            file_test_data_list.append(new)

        else:
            # http POST requests

            new = row_template.copy()
            uuid_str = str(uuid4())

            new['file_name'] = file_name % loop
            new['identifier'] = "pid:urn:" + uuid_str
            new['file_characteristics']['title'] = json_title % loop
            new['file_characteristics']['description'] = json_description % loop
            new['file_storage'] = file_storage

            if validate_json or i == 1:
                json_validate(new['file_characteristics'], json_schema)

            if mode == 'request':
                start = time.time()
                res = requests.post(url, data=json_dumps(new), headers={'Content-Type': 'application/json'},
                                    verify=False)
                end = time.time()
                total_time_elapsed += (end - start)

                if res.status_code == 201:
                    # to be used in creating datasets
                    file_test_data_list.append(res.data)

                if DEBUG:
                    print(res.status_code)
                    if res.status_code == 201:
                        print(res.text)
                    else:
                        print('request failed:')
                        print(res.text)
                        print('------------------------------------------')
                        return
            else:
                # sent later in bulk request
                file_test_data_list.append(new)

        percent = i / float(file_max_rows) * 100.0

        if percent % 10 == 0:
            print("%d%%%s" % (percent, '' if percent == 100.0 else '...'))

    if mode in ("json", 'request_list'):
        print('generated files into a list')
    elif mode == 'request':
        print('collected created objects from responses into a list')
        print('total time elapsed for %d rows: %.3f seconds' % (file_max_rows, total_time_elapsed))

    return file_test_data_list, directory_test_data_list


def get_parent_directory_for_path(directories, file_path, directory_test_data_list, project_identifier):
    dir_name = os.path.dirname(file_path)
    for d in directories:
        if d['fields']['directory_path'] == dir_name and d['fields']['project_identifier'] == project_identifier:
            return d['pk']
    return create_parent_directory_for_path(directories, dir_name, directory_test_data_list, project_identifier)


def create_parent_directory_for_path(directories, file_path, directory_test_data_list, project_identifier):
    """
    Recursively creates the requested directories for file_path
    """
    with open('directory_test_data_template.json') as json_file:
        row_template = json_load(json_file)

    if file_path == '/':
        directory_id = None
    else:
        # the directory where a file or dir belongs to, must be retrieved or created first
        directory_id = get_parent_directory_for_path(directories, file_path, directory_test_data_list,
                                                     project_identifier)

    # all parent dirs have been created - now create the dir that was originally asked for

    new_id = len(directories) + 1

    new = {
        'fields': row_template.copy(),
        'model': 'metax_api.directory',
        'pk': new_id,
    }

    # note: it is possible that parent_directory is null (top-level directories)
    new['fields']['parent_directory'] = directory_id
    new['fields']['directory_name'] = os.path.basename(file_path)
    new['fields']['directory_path'] = file_path
    new['fields']['identifier'] = new['fields']['identifier'] % new_id
    new['fields']['project_identifier'] = project_identifier

    directory_test_data_list.append(new)
    directories.append(new)

    return new_id


def save_test_data(mode, file_storage_list, file_list, directory_list,
                   data_catalogs_list, contract_list, catalog_record_list, batch_size):
    if mode == 'json':

        with open('test_data.json', 'w') as f:
            print('dumping test data as json to metax_api/tests/test_data.json...')
            json_dump(file_storage_list + directory_list + file_list + data_catalogs_list + contract_list +
                      catalog_record_list, f, indent=4, sort_keys=True)

    elif mode == 'request_list':

        total_rows_count = len(file_list)
        start = 0
        end = batch_size or total_rows_count
        i = 0
        total_time_elapsed = 0

        print('uploading to db using POST requests...')

        while end <= total_rows_count:

            batch_start = time.time()
            res = requests.post(url, data=json_dumps(file_list[start:end]),
                                headers={'Content-Type': 'application/json'}, verify=False)
            batch_end = time.time()
            batch_time_elapsed = batch_end - batch_start
            total_time_elapsed += batch_time_elapsed
            print('completed batch #%d: %d rows in %.3f seconds'
                  % (i, batch_size or total_rows_count, batch_time_elapsed))

            start += batch_size
            end += batch_size or total_rows_count
            i += 1

            try:
                res_json = json.loads(res.text)
            except Exception:
                print('something went wrong when loading res.text to json, here is the text:')
                print(res.text)
                return

            if DEBUG and res_json and res_json['failed']:
                print('some insertions failed. here is the object and the errors from the first row:')
                print(res_json['failed'][0])
                return

        print('total time elapsed for %d rows in %d requests: %.3f seconds' % (total_rows_count, i, total_time_elapsed))

    else:
        pass


def generate_data_catalogs(mode, start_idx, data_catalog_max_rows, validate_json, type):
    test_data_catalog_list = []
    json_schema = get_json_schema('datacatalog')

    if mode == 'json':

        with open('data_catalog_test_data_template.json') as json_file:
            row_template = json_load(json_file)

        for i in range(start_idx, start_idx + data_catalog_max_rows):

            new = {
                'fields': deepcopy(row_template),
                'model': "metax_api.datacatalog",
                'pk': i,
            }
            new['fields']['date_modified'] = '2017-06-15T10:07:22Z'
            new['fields']['date_created'] = '2017-05-15T10:07:22Z'
            new['fields']['catalog_json']['identifier'] = generate_test_identifier(dc_type, i)

            if type == 'ida':
                new['fields']['catalog_json']['research_dataset_schema'] = 'ida'
            elif type == 'att':
                new['fields']['catalog_json']['research_dataset_schema'] = 'att'

            if i in (start_idx, start_idx + 1):
                # lets pretend that the first two data catalogs will support versioning.
                dataset_versioning = True
            else:
                dataset_versioning = False
            new['fields']['catalog_json']['dataset_versioning'] = dataset_versioning

            test_data_catalog_list.append(new)

            if validate_json or i == start_idx:
                json_validate(new['fields']['catalog_json'], json_schema)

    return test_data_catalog_list


def generate_contracts(mode, contract_max_rows, validate_json):
    test_contract_list = []
    json_schema = get_json_schema('contract')

    if mode == 'json':

        with open('contract_test_data_template.json') as json_file:
            row_template = json_load(json_file)

        for i in range(1, contract_max_rows + 1):

            new = {
                'fields': deepcopy(row_template),
                'model': "metax_api.contract",
                'pk': i,
            }

            new['fields']['contract_json']['identifier'] = "optional:contract:identifier%d" % i
            new['fields']['contract_json']['title'] = "Title of Contract %d" % i
            new['fields']['contract_json']['organization']['organization_identifier'] = "1234567-%d" % i
            new['fields']['date_modified'] = '2017-06-15T10:07:22Z'
            new['fields']['date_created'] = '2017-05-15T10:07:22Z'
            test_contract_list.append(new)

            if validate_json or i == 1:
                json_validate(new['fields']['contract_json'], json_schema)

    return test_contract_list


def generate_catalog_records(mode, basic_catalog_record_max_rows, data_catalogs_list, contract_list, file_list,
                             validate_json, url, type, test_data_list=[]):
    print('generating {0} catalog records{1}...' .format(type,
                                                         '' if mode in ('json', 'request_list') else ' and uploading'))

    with open('catalog_record_test_data_template.json') as json_file:
        row_template = json_load(json_file)

    total_time_elapsed = 0
    files_start_idx = 1
    data_catalog_id = data_catalogs_list[0]['pk']
    owner_idx = 0
    loop_counter = 0
    start_idx = len(test_data_list) + 1

    for i in range(start_idx, start_idx + basic_catalog_record_max_rows):
        loop_counter += 1
        if mode == 'json':

            json_schema = None
            if type == 'ida':
                json_schema = get_json_schema('ida_dataset')
            elif type == 'att':
                json_schema = get_json_schema('att_dataset')

            new = {
                'fields': row_template.copy(),
                'model': 'metax_api.catalogrecord',
                'pk': i,
            }

            # comment this line. i dare you.
            # for real tho, required to prevent some strange behaving references to old data
            new['fields']['research_dataset'] = row_template['research_dataset'].copy()

            new['fields']['data_catalog'] = data_catalog_id
            new['fields']['research_dataset']['metadata_version_identifier'] = generate_test_identifier(cr_type, i)
            new['fields']['research_dataset']['preferred_identifier'] = "pid:urn:preferred:dataset%d" % i
            new['fields']['date_modified'] = '2017-06-23T10:07:22Z'
            new['fields']['date_created'] = '2017-05-23T10:07:22Z'
            new['fields']['editor'] = {
                'owner_id': catalog_records_owner_ids[owner_idx],
                'creator_id': catalog_records_owner_ids[owner_idx],
                'identifier': 'qvain',
            }
            new['fields']['files'] = []

            owner_idx += 1
            if owner_idx >= len(catalog_records_owner_ids):
                owner_idx = 0

            # add files

            if type == 'ida':
                new['fields']['files'] = []
                dataset_files = []
                total_ida_byte_size = 0
                file_divider = 4

                for j in range(files_start_idx, files_start_idx + files_per_dataset):

                    dataset_files.append({
                        'identifier': file_list[j]['fields']['identifier'],
                        'title': 'File metadata title %d' % (j - 1)
                    })
                    if j < file_divider:
                        # first fifth of files
                        dataset_files[-1]['file_type'] = {
                            "identifier": "http://purl.org/att/es/reference_data/file_type/file_type_text",
                        }
                        dataset_files[-1]['use_category'] = {
                            'identifier': 'source'
                        }

                    elif file_divider <= j < (file_divider * 2):
                        # second fifth of files
                        dataset_files[-1]['file_type'] = {
                            "identifier": "http://purl.org/att/es/reference_data/file_type/file_type_video"
                        }
                        dataset_files[-1]['use_category'] = {
                            'identifier': 'outcome'
                        }
                    elif (file_divider * 2) <= j < (file_divider * 3):
                        # third fifth of files
                        dataset_files[-1]['file_type'] = {
                            "identifier": "http://purl.org/att/es/reference_data/file_type/file_type_image"
                        }
                        dataset_files[-1]['use_category'] = {
                            'identifier': 'publication'
                        }
                    elif (file_divider * 3) <= j < (file_divider * 4):
                        # fourth fifth of files
                        dataset_files[-1]['file_type'] = {
                            "identifier": "http://purl.org/att/es/reference_data/file_type/file_type_source_code"
                        }
                        dataset_files[-1]['use_category'] = {
                            'identifier': 'documentation'
                        }
                    else:
                        # the rest of files
                        dataset_files[-1]['use_category'] = {
                            'identifier': 'configuration'
                        }

                    new['fields']['files'].append(file_list[j - 1]['pk'])
                    total_ida_byte_size += file_list[j - 1]['fields']['byte_size']

                new['fields']['research_dataset']['files'] = dataset_files
                new['fields']['research_dataset']['total_ida_byte_size'] = total_ida_byte_size
                files_start_idx += files_per_dataset

            elif type == 'att':
                new['fields']['research_dataset']['remote_resources'] = [
                    {
                        "title": "Remote resource {0}".format(str(i)),
                        "modified": "2014-01-12T17:11:54Z",
                        "use_category": {"identifier": "outcome"},
                        "checksum": {"algorithm": "SHA-256", "checksum_value": "u5y6f4y68765ngf6ry8n"},
                        "byte_size": i * 512
                    },
                    {
                        "title": "Other remote resource {0}".format(str(i)),
                        "modified": "2013-01-12T11:11:54Z",
                        "use_category": {"identifier": "source"},
                        "checksum": {"algorithm": "SHA-512", "checksum_value": "u3k4kn7n1g56l6rq5a5s"},
                        "byte_size": i * 1024
                    }
                ]
                total_remote_resources_byte_size = 0
                for rr in new['fields']['research_dataset']['remote_resources']:
                    total_remote_resources_byte_size += rr.get('byte_size', 0)
                new['fields']['research_dataset'][
                    'total_remote_resources_byte_size'] = total_remote_resources_byte_size

            if validate_json or i == start_idx:
                json_validate(new['fields']['research_dataset'], json_schema)

            test_data_list.append(new)

        # else:
        #     # http POST requests

        #     new = row_template.copy()

        #     new['file_name'] = file_name % loop
        #     new['identifier'] = uuid_str
        #     new['file_characteristics']['title'] = json_title % loop
        #     new['file_characteristics']['description'] = json_description % loop
        #     new['file_storage'] = file_storage

        #     if validate_json or i == 1:
        #         json_validate(new['file_characteristics'], json_schema)

        #     if mode == 'request':
        #         start = time.time()
        #         res = requests.post(url, data=json_dumps(new),
        # headers={ 'Content-Type': 'application/json' }, verify=False)
        #         end = time.time()
        #         total_time_elapsed += (end - start)

        #         if res.status_code == 201:
        #             # to be used in creating datasets
        #             test_data_list.append(res.data)

        #         if DEBUG:
        #             print(res.status_code)
        #             if res.status_code == 201:
        #                 print(res.text)
        #             else:
        #                 print('request failed:')
        #                 print(res.text)
        #                 print('------------------------------------------')
        #                 return
        #     else:
        #         # sent later in bulk request
        #         test_data_list.append(new)

        percent = loop_counter / float(basic_catalog_record_max_rows) * 100.0

        if percent % 10 == 0:
            print("%d%%%s" % (percent, '' if percent == 100.0 else '...'))

    # set some preservation_state dependent values
    pres_state_value = 1
    for i in range(start_idx, start_idx + 5):

        test_data_list[i]['fields']['preservation_state'] = pres_state_value

        if i > 0:
            test_data_list[i]['fields']['contract'] = 1

        if 3 <= i <= 4:
            test_data_list[i]['fields']['mets_object_identifier'] = ["a", "b", "c"]

        test_data_list[i]['fields']['research_dataset']['curator'] = [{
            "@type": "Person",
            "name": "Rahikainen",
            "identifier": "id:of:curator:rahikainen",
            "member_of": {
                "@type": "Organization",
                "name": {
                    "fi": "MysteeriOrganisaatio"
                }
            }
        }]
        pres_state_value += 1

    # set different owner
    for i in range(start_idx + 5, len(test_data_list)):
        test_data_list[i]['fields']['research_dataset']['curator'] = [{
            "@type": "Person",
            "name": "Jarski",
            "identifier": "id:of:curator:jarski",
            "member_of": {
                "@type": "Organization",
                "name": {
                    "fi": "MysteeriOrganisaatio"
                }
            }
        }]

    # if preservation_state is other than 0, means it has been modified at some point,
    # so set timestamp
    for i in range(start_idx - 1, len(test_data_list)):
        row = test_data_list[i]
        if row['fields']['preservation_state'] != 0:
            row['fields']['preservation_state_modified'] = '2017-05-23T10:07:22.559656Z'

    # add a couple of catalog records with fuller research_dataset fields belonging to both ida and att data catalog
    total_ida_byte_size = 0
    if type == 'ida':
        template = 'catalog_record_test_data_template_full_ida.json'
    elif type == 'att':
        template = 'catalog_record_test_data_template_full_att.json'

    with open(template) as json_file:
        row_template_full = json_load(json_file)

    for j in [0, 1, 2]:
        new = {
            'fields': deepcopy(row_template_full),
            'model': 'metax_api.catalogrecord',
            'pk': len(test_data_list) + 1,
        }
        # for the relation in the db. includes dir id 3, which includes all 20 files

        new['fields']['data_catalog'] = data_catalog_id
        new['fields']['date_modified'] = '2017-09-23T10:07:22Z'
        new['fields']['date_created'] = '2017-05-23T10:07:22Z'
        new['fields']['editor'] = {
            'owner_id': catalog_records_owner_ids[j],
            'creator_id': catalog_records_owner_ids[owner_idx],
        }
        new['fields']['research_dataset']['metadata_version_identifier'] = \
            generate_test_identifier(cr_type, len(test_data_list) + 1)
        new['fields']['research_dataset']['preferred_identifier'] = 'very:unique:urn-%d' % j

        if type == 'ida':
            if j in [0, 1]:
                new['fields']['files'] = [i for i in range(1, 21)]
                file_identifier_0 = file_list[0]['fields']['identifier']
                file_identifier_1 = file_list[1]['fields']['identifier']
                total_ida_byte_size = sum(f['fields']['byte_size'] for f in file_list[0:19])
                new['fields']['research_dataset']['total_ida_byte_size'] = total_ida_byte_size
                new['fields']['research_dataset']['files'][0]['identifier'] = file_identifier_0
                new['fields']['research_dataset']['files'][1]['identifier'] = file_identifier_1
            elif j == 2:
                db_files = []
                directories = []
                files = []

                db_files = [6, 10, 22, 23, 24, 25, 26]
                db_files.extend(list(range(35, 116)))

                files = [
                    {
                        "identifier": "pid:urn:6",
                        "title": "file title 6",
                        "description": "file description 6",
                        "file_type": {
                            "identifier": "http://purl.org/att/es/reference_data/file_type/file_type_video",
                            "definition": [
                                {
                                    "en": "A statement or formal explanation of the meaning of a concept."
                                }
                            ],
                            "in_scheme": [
                                {
                                    "pref_label": {
                                        "en": "The preferred lexical label for a resource"
                                    },
                                    "identifier": "http://uri.of.filetype.concept/scheme"
                                }
                            ]
                        },
                        "use_category": {
                            "identifier": "configuration"
                        }
                    },
                    {
                        "identifier": "pid:urn:10",
                        "title": "file title 10",
                        "description": "file description 10",
                        "file_type": {
                            "identifier": "http://purl.org/att/es/reference_data/file_type/file_type_software",
                            "definition": [
                                {
                                    "en": "A statement or formal explanation of the meaning of a concept."
                                }
                            ],
                            "in_scheme": [
                                {
                                    "pref_label": {
                                        "en": "The preferred lexical label for a resource"
                                    },
                                    "identifier": "http://uri.of.filetype.concept/scheme"
                                }
                            ]
                        },
                        "use_category": {
                            "identifier": "http://purl.org/att/es/reference_data/use_category/use_category_publication"
                        }
                    }
                ]

                directories = [
                    {
                        "identifier": "pid:urn:dir:18",
                        "title": "Phase 1 of science data C",
                        "description": "Description of the directory",
                        "use_category": {
                            "identifier": "http://purl.org/att/es/reference_data/use_category/use_category_outcome"
                        }
                    },
                    {
                        "identifier": "pid:urn:dir:22",
                        "title": "Phase 2 of science data C",
                        "description": "Description of the directory",
                        "use_category": {
                            "identifier": "http://purl.org/att/es/reference_data/use_category/use_category_outcome"
                        }
                    },
                    {
                        "identifier": "pid:urn:dir:12",
                        "title": "Phase 1 01/2018 of Science data A",
                        "description": "Description of the directory",
                        "use_category": {
                            "identifier": "http://purl.org/att/es/reference_data/use_category/use_category_outcome"
                        }
                    },
                    {
                        "identifier": "pid:urn:dir:13",
                        "title": "Science data B",
                        "description": "Description of the directory",
                        "use_category": {
                            "identifier": "http://purl.org/att/es/reference_data/use_category/use_category_source"
                        }
                    },
                    {
                        "identifier": "pid:urn:dir:14",
                        "title": "Other stuff",
                        "description": "Description of the directory",
                        "use_category": {
                            "identifier": "http://purl.org/att/es/reference_data/use_category/use_category_method"
                        }
                    }
                ]

                total_ida_byte_size += sum(file_list[file_pk - 1]['fields']['byte_size'] for file_pk in db_files)

                new['fields']['files'] = db_files
                new['fields']['research_dataset']['files'] = files
                new['fields']['research_dataset']['directories'] = directories
                new['fields']['research_dataset']['total_ida_byte_size'] = total_ida_byte_size
        elif type == 'att':
            total_remote_resources_byte_size = 0
            if 'remote_resources' in new['fields']['research_dataset']:
                for rr in new['fields']['research_dataset']['remote_resources']:
                    total_remote_resources_byte_size += rr.get('byte_size', 0)
                new['fields']['research_dataset']['total_remote_resources_byte_size'] = total_remote_resources_byte_size

        json_validate(new['fields']['research_dataset'], json_schema)
        test_data_list.append(new)

    if mode in ("json", 'request_list'):
        print('generated catalog records into a list')
    elif mode == 'request':
        print('collected created objects from responses into a list')
        print('total time elapsed for %d rows: %.3f seconds' % (basic_catalog_record_max_rows, total_time_elapsed))

    return test_data_list


def generate_alt_catalog_records(test_data_list):
    #
    # create a couple of alternate records for record with id 10
    #
    # note, these alt records wont have an editor-field set, since they presumably
    # originated to metax from somewhere else than qvain (were harvested).
    #
    alternate_record_set = {
        'fields': {},
        'model': 'metax_api.alternaterecordset',
        'pk': 1,
    }

    # first record belongs to alt record set
    test_data_list[9]['fields']['alternate_record_set'] = 1

    # create one other record
    alt_rec = deepcopy(test_data_list[9])
    alt_rec['pk'] = test_data_list[-1]['pk'] + 1
    alt_rec['fields']['research_dataset']['preferred_identifier'] = test_data_list[9]['fields']['research_dataset'][
        'preferred_identifier']
    alt_rec['fields']['research_dataset']['metadata_version_identifier'] += '-alt-1'
    alt_rec['fields']['data_catalog'] = 2
    alt_rec['fields']['alternate_record_set'] = 1
    test_data_list.append(alt_rec)

    # create second other record
    alt_rec = deepcopy(test_data_list[9])
    alt_rec['pk'] = test_data_list[-1]['pk'] + 1
    alt_rec['fields']['research_dataset']['preferred_identifier'] = test_data_list[9]['fields']['research_dataset'][
        'preferred_identifier']
    alt_rec['fields']['research_dataset']['metadata_version_identifier'] += '-alt-2'
    alt_rec['fields']['data_catalog'] = 3
    alt_rec['fields']['alternate_record_set'] = 1
    test_data_list.append(alt_rec)

    # alternate record set must exist before importing catalog records, so prepend it
    test_data_list.insert(0, alternate_record_set)
    return test_data_list

print('generating %d test file rows' % file_max_rows)
print('mode: %s' % mode)
if 'request' in mode:
    print('POST requests to url: %s' % url)
print('validate json: %s' % str(validate_json))
print('generating %d test file storage rows' % file_storage_max_rows)
print('DEBUG: %s' % str(DEBUG))

contract_list = generate_contracts(mode, contract_max_rows, validate_json)
file_storage_list = generate_file_storages(mode, file_storage_max_rows)
file_list, directory_list = generate_files(mode, file_storage_list, validate_json, url)

ida_data_catalogs_list = generate_data_catalogs(mode, 1, ida_data_catalog_max_rows, validate_json, 'ida')
att_data_catalogs_list = generate_data_catalogs(mode, ida_data_catalog_max_rows + 1, att_data_catalog_max_rows,
                                                validate_json, 'att')

catalog_record_list = generate_catalog_records(mode, ida_catalog_record_max_rows, ida_data_catalogs_list,
                                               contract_list, file_list, validate_json, url, 'ida')
catalog_record_list = generate_catalog_records(mode, att_catalog_record_max_rows, att_data_catalogs_list,
                                               contract_list, [], validate_json, url, 'att', catalog_record_list)

catalog_record_list = generate_alt_catalog_records(catalog_record_list)
save_test_data(mode, file_storage_list, directory_list, file_list, ida_data_catalogs_list + att_data_catalogs_list,
               contract_list, catalog_record_list, batch_size)

print('done')
