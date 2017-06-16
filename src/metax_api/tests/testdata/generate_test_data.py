import json
from json import load as json_load
from json import dump as json_dump
from json import dumps as json_dumps
from jsonschema import validate as json_validate
import os
import requests
import sys
import time
from uuid import uuid4
import urllib3

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils import get_json_schema

"""
Execute this file to generate file_max_rows amount of rows to metax_api_file table. Uses
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
file_max_rows = 100

# how many filestorage rows to generate
file_storage_max_rows = 3

dataset_catalog_max_rows = 3

catalog_record_max_rows = 10

files_per_dataset = 3

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
                    'modified_by_api': '2017-05-23T10:07:22.559656Z',
                    'created_by_api': '2017-05-23T10:07:22.559656Z',
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


def generate_files(mode, file_max_rows, test_file_storage_list, validate_json, url):

    print('generating files%s...' % ('' if mode in ('json', 'request_list') else ' and uploading'))

    with open('file_test_data_template.json') as json_file:
        row_template = json_load(json_file)

    test_data_list = []
    json_template = row_template['file_characteristics'].copy()
    file_name = row_template['file_name']
    download_url = row_template['download_url']
    json_title = json_template['title']
    json_description = json_template['description']
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

        loop = str(i)

        if mode == 'json':

            new = {
                'fields': row_template.copy(),
                'model': 'metax_api.file',
            }

            new['fields']['file_name'] = file_name % loop
            new['fields']['identifier'] = "pid:urn:" + loop
            new['fields']['download_url'] = download_url % loop
            new['fields']['modified_by_api'] = '2017-05-23T10:07:22.559656Z'
            new['fields']['created_by_api'] = '2017-05-23T10:07:22.559656Z'
            new['fields']['file_characteristics']['title'] = json_title % loop
            new['fields']['file_characteristics']['description'] = json_description % loop
            new['fields']['file_storage'] = file_storage
            new['pk'] = i

            if validate_json or i == 1:
                json_validate(new['fields']['file_characteristics'], json_schema)

            test_data_list.append(new)

        else:
            # http POST requests

            new = row_template.copy()
            uuid_str = str(uuid4())

            new['file_name'] = file_name % loop
            new['identifier'] = "pid:urn:" + uuid_str
            new['download_url'] = download_url % loop
            new['file_characteristics']['title'] = json_title % loop
            new['file_characteristics']['description'] = json_description % loop
            new['file_storage'] = file_storage

            if validate_json or i == 1:
                json_validate(new['file_characteristics'], json_schema)

            if mode == 'request':
                start = time.time()
                res = requests.post(url, data=json_dumps(new), headers={ 'Content-Type': 'application/json' }, verify=False)
                end = time.time()
                total_time_elapsed += (end - start)

                if res.status_code == 201:
                    # to be used in creating datasets
                    test_data_list.append(res.data)

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
                test_data_list.append(new)

        percent = i / float(file_max_rows) * 100.0

        if percent % 10 == 0:
            print("%d%%%s" % (percent, '' if percent == 100.0 else '...'))

    if mode in ("json", 'request_list'):
        print('generated files into a list')
    elif mode == 'request':
        print('collected created objects from responses into a list')
        print('total time elapsed for %d rows: %.3f seconds' % (file_max_rows, total_time_elapsed))

    return test_data_list


def save_test_data(mode, file_storage_list, file_list, dataset_catalogs_list, catalog_record_list, batch_size):
    if mode == 'json':

        with open('test_data.json', 'w') as f:
            print('dumping test data as json to metax_api/tests/test_data.json...')
            json_dump(file_storage_list + file_list + dataset_catalogs_list + catalog_record_list, f, indent=4)

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
                                headers={ 'Content-Type': 'application/json' }, verify=False)
            batch_end = time.time()
            batch_time_elapsed = batch_end - batch_start
            total_time_elapsed += batch_time_elapsed
            print('completed batch #%d: %d rows in %.3f seconds' % (i, batch_size or total_rows_count, batch_time_elapsed))

            start += batch_size
            end += batch_size or total_rows_count
            i += 1

            try:
                res_json = json.loads(res.text)
            except Exception as e:
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


def generate_dataset_catalogs(mode, dataset_catalog_max_rows):

    test_dataset_catalog_list = []

    if mode == 'json':

        with open('dataset_catalog_test_data_template.json') as json_file:
            row_template = json_load(json_file)

        for i in range(1, file_storage_max_rows + 1):

            new = {
                'fields': row_template.copy(),
                'model': "metax_api.datasetcatalog",
                'pk': i,
            }
            new['fields']['modified_by_api'] = '2017-05-15T10:07:22.559656Z'
            new['fields']['created_by_api'] = '2017-05-15T10:07:22.559656Z'
            new['fields']['catalog_json']['identifier'] = "pid:urn:catalog%d" % i
            test_dataset_catalog_list.append(new)

    return test_dataset_catalog_list


def generate_catalog_records(mode, catalog_record_max_rows, dataset_catalogs_list, file_list, validate_json, url):

    print('generating catalog records%s...' % ('' if mode in ('json', 'request_list') else ' and uploading'))

    with open('catalog_record_test_data_template.json') as json_file:
        row_template = json_load(json_file)

    test_data_list = []
    json_schema = get_json_schema('dataset')
    total_time_elapsed = 0
    files_start_idx = 0

    if mode == "json":
        dataset_catalog = dataset_catalogs_list[0]['pk']

    for i in range(1, catalog_record_max_rows + 1):

        if mode == 'json':

            new = {
                'fields': row_template.copy(),
                'model': 'metax_api.catalogrecord',
                'pk': i,
            }

            # comment this line. i dare you.
            # for real tho, required to prevent some strange behaving references to old data
            new['fields']['research_dataset'] = row_template['research_dataset'].copy()

            new['fields']['identifier'] = "pid:urn:cr%d" % i
            new['fields']['dataset_catalog'] = dataset_catalog
            new['fields']['research_dataset']['identifier'] = "pid:urn:cr%d" % i
            new['fields']['modified_by_api'] = '2017-05-23T10:07:22.559656Z'
            new['fields']['created_by_api'] = '2017-05-23T10:07:22.559656Z'
            new['fields']['files'] = []

            files = []

            for j in range(files_start_idx, files_start_idx + files_per_dataset):
                files.append({
                    'identifier': file_list[j]['fields']['identifier'],
                    'title': 'File metadata title %d' % j,
                })
                new['fields']['files'].append(file_list[j]['pk'])

            new['fields']['research_dataset']['files'] = files
            files_start_idx += files_per_dataset

            if validate_json or i == 1:
                json_validate(new['fields']['research_dataset'], json_schema)

            test_data_list.append(new)

        # else:
        #     # http POST requests

        #     new = row_template.copy()

        #     new['file_name'] = file_name % loop
        #     new['identifier'] = uuid_str
        #     new['download_url'] = download_url % loop
        #     new['file_characteristics']['title'] = json_title % loop
        #     new['file_characteristics']['description'] = json_description % loop
        #     new['file_storage'] = file_storage

        #     if validate_json or i == 1:
        #         json_validate(new['file_characteristics'], json_schema)

        #     if mode == 'request':
        #         start = time.time()
        #         res = requests.post(url, data=json_dumps(new), headers={ 'Content-Type': 'application/json' }, verify=False)
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

        percent = i / float(catalog_record_max_rows) * 100.0

        if percent % 10 == 0:
            print("%d%%%s" % (percent, '' if percent == 100.0 else '...'))

    if mode in ("json", 'request_list'):
        print('generated catalog records into a list')
    elif mode == 'request':
        print('collected created objects from responses into a list')
        print('total time elapsed for %d rows: %.3f seconds' % (catalog_record_max_rows, total_time_elapsed))
    return test_data_list


print('generating %d test file rows' % file_max_rows)
print('mode: %s' % mode)
if 'request' in mode:
    print('POST requests to url: %s' % url)
print('validate json: %s' % str(validate_json))
print('generating %d test file storage rows' % file_storage_max_rows)
print('DEBUG: %s' % str(DEBUG))

file_storage_list = generate_file_storages(mode, file_storage_max_rows)
file_list = generate_files(mode, file_max_rows, file_storage_list, validate_json, url)
dataset_catalogs_list = generate_dataset_catalogs(mode, dataset_catalog_max_rows)
catalog_record_list = generate_catalog_records(mode, catalog_record_max_rows, dataset_catalogs_list, file_list, validate_json, url)

save_test_data(mode, file_storage_list, file_list, dataset_catalogs_list, catalog_record_list, batch_size)

print('done')
