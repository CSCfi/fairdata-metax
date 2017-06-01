import json
from json import load as json_load
from json import dump as json_dump
from json import dumps as json_dumps
from jsonschema import validate as json_validate
import requests
import urllib3
from uuid import uuid4

from utils import get_json_schema

"""
Execute this file to generate file_max_rows amount of rows to metax_api_file table. Uses
file_test_data_template.json as template, and slightly modifies fields each loop.

Data input options:

- Generate rows as a json list to file file_test_data_list.json. Note that manage.py wont
  handle very large files very well, i.e. dont try to load half a million rows in one file.
  Execute the following to load the json data to django db:
  python manage.py loaddata metax_api/tests/file_test_data_list.json

- Send generated rows directly as http POST requests to your api.
  Optionally send either one by one, or in bulk in one request. Note that nginx max request
  size restriction applies.

todo:
- patch size for requests / files to create very large sets of data?
  i.e. file_max_rows = 10000000, patch_size = 10000 -> do a list POST request every patch_size rows
"""

# how many file rows to generate
file_max_rows = 10

# how many filestorage rows to generate
file_storage_max_rows = 3

# mode: json for json-file, request for request per row, request_list for bulk post
mode = 'json'

# leading zeros for generated file names etc when looping
fill_zeros = 10

# very slow with a large number of rows
validate_json = False

# where to send POST requests if using it
url = 'https://metax.csc.local/rest/files/'

# print tasty debug information from POST requests
DEBUG = False

#  to disable self-signed certificate warnings
urllib3.disable_warnings()

if validate_json:
    json_schema = get_json_schema('file')

test_data_list = []
test_file_storage_list = []


print('generating %d test file rows:' % file_max_rows)
print('mode: %s' % mode)
if 'request' in mode:
    print('POST requests to url: %s' % url)
print('validate json: %s' % str(validate_json))
print('generating %d test file storage rows:' % file_storage_max_rows)
print('DEBUG: %s' % str(DEBUG))

print('generating...')

with open('file_storage_test_data_template.json') as json_file:

    row_template = json_load(json_file)
    title = row_template['file_storage_json']['title']
    identifier = row_template['file_storage_json']['identifier']

    for i in range(1, file_storage_max_rows + 1):

        # if mode == 'json':
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
            'pk': str(uuid4())
        }
        test_file_storage_list.append(new)

with open('file_test_data_template.json') as json_file:

    row_template = json_load(json_file)
    json_template = row_template['file_characteristics'].copy()

    file_name = row_template['file_name']
    identifier = row_template['identifier']
    file_path = row_template['file_path']
    download_url = row_template['download_url']
    json_title = json_template['title']
    json_description = json_template['description']
    file_storage_id = test_file_storage_list[0]['pk']

    for i in range(1, file_max_rows + 1):

        loop = str(i).zfill(fill_zeros)

        if mode == 'json':

            new = {
                'fields': row_template.copy(),
                'model': 'metax_api.file',
            }

            new['fields']['file_name'] = file_name % loop
            new['fields']['identifier'] = identifier % loop
            new['fields']['download_url'] = download_url % loop
            new['fields']['modified_by_api'] = '2017-05-23T10:07:22.559656Z'
            new['fields']['created_by_api'] = '2017-05-23T10:07:22.559656Z'
            new['fields']['file_characteristics']['title'] = json_title % loop
            new['fields']['file_characteristics']['description'] = json_description % loop
            new['fields']['file_storage_id'] = file_storage_id
            new['pk'] = str(uuid4())

            if validate_json:
                json_validate(new['fields']['file_characteristics'], json_schema)

            test_data_list.append(new)

        else:
            # http POST requests

            new = row_template.copy()

            new['file_name'] = file_name % loop
            new['identifier'] = identifier % loop
            new['download_url'] = download_url % loop
            new['file_characteristics']['title'] = json_title % loop
            new['file_characteristics']['description'] = json_description % loop

            if validate_json:
                json_validate(new['file_characteristics'], json_schema)

            if mode == 'request':
                res = requests.post(url, data=json_dumps(new), headers={ 'Content-Type': 'application/json' }, verify=False)

                if DEBUG:
                    print(res.status_code)
                    if res.status_code == 201:
                        print(res.text)
                    else:
                        print('request failed:')
                        print(res.text)
                        print('------------------------------------------')
            else:
                test_data_list.append(new)

        percent = i / float(file_max_rows) * 100.0
        if percent % 10 == 0:
            print("%d%%..." % percent)

if mode == 'json':

    if test_data_list:
        with open('file_test_data_list.json', 'w') as f:
            print('dumping test data as json to metax_api/tests/file_test_data_list.json...')
            json_dump(test_file_storage_list + test_data_list, f, indent=4)
    else:
        print('test data list is empty? something went wrong')

elif mode == 'request_list':

    print('sending %d rows in a single request...' % len(test_data_list))

    res = requests.post(url, data=json_dumps(test_data_list), headers={ 'Content-Type': 'application/json' }, verify=False)
    try:
        res_json = json.loads(res.text)
    except Exception as e:
        print('something went wrong when loading res.text to json, here is the text:')
        print(res.text)

    if DEBUG and res_json and res_json['failed']:
        print('some insertions failed. here is the object and the errors from the first row:')
        print(res_json['failed'][0])

else:
    pass

print('done')
