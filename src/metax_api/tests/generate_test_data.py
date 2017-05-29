from hashlib import sha256
import json
from json import load as json_load
from json import dump as json_dump
from json import dumps as json_dumps
from jsonschema import validate as json_validate
from os import path
import requests
import urllib3
from uuid import uuid4

"""
Execute this file to generate max_rows amount of rows to metax_api_file table. Uses
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
  i.e. max_rows = 10000000, patch_size = 10000 -> do a list POST request every patch_size rows
"""

# how many rows to generate
max_rows = 100

# mode: json for json-file, request for request per row, request_list for bulk post
mode = 'request_list'

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

test_data_list = []

with open(path.dirname(path.realpath(__file__)) + '/../api/base/schemas/json_schema_file.json') as f:
    json_schema = json_load(f)

print('generating %d test rows:' % max_rows)
print('mode: %s' % mode)
if 'request' in mode:
    print('POST requests to url: %s' % url)
print('validate json: %s' % str(validate_json))
print('DEBUG: %s' % str(DEBUG))

print('generating...')

with open('file_test_data_template.json') as json_file:

    row_template = json_load(json_file)
    json_template = row_template['file_characteristics'].copy()

    file_name = row_template['file_name']
    identifier = row_template['identifier']
    file_path = row_template['file_path']
    download_url = row_template['download_url']
    json_title = json_template['title']
    json_description = json_template['description']

    for i in range(1, max_rows + 1):

        loop = str(i).zfill(fill_zeros)

        if mode == 'json':

            new = {
                'fields': row_template.copy(),
                'model': 'metax_api.file',
            }

            new['fields']['file_name'] = file_name % loop
            new['fields']['identifier'] = identifier % loop
            new['fields']['identifier_sha256'] = int(sha256(new['fields']['identifier'].encode('utf-8')).hexdigest(), 16)
            new['fields']['file_path'] = file_path % loop
            new['fields']['download_url'] = download_url % loop
            new['fields']['modified_by_api'] = '2017-05-23 10:07:22.559656+03'
            new['fields']['created_by_api'] = '2017-05-23 10:07:22.559656+03'
            new['fields']['file_characteristics']['title'] = json_title % loop
            new['fields']['file_characteristics']['description'] = json_description % loop
            new['pk'] = str(uuid4())

            if validate_json:
                json_validate(new['fields']['file_characteristics'], json_schema)

            test_data_list.append(new)

        else:
            # http POST requests

            new = row_template.copy()

            new['file_name'] = file_name % loop
            new['identifier'] = identifier % loop
            new['file_path'] = file_path % loop
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

        percent = i / float(max_rows) * 100.0
        if percent % 10 == 0:
            print("%d%%..." % percent)

if mode == 'json':

    if test_data_list:
        with open('file_test_data_list.json', 'w') as f:
            print('dumping test data as json to metax_api/tests/file_test_data_list.json...')
            json_dump(test_data_list, f, indent=4)
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
