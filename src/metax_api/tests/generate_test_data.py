from json import load as json_load
from json import dump as json_dump
from jsonschema import validate as json_validate
from datetime import datetime
from os import path
from uuid import uuid4

"""
Execute this file to generate max_rows amount of rows to metax_api_file table. Uses
file_test_data_template.json as template, and slightly modifies fields each loop.
Saves generated rows as a json list to file file_test_data_list.json.

Execute the following to load the data to django db:
python manage.py loaddata metax_api/tests/file_test_data_list.json
"""

max_rows = 100
fill_zeros = len(str(max_rows))
fill_zeros = fill_zeros if fill_zeros > 7 else 7
test_data_list = []

with open(path.dirname(path.realpath(__file__)) + '/../api/base/schemas/json_schema_file.json') as f:
    json_schema = json_load(f)

print('generating test %d rows...' % max_rows)

for i in range(1, max_rows):

    with open('file_test_data_template.json') as json_file:

        loop = str(i).zfill(fill_zeros)
        row_template = json_load(json_file)
        json_template = row_template['json'].copy()

        new = {
            'fields': row_template,
            'model': 'metax_api.file',
            'pk': str(uuid4()),
        }

        new['fields']['file_name'] = row_template['file_name'] % loop
        new['fields']['modified_by_api'] = '2017-05-23 10:07:22.559656+03'
        new['fields']['created_by_api'] = '2017-05-23 10:07:22.559656+03'
        new['fields']['json']['identifier'] = json_template['identifier'] % loop
        new['fields']['json']['fileName'] = json_template['fileName'] % loop
        new['fields']['json']['url'] = json_template['url'] % loop
        new['fields']['json']['replicationPath'] = json_template['replicationPath'] % loop

        json_validate(new['fields']['json'], json_schema)
        test_data_list.append(new)

if test_data_list:
    with open('file_test_data_list_django.json', 'w') as f:
        print('dumping test data to metax_api/tests/file_test_data_list.json...')
        json_dump(test_data_list, f, indent=4)
        print('done')
else:
    print('test data list is empty? something went wrong')
