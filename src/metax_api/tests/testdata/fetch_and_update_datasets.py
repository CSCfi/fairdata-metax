# This file is part of the Metax API service
#
# Copyright 2017-2018 Ministry of Education and Culture, Finland
#
# :author: CSC - IT Center for Science Ltd., Espoo Finland <servicedesk@csc.fi>
# :license: MIT

from base64 import b64encode
from json import dumps
from time import sleep

import django
import requests
import urllib3
from django.conf import settings

django.setup()

"""
Since test data imported into the db using the loaddata command has not had
the "reference data population" treatment, this script should be executed always
after loaddata, to retrieve all datasets in the db, and PUT-update all of them back
without changing anything.

The PUT-update will trigger a reference_data population in the backend, transforming
the "incomplete" test data to what it would normally look like if somebody had
created it using a normal POST request.
"""


urllib3.disable_warnings()


def get_auth_header():
    for u in settings.API_USERS:
        if u['username'] == 'metax':
            return {
                'Authorization': 'Basic %s'
                % b64encode(bytes('%s:%s' % (u['username'], u['password']), 'utf-8')).decode('utf-8')
            }

def get_test_datasets():
    print('retrieving test datasets...')
    response = requests.get('https://localhost/rest/datasets?metadata_owner_org=abc-org-123&pagination=false',
        headers=headers, verify=False)
    if response.status_code != 200:
        raise Exception(response.content)

    records = response.json()

    if not records:
        print('Received no records. Quiting...')
        return

    print('Filtering out possible non-test datasets...')
    test_records = []
    for record in records:
        if dataset_is_testdata(record):
            test_records.append(record)

    return test_records

def dataset_is_testdata(record):
    # Test datasets have fixed attributes so these should verify that it is a test dataset
    # Other tests could be added but these should be sufficient
    end = f'd{record["id"]}' if record['id'] < 10 else f'{record["id"]}'

    if record['identifier'] != f'cr955e904-e3dd-4d7e-99f1-3fed446f96{end}' or \
            record['research_dataset']['creator'][0]['name'] != 'Teppo Testaaja':
        return False

    return True

def retrieve_and_update_all_datasets_in_db(headers):
    print('-- begin retrieving and updating test datasets --')

    test_records = get_test_datasets()
    # dont want to create new versions from datasets for this operation,
    # so use parameter preserve_version
    print('updating the test datasets...')
    response = requests.put('https://localhost/rest/datasets?preserve_version',
        headers=headers, data=dumps(test_records), verify=False)

    if response.status_code not in (200, 201, 204):
        print(response.status_code)
        raise Exception(response.text)
    elif response.text and len(response.json().get('failed', [])) > 0:
        for fail in response.json().get('failed'):
            raise Exception(fail)

    print('-- done --')


def retrieve_and_update_all_data_catalogs_in_db(headers):
    print('-- begin retrieving and updating all data catalogs in the db --')

    print('retrieving all data catalog IDs...')
    response = requests.get('https://localhost/rest/datacatalogs?limit=100', headers=headers, verify=False)
    if response.status_code != 200:
        raise Exception(response.content)

    data_catalog_ids = []
    for dc in response.json().get('results', []):
        data_catalog_ids.append(dc.get('id'))

    print('retrieving details of data catalogs and updating %d data catalogs...' % len(data_catalog_ids))

    for dc_id in data_catalog_ids:
        response = requests.get('https://localhost/rest/datacatalogs/%s' % dc_id, headers=headers, verify=False)

        if response.status_code == 200:
            update_response = requests.put('https://localhost/rest/v2/datacatalogs/%s' % dc_id,
                                           headers=headers, json=response.json(), verify=False)
            if update_response.status_code not in (200, 201, 204):
                print(response.status_code)
                raise Exception(response.text)
        else:
            print(response.status_code)
            raise Exception(response.content)

    print('-- done --')


def update_directory_byte_sizes_and_file_counts(headers):
    print('-- begin updating byte sizes and file counts in all dirs in all projects --')
    response = requests.get('https://localhost/rest/directories/update_byte_sizes_and_file_counts',
        headers=headers, verify=False)
    if response.status_code not in (200, 201, 204):
        raise Exception(response.text)
    print('-- done --')


def update_ida_datasets_total_files_byte_size(headers):
    print('-- begin updating IDA CR total ida byte sizes --')
    response = requests.get('https://localhost/rest/datasets/update_cr_total_files_byte_sizes',
        headers=headers, verify=False)
    if response.status_code not in (200, 201, 204):
        raise Exception(response.text)
    print('-- done --')


def update_cr_directory_browsing_data(headers):
    print('-- begin updating test CR directory byte sizes and file counts --')
    test_records = get_test_datasets()

    for r in test_records:
        response = requests.get(f'https://localhost/rest/datasets/update_cr_directory_browsing_data?id={r["id"]}',
            headers=headers, verify=False)
        if response.status_code not in (200, 201, 204):
            raise Exception(response.text)
    print('-- done --')


if __name__ == '__main__':
    headers = {'Content-type': 'application/json'}
    headers.update(get_auth_header())

    for i in range(1, 10):
        response = requests.get('https://localhost/rest/datasets/1', headers=headers, verify=False)
        if response.status_code == 200:
            break
        sleep(1)
    else:
        print("Unable to GET dataset with pk 1, aborting... reason:")
        print(response.content)
        import sys
        sys.exit(1)

    retrieve_and_update_all_datasets_in_db(headers)
    retrieve_and_update_all_data_catalogs_in_db(headers)
    update_directory_byte_sizes_and_file_counts(headers)
    update_ida_datasets_total_files_byte_size(headers)
    update_cr_directory_browsing_data(headers)
