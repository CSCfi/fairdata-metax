from base64 import b64encode
from json import dumps
import requests
import yaml
import urllib3


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
    with open('/home/metax-user/app_config') as app_config:
        app_config_dict = yaml.load(app_config)

    for u in app_config_dict['API_USERS']:
        if u['username'] == 'metax':
            return {
                'Authorization': 'Basic %s'
                % b64encode(bytes('%s:%s' % (u['username'], u['password']), 'utf-8')).decode('utf-8')
            }


def retrieve_and_update_all_datasets_in_db():
    print('-- begin retrieving and updating all datasets in the db --')
    headers = { 'Content-type': 'application/json' }
    headers.update(get_auth_header())

    print('retrieving all urn_identifiers...')
    response = requests.get('https://localhost/rest/datasets/urn_identifiers', headers=headers, verify=False)
    if response.status_code != 200:
        raise Exception(response.content)

    records = []
    count = 0

    print('retrieving details of datasets...')

    for urn in response.json():
        response = requests.get('https://localhost/rest/datasets/%s' % urn,
            headers=headers, verify=False)

        if response.status_code == 200:
            records.append(response.json())
            count += 1
        else:
            print(response.status_code)
            raise Exception(response.content)

    print('updating %d datasets using bulk update...' % count)

    http = urllib3.PoolManager()
    response = http.urlopen('PUT', 'https://localhost/rest/datasets', headers=headers,
        body=dumps(records))

    if response.status not in (200, 201, 204):
        print(response.status)
        raise Exception(response.data)
    else:
        print('-- done --')


if __name__ == '__main__':
    retrieve_and_update_all_datasets_in_db()
