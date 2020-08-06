# This file is part of the Metax API service
#
# Copyright 2017-2018 Ministry of Education and Culture, Finland
#
# :author: CSC - IT Center for Science Ltd., Espoo Finland <servicedesk@csc.fi>
# :license: MIT

from django.core.management import call_command
from rest_framework import status
from rest_framework.test import APITestCase

from metax_api.models import DataCatalog
from metax_api.services import RedisCacheService as cache
from metax_api.tests.utils import test_data_file_path, TestClassUtils


class DataCatalogApiWriteCommon(APITestCase, TestClassUtils):
    @classmethod
    def setUpClass(cls):
        """
        Loaded only once for test cases inside this class.
        """
        call_command('loaddata', test_data_file_path, verbosity=0)
        super(DataCatalogApiWriteCommon, cls).setUpClass()

    def setUp(self):
        self.new_test_data = self._get_object_from_test_data('datacatalog')
        self.new_test_data.pop('id')
        self.new_test_data['catalog_json']['identifier'] = 'new-data-catalog'
        self._use_http_authorization()


class DataCatalogApiWriteBasicTests(DataCatalogApiWriteCommon):

    def test_identifier_is_auto_generated(self):
        response = self.client.post('/rest/datacatalogs', self.new_test_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED)
        self.assertNotEqual(response.data['catalog_json'].get('identifier', None), None, 'identifier should be created')

    def test_research_dataset_schema_missing_ok(self):
        self.new_test_data['catalog_json'].pop('research_dataset_schema', None)
        response = self.client.post('/rest/datacatalogs', self.new_test_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED)

    def test_research_dataset_schema_not_found_error(self):
        self.new_test_data['catalog_json']['research_dataset_schema'] = 'notfound'
        response = self.client.post('/rest/datacatalogs', self.new_test_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST, response.data)

    def test_disallow_versioning_in_harvested_catalogs(self):
        self.new_test_data['catalog_json']['dataset_versioning'] = True
        self.new_test_data['catalog_json']['harvested'] = True
        response = self.client.post('/rest/datacatalogs', self.new_test_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual('versioning' in response.data['detail'][0], True, response.data)

    def test_create_identifier_already_exists(self):
        response = self.client.post('/rest/datacatalogs', self.new_test_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED)
        response = self.client.post('/rest/datacatalogs', self.new_test_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual('already exists' in response.data['catalog_json']['identifier'][0],
            True, response.data)

    def test_delete(self):
        response = self.client.delete('/rest/datacatalogs/1')
        self.assertEqual(response.status_code, status.HTTP_204_NO_CONTENT)

        dc_deleted = DataCatalog.objects_unfiltered.get(pk=1)
        self.assertEqual(dc_deleted.removed, True)
        self.assertEqual(dc_deleted.date_modified, dc_deleted.date_removed, 'date_modified should be updated')


class DataCatalogApiWriteReferenceDataTests(DataCatalogApiWriteCommon):
    """
    Tests related to reference_data validation and data catalog fields population
    from reference_data, according to given uri or code as the value.
    """

    def test_create_data_catalog_with_invalid_reference_data(self):
        dc = self.new_test_data['catalog_json']
        dc['field_of_science'][0]['identifier'] = 'nonexisting'
        dc['language'][0]['identifier'] = 'nonexisting'
        dc['access_rights']['access_type'][0]['identifier'] = 'nonexisting'
        dc['access_rights']['license'][0]['identifier'] = 'nonexisting'
        response = self.client.post('/rest/datacatalogs', self.new_test_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual('catalog_json' in response.data.keys(), True)
        self.assertEqual(len(response.data['catalog_json']), 4)

    def test_create_data_catalog_populate_fields_from_reference_data(self):
        """
        1) Insert codes from cached reference data to data catalog identifier fields
           that will be validated, and then populated
        2) Check that that the values in data catalog identifier fields are changed from
           codes to uris after a successful create
        3) Check that labels have also been copied to data catalog to their approriate fields
        """
        refdata = cache.get('reference_data')['reference_data']
        orgdata = cache.get('reference_data')['organization_data']
        refs = {}

        data_types = [
            'access_type',
            'field_of_science',
            'language',
            'license',
        ]

        # the values in these selected entries will be used throghout the rest of the test case
        for dtype in data_types:
            entry = refdata[dtype][0]
            refs[dtype] = {
                'code': entry['code'],
                'uri': entry['uri'],
                'label': entry.get('label', None),
            }

        refs['organization'] = {
            'uri': orgdata['organization'][1]['uri'],
            'code': orgdata['organization'][1]['code'],
            'label': orgdata['organization'][1]['label'],
        }

        # replace the relations with objects that have only the identifier set with code as value,
        # to easily check that label was populated (= that it appeared in the dataset after create)
        # without knowing its original value from the generated test data
        dc = self.new_test_data['catalog_json']
        dc['field_of_science'][0] = {'identifier': refs['field_of_science']['code']}
        dc['language'][0] = {'identifier': refs['language']['code']}
        dc['access_rights']['access_type'][0] = {'identifier': refs['access_type']['code']}
        dc['access_rights']['license'][0] = {'identifier': refs['license']['code']}

        # these have other required fields, so only update the identifier with code
        dc['publisher']['identifier'] = refs['organization']['code']
        dc['access_rights']['has_rights_related_agent'][0]['identifier'] = refs['organization']['code']

        response = self.client.post('/rest/datacatalogs', self.new_test_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)
        self.assertEqual('catalog_json' in response.data.keys(), True)

        new_dc = response.data['catalog_json']
        self._assert_uri_copied_to_identifier(refs, new_dc)
        self._assert_label_copied_to_pref_label(refs, new_dc)
        self._assert_label_copied_to_title(refs, new_dc)
        self._assert_label_copied_to_name(refs, new_dc)

    def _assert_uri_copied_to_identifier(self, refs, new_dc):
        self.assertEqual(refs['field_of_science']['uri'], new_dc['field_of_science'][0]['identifier'])
        self.assertEqual(refs['language']['uri'], new_dc['language'][0]['identifier'])
        self.assertEqual(refs['access_type']['uri'], new_dc['access_rights']['access_type'][0]['identifier'])
        self.assertEqual(refs['license']['uri'], new_dc['access_rights']['license'][0]['identifier'])
        self.assertEqual(refs['organization']['uri'], new_dc['publisher']['identifier'])
        self.assertEqual(refs['organization']['uri'],
                         new_dc['access_rights']['has_rights_related_agent'][0]['identifier'])

    def _assert_label_copied_to_pref_label(self, refs, new_dc):
        self.assertEqual(refs['field_of_science']['label'], new_dc['field_of_science'][0].get('pref_label', None))
        self.assertEqual(refs['access_type']['label'],
                         new_dc['access_rights']['access_type'][0].get('pref_label', None))

    def _assert_label_copied_to_title(self, refs, new_dc):
        self.assertEqual(refs['license']['label'], new_dc['access_rights']['license'][0].get('title', None))

    def _assert_label_copied_to_name(self, refs, new_dc):
        self.assertEqual(refs['organization']['label'], new_dc['publisher']['name'])
