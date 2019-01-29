# This file is part of the Metax API service
#
# Copyright 2017-2018 Ministry of Education and Culture, Finland
#
# :author: CSC - IT Center for Science Ltd., Espoo Finland <servicedesk@csc.fi>
# :license: MIT

from copy import deepcopy

from django.core.management import call_command
# from django.db import connection
from rest_framework import status
from rest_framework.test import APITestCase

from metax_api.models import CatalogRecord
# from metax_api.models import CatalogRecord, DataCatalog, File
from metax_api.models.catalog_record import ACCESS_TYPES
from metax_api.tests.utils import TestClassUtils, test_data_file_path
from metax_api.utils import parse_timestamp_string_to_tz_aware_datetime


class StatisticRPCTests(APITestCase, TestClassUtils):

    @classmethod
    def setUpClass(cls):
        """
        Loaded only once for test cases inside this class.
        """
        super().setUpClass()
        call_command('loaddata', test_data_file_path, verbosity=0)

    # @classmethod
    # def setUpTestData(cls):

    def setUp(self):
        super().setUp()
        self._use_http_authorization(username='metax')
        self._setup_testdata()

    def _setup_testdata(self):
        test_orgs = ['org_1', 'org_2', 'org_3']
        access_types = [ v for k, v in ACCESS_TYPES.items() ]

        # ensure testdata has something sensible to test against.
        # todo may be feasible to move these to generate_test_data without breaking all tests ?
        # or maybe even shove into fetch_and_update_datasets.py ...
        for cr in CatalogRecord.objects.filter():

            # distribute records between orgs
            if cr.id % 3 == 0:
                cr.metadata_owner_org = test_orgs[2]
            elif cr.id % 2 == 0:
                cr.metadata_owner_org = test_orgs[1]
            else:
                cr.metadata_owner_org = test_orgs[0]

            # distribute records between access types
            if cr.id % 5 == 0:
                cr.research_dataset['access_rights']['access_type']['identifier'] = access_types[4]
            elif cr.id % 4 == 0:
                cr.research_dataset['access_rights']['access_type']['identifier'] = access_types[3]
            elif cr.id % 3 == 0:
                cr.research_dataset['access_rights']['access_type']['identifier'] = access_types[2]
            elif cr.id % 2 == 0:
                cr.research_dataset['access_rights']['access_type']['identifier'] = access_types[1]
            else:
                cr.research_dataset['access_rights']['access_type']['identifier'] = access_types[0]

            # distribute records between some creation months
            date = '20%s-%s-13'
            if cr.id % 8 == 0:
                cr.date_created = parse_timestamp_string_to_tz_aware_datetime(date % ('18', '06'))
            elif cr.id % 7 == 0:
                cr.date_created = parse_timestamp_string_to_tz_aware_datetime(date % ('18', '07'))
            elif cr.id % 6 == 0:
                cr.date_created = parse_timestamp_string_to_tz_aware_datetime(date % ('18', '10'))
            elif cr.id % 5 == 0:
                cr.date_created = parse_timestamp_string_to_tz_aware_datetime(date % ('18', '11'))
            elif cr.id % 4 == 0:
                cr.date_created = parse_timestamp_string_to_tz_aware_datetime(date % ('18', '12'))
            elif cr.id % 3 == 0:
                cr.date_created = parse_timestamp_string_to_tz_aware_datetime(date % ('19', '01'))
            elif cr.id % 2 == 0:
                cr.date_created = parse_timestamp_string_to_tz_aware_datetime(date % ('19', '02'))
            else:
                cr.date_created = parse_timestamp_string_to_tz_aware_datetime(date % ('19', '03'))

            # set some records as "created through end user api"
            if cr.id % 10 == 0:
                cr.service_created = None
                cr.user_created = 'abc%d@fairdataid' % cr.id

            cr.force_save()

        # set deprecated datasets
        cr = CatalogRecord.objects.filter(data_catalog__catalog_json__research_dataset_schema='ida').last()
        cr.deprecated = True
        cr.force_save()

        # set removed datasets
        cr = CatalogRecord.objects.filter(data_catalog__catalog_json__research_dataset_schema='att').last()
        cr.removed = True
        cr.force_save()

        # create a few files which do not belong to any datasets
        response = self.client.get('/rest/files/1', format='json')

        files = []

        for i in range(5):
            f = deepcopy(response.data)
            del f['id']
            f['identifier'] += 'unique' + str(i)
            f['file_name'] += str(i)
            f['file_path'] += str(i)
            f['project_identifier'] = 'prj123'
            files.append(f)

        response = self.client.post('/rest/files', files, format='json')
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)
        response = self.client.get('/rest/directories/update_byte_sizes_and_file_counts', format='json')
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        response = self.client.get('/rest/datasets/update_cr_total_files_byte_sizes', format='json')
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)

    def test_something(self):
        pass
        # response = self.client.get('/rpc/statistics/something')
        # self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
