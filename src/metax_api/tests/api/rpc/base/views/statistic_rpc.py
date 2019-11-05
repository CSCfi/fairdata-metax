# This file is part of the Metax API service
#
# Copyright 2017-2018 Ministry of Education and Culture, Finland
#
# :author: CSC - IT Center for Science Ltd., Espoo Finland <servicedesk@csc.fi>
# :license: MIT

from copy import deepcopy

from django.conf import settings
from django.core.management import call_command
from rest_framework import status
from rest_framework.test import APITestCase

from metax_api.models import CatalogRecord, DataCatalog
from metax_api.models.catalog_record import ACCESS_TYPES
from metax_api.tests.api.rest.base.views.datasets.write import CatalogRecordApiWriteCommon
from metax_api.tests.utils import TestClassUtils, test_data_file_path
from metax_api.utils import parse_timestamp_string_to_tz_aware_datetime


class StatisticRPCCommon(APITestCase, TestClassUtils):

    @classmethod
    def setUpClass(cls):
        """
        Loaded only once for test cases inside this class.
        """
        super().setUpClass()
        call_command('loaddata', test_data_file_path, verbosity=0)

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

        # create legacy datacatalog
        dc = DataCatalog.objects.filter(catalog_json__research_dataset_schema='ida').first()
        dc.catalog_json['identifier'] = settings.LEGACY_CATALOGS[0]
        dc_json = { "catalog_json": dc.catalog_json }
        response = self.client.post('/rest/datacatalogs', dc_json, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)

    def _set_deprecated_dataset(self, id=1):
        cr = CatalogRecord.objects.get(id=id)
        cr.deprecated = True
        cr.force_save()

    def _set_removed_dataset(self, id=1):
        cr = CatalogRecord.objects.get(id=id)
        cr.removed = True
        cr.force_save()

    def _create_legacy_dataset(self):
        """
        Creates one new legacy dataset and returns its id
        """
        legacy_dataset = {
            "data_catalog": {
                "identifier": settings.LEGACY_CATALOGS[0]
            },
            "metadata_owner_org": "some_org_id",
            "metadata_provider_org": "some_org_id",
            "metadata_provider_user": "some_user_id",
            "research_dataset": {
                "title": {
                    "en": "Test Dataset Title"
                },
                "description": {
                    "en": "A descriptive description describing the contents of this dataset. Must be descriptive."
                },
                "files": [
                    {
                        "identifier": "pid:urn:1",
                        "title": "File Title",
                        "description": "informative description",
                        "use_category": {
                            "identifier": "method"
                        }
                    },
                    {
                        "identifier": "pid:urn:3",
                        "title": "File Title",
                        "description": "informative description",
                        "use_category": {
                            "identifier": "method"
                        }
                    }
                ],
                "creator": [
                    {
                        "name": "Teppo Testaaja",
                        "@type": "Person",
                        "member_of": {
                            "name": {
                                "fi": "Testiorganisaatio"
                            },
                            "@type": "Organization"
                        }
                    }
                ],
                "access_rights": {
                    "access_type": {
                        "identifier": "http://uri.suomi.fi/codelist/fairdata/access_type/code/open",
                        "pref_label": {
                            "fi": "Avoin",
                            "en": "Open",
                            "und": "Avoin"
                        },
                        "in_scheme": "http://uri.suomi.fi/codelist/fairdata/access_type"
                    }
                },
                "preferred_identifier": "uniikkinen_aidentifaijeri"
            }
        }
        response = self.client.post('/rest/datasets', legacy_dataset, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)

        return response.data['id']

    def _create_new_dataset_version(self, id=1):
        """
        Finds the latest version of given dataset, deletes one file from it and updates.
        Does not check if there are files to be deleted.
        """
        cr = self.client.get(f'/rest/datasets/{id}', format='json').data
        while cr.get('next_dataset_version', False):
            id = cr['next_dataset_version']['id']
            cr = self.client.get(f'/rest/datasets/{id}', format='json').data

        cr['research_dataset']['files'].pop()
        response = self.client.put(f'/rest/datasets/{id}', cr, format="json")
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)

        return response.data['next_dataset_version']['id']


class StatisticRPCCountDatasets(StatisticRPCCommon, CatalogRecordApiWriteCommon):
    """
    Test suite for count_datasets api. So far tests only parameters removed, legacy, latest and
    combinations.
    """

    def test_count_datasets_single(self):
        """
        Tests single parameters for api. Empty removed and legacy parameters returns true AND false matches
        """
        total_count = CatalogRecord.objects_unfiltered.count()
        response = self.client.get('/rpc/statistics/count_datasets').data
        self.assertEqual(total_count, response['count'], response)

        # test removed -parameter
        self._set_removed_dataset(id=2)
        response = self.client.get('/rpc/statistics/count_datasets?removed=true').data
        self.assertEqual(response['count'], 1, response)

        response = self.client.get('/rpc/statistics/count_datasets?removed=false').data
        self.assertEqual(response['count'], total_count - 1, response)

        # test legacy -parameter
        self._create_legacy_dataset()
        total_count = CatalogRecord.objects_unfiltered.count()
        response = self.client.get('/rpc/statistics/count_datasets?legacy=true').data
        self.assertEqual(response['count'], 1, response)

        response = self.client.get('/rpc/statistics/count_datasets?legacy=false').data
        self.assertEqual(response['count'], total_count - 1, response)

        # test latest -parameter
        self._create_new_dataset_version()
        self._create_new_dataset_version()
        total_count = CatalogRecord.objects_unfiltered.count()
        response = self.client.get('/rpc/statistics/count_datasets?latest=false').data # returns all
        self.assertEqual(response['count'], total_count, response)

        with_param = self.client.get('/rpc/statistics/count_datasets?latest=true').data
        without_param = self.client.get('/rpc/statistics/count_datasets').data # default is true
        self.assertEqual(with_param['count'], total_count - 2, with_param)
        self.assertEqual(with_param['count'], without_param['count'], with_param)

    def test_count_datasets_removed_latest(self):
        second_ver = self._create_new_dataset_version()
        self._create_new_dataset_version(second_ver)
        self._set_removed_dataset()
        self._set_removed_dataset(id=second_ver)
        self._set_removed_dataset(id=2)

        rem_lat = self.client.get('/rpc/statistics/count_datasets?removed=true&latest=true').data
        rem_not_lat = self.client.get('/rpc/statistics/count_datasets?removed=true&latest=false').data

        self.assertEqual(rem_lat['count'], 1, 'Only latest versions should be checked') # id=2
        self.assertEqual(rem_not_lat['count'], 3, 'Only the prev versions should be removed')

        # create new dataset with 2 versions
        response = self.client.post('/rest/datasets', self.cr_test_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)
        self._create_new_dataset_version(response.data['id'])

        not_rem_lat = self.client.get('/rpc/statistics/count_datasets?removed=false&latest=true').data
        not_rem_not_lat = self.client.get('/rpc/statistics/count_datasets?removed=false&latest=false').data

        self.assertEqual(not_rem_lat['count'], not_rem_not_lat['count'] - 1)

    def test_count_datasets_removed_legacy(self):
        self._create_legacy_dataset()
        self._create_legacy_dataset()
        leg_cr = self._create_legacy_dataset()
        self._set_removed_dataset(leg_cr)
        total_count = CatalogRecord.objects_unfiltered.count()

        rem_leg = self.client.get('/rpc/statistics/count_datasets?removed=true&legacy=true').data
        rem_not_leg = self.client.get('/rpc/statistics/count_datasets?removed=true&legacy=false').data
        not_rem_leg = self.client.get('/rpc/statistics/count_datasets?removed=false&legacy=true').data
        not_rem_not_leg = self.client.get('/rpc/statistics/count_datasets?removed=false&legacy=false').data

        self.assertEqual(rem_leg['count'], 1)
        self.assertEqual(rem_not_leg['count'], 0)
        self.assertEqual(not_rem_leg['count'], 2)
        self.assertEqual(not_rem_not_leg['count'], total_count - 3)

    def test_count_datasets_latest_legacy(self):
        leg_cr = self._create_legacy_dataset()
        self._create_new_dataset_version(leg_cr)
        self._create_new_dataset_version(leg_cr)
        total_count = CatalogRecord.objects_unfiltered.count()

        leg_lat = self.client.get('/rpc/statistics/count_datasets?legacy=true&latest=true').data
        leg_not_lat = self.client.get('/rpc/statistics/count_datasets?legacy=true&latest=false').data
        not_leg_not_lat = self.client.get('/rpc/statistics/count_datasets?legacy=false&latest=false').data

        self.assertEqual(leg_lat['count'], 1)
        self.assertEqual(leg_not_lat['count'], 3)
        self.assertEqual(not_leg_not_lat['count'], total_count - 3)
