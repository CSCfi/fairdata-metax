# This file is part of the Metax API service
#
# Copyright 2017-2018 Ministry of Education and Culture, Finland
#
# :author: CSC - IT Center for Science Ltd., Espoo Finland <servicedesk@csc.fi>
# :license: MIT

from datetime import timedelta
import urllib.parse

from django.conf import settings
from django.core.management import call_command
from django.utils import timezone
from pytz import timezone as tz
import responses
from rest_framework import status
from rest_framework.test import APITestCase

from metax_api.models import CatalogRecord, Contract, File
from metax_api.tests.utils import test_data_file_path, TestClassUtils


class CatalogRecordApiReadCommon(APITestCase, TestClassUtils):
    @classmethod
    def setUpClass(cls):
        """
        Loaded only once for test cases inside this class.
        """
        call_command('loaddata', test_data_file_path, verbosity=0)
        super(CatalogRecordApiReadCommon, cls).setUpClass()

    def setUp(self):
        self.cr_from_test_data = self._get_object_from_test_data('catalogrecord', requested_index=0)
        self.pk = self.cr_from_test_data['id']
        self.metadata_version_identifier = self.cr_from_test_data['research_dataset']['metadata_version_identifier']
        self.preferred_identifier = self.cr_from_test_data['research_dataset']['preferred_identifier']
        self.identifier = self.cr_from_test_data['identifier']
        self._use_http_authorization()


class CatalogRecordApiReadBasicTests(CatalogRecordApiReadCommon):

    """
    Basic read operations
    """

    def test_read_catalog_record_list(self):
        response = self.client.get('/rest/datasets')
        self.assertEqual(response.status_code, status.HTTP_200_OK)

    def test_read_catalog_record_details_by_pk(self):
        response = self.client.get('/rest/datasets/%s' % self.pk)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data['identifier'], self.identifier)
        self.assertEqual('identifier' in response.data['data_catalog'], True)

    def test_read_catalog_record_details_by_identifier(self):
        response = self.client.get('/rest/datasets/%s' % self.identifier)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data['identifier'],
            self.identifier)

    def test_get_by_preferred_identifier(self):
        cr = CatalogRecord.objects.get(pk=1)
        cr.research_dataset['preferred_identifier'] = '%s-/uhoh/special.chars?all&around' % cr.preferred_identifier
        cr.force_save()
        response = self.client.get('/rest/datasets?preferred_identifier=%s' %
            urllib.parse.quote(cr.preferred_identifier))
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data['research_dataset']['preferred_identifier'], cr.preferred_identifier)

    def test_get_removed_by_preferred_identifier(self):
        self._use_http_authorization()
        response = self.client.delete('/rest/datasets/%s' % self.identifier)

        self.assertEqual(response.status_code, status.HTTP_204_NO_CONTENT)
        response = self.client.get('/rest/datasets?preferred_identifier=%s&removed=true' %
            urllib.parse.quote(self.preferred_identifier))
        self.assertEqual(response.status_code, status.HTTP_200_OK)

    def test_get_by_preferred_identifier_search_prefers_oldest_data_catalog(self):
        '''
        Search by preferred_identifier should prefer hits from oldest created catalogs
        which are assumed to be att/fairdata catalogs.
        '''

        # get a cr that has alternate records
        cr = self._get_object_from_test_data('catalogrecord', requested_index=9)
        pid = cr['research_dataset']['preferred_identifier']

        # verify there are more than one record with same pid!
        count = CatalogRecord.objects.filter(research_dataset__preferred_identifier=pid).count()
        self.assertEqual(count > 1, True, 'makes no sense to test with a pid that exists only once')

        # the retrieved record should be the one that is in catalog 1
        response = self.client.get('/rest/datasets?preferred_identifier=%s' %
            urllib.parse.quote(pid))
        self.assertEqual('alternate_record_set' in response.data, True)
        self.assertEqual(response.data['data_catalog']['id'], cr['data_catalog'])

    def test_read_catalog_record_details_not_found(self):
        response = self.client.get('/rest/datasets/shouldnotexist')
        self.assertEqual(response.status_code, status.HTTP_404_NOT_FOUND)

    def test_read_catalog_record_metadata_version_identifiers(self):
        response = self.client.get('/rest/datasets/metadata_version_identifiers')
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertTrue(isinstance(response.data, list))
        self.assertTrue(len(response.data) > 0)

    def test_get_unique_preferred_identifiers(self):
        """
        Get all unique preferred_identifiers, no matter if they are the latest dataset version or not.
        """
        response = self.client.get('/rest/datasets/unique_preferred_identifiers')
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertTrue(isinstance(response.data, list))
        self.assertTrue(len(response.data) > 0)

        # save the current len, do some more operations, and compare the difference
        ids_len = len(response.data)

        self._create_new_ds()
        self._create_new_ds()
        response = self.client.get('/rest/datasets/unique_preferred_identifiers')
        self.assertEqual(len(response.data) - ids_len, 2, 'should be two new PIDs')

    def test_get_latest_unique_preferred_identifiers(self):
        """
        Get all unique preferred_identifiers, but only from the latest dataset versions.
        """
        response = self.client.get('/rest/datasets/unique_preferred_identifiers?latest')
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertTrue(isinstance(response.data, list))
        self.assertTrue(len(response.data) > 0)

        # save the current len, do some more operations, and compare the difference
        ids_len = len(response.data)

        # files change
        cr = CatalogRecord.objects.get(pk=1)
        new_file_id = cr.files.all().order_by('-id').first().id + 1
        file_from_testdata = self._get_object_from_test_data('file', requested_index=new_file_id)
        # warning, this is actual file metadata, would not pass schema validation if sent through api
        cr.research_dataset['files'] = [file_from_testdata]
        cr.save()
        response = self.client.get('/rest/datasets/unique_preferred_identifiers?latest')
        self.assertEqual(ids_len, len(response.data), 'count should stay the same')

        # create new
        self._create_new_ds()
        self._create_new_ds()
        response = self.client.get('/rest/datasets/unique_preferred_identifiers?latest')
        self.assertEqual(len(response.data) - ids_len, 2, 'should be two new PIDs')

    def test_expand_relations(self):
        cr = CatalogRecord.objects.get(pk=1)
        cr.contract_id = 1
        cr.force_save()

        response = self.client.get('/rest/datasets/1?expand_relation=data_catalog,contract')
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        self.assertEqual('catalog_json' in response.data['data_catalog'], True, response.data['data_catalog'])
        self.assertEqual('contract_json' in response.data['contract'], True, response.data['contract'])

    def test_strip_sensitive_fields(self):
        """
        Strip fields not intended for general public
        """
        def _check_fields(obj):
            for sensitive_field in ['email', 'telephone', 'phone']:
                self.assertEqual(sensitive_field not in obj['research_dataset']['curator'][0], True,
                    'field %s should have been stripped' % sensitive_field)

        for cr in CatalogRecord.objects.filter(pk__in=(1, 2, 3)):
            cr.research_dataset['curator'][0].update({
                'email': 'email@mail.com',
                'phone': '123124',
                'telephone': '123124',
            })
            cr.force_save()

        self.client._credentials = {}

        response = self.client.get('/rest/datasets/1')
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        _check_fields(response.data)

        response = self.client.get('/rest/datasets')
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        for obj in response.data['results']:
            _check_fields(obj)

        response = self.client.get('/rest/datasets?no_pagination')
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        for obj in response.data:
            _check_fields(obj)

    def _create_new_ds(self):
        new_cr = self.client.get('/rest/datasets/2').data
        new_cr.pop('id')
        new_cr['research_dataset'].pop('preferred_identifier')
        new_cr.pop('identifier')
        self._use_http_authorization()
        response = self.client.post('/rest/datasets', new_cr, format='json')
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)


class CatalogRecordApiReadBasicAuthorizationTests(CatalogRecordApiReadCommon):
    """
    Basic read operations from authorization perspective
    """

    # THE OK TESTS

    def test_returns_all_file_dir_info_for_open_catalog_record_if_no_authorization(self):
        open_cr_json = self.get_open_cr_with_files_and_dirs_from_api_with_file_details()

        # Verify all file and dir details info is returned for open cr /rest/datasets/<pk> without
        # authorization
        self._assert_ok(open_cr_json, 'no')

    def test_returns_all_file_dir_info_for_login_catalog_record_if_no_authorization(self):
        login_cr_json = self.get_open_cr_with_files_and_dirs_from_api_with_file_details(use_login_access_type=True)

        # Verify all file and dir details info is returned for login cr /rest/datasets/<pk> without authorization
        self._assert_ok(login_cr_json, 'no')

    @responses.activate
    def test_returns_all_file_dir_info_for_open_catalog_record_if_owner_authorization(self):
        self.create_end_user_data_catalogs()
        open_cr_json = self.get_open_cr_with_files_and_dirs_from_api_with_file_details(True)

        # Verify all file and dir details info is returned for open owner-owned cr /rest/datasets/<pk> with
        # owner authorization
        self._assert_ok(open_cr_json, 'owner')

    @responses.activate
    def test_returns_all_file_dir_info_for_login_catalog_record_if_owner_authorization(self):
        self.create_end_user_data_catalogs()
        login_cr_json = self.get_open_cr_with_files_and_dirs_from_api_with_file_details(set_owner=True,
                                                                                        use_login_access_type=True)

        # Verify all file and dir details info is returned for login owner-owned cr /rest/datasets/<pk> with
        # owner authorization
        self._assert_ok(login_cr_json, 'owner')

    def test_returns_all_file_dir_info_for_restricted_catalog_record_if_service_authorization(self):
        restricted_cr_json = self.get_restricted_cr_with_files_and_dirs_from_api_with_file_details()

        # Verify all file and dir details info is returned for restricted cr /rest/datasets/<pk> with
        # service authorization
        self._assert_ok(restricted_cr_json, 'service')

    @responses.activate
    def test_returns_all_file_dir_info_for_restricted_catalog_record_if_owner_authorization(self):
        self.create_end_user_data_catalogs()
        restricted_cr_json = self.get_restricted_cr_with_files_and_dirs_from_api_with_file_details(True)

        # Verify all file and dir details info is returned for restricted owner-owned cr /rest/datasets/<pk> with
        # owner authorization
        self._assert_ok(restricted_cr_json, 'owner')

    def test_returns_all_file_dir_info_for_embargoed_catalog_record_if_available_reached_and_no_authorization(self):
        available_embargoed_cr_json = self.get_embargoed_cr_with_files_and_dirs_from_api_with_file_details(True)

        # Verify all file and dir details info is returned for embargoed cr /rest/datasets/<pk> when
        # embargo date has been reached without authorization
        self._assert_ok(available_embargoed_cr_json, 'no')

    # THE FORBIDDEN TESTS

    def test_returns_limited_file_dir_info_for_restricted_catalog_record_if_no_authorization(self):
        restricted_cr_json = self.get_restricted_cr_with_files_and_dirs_from_api_with_file_details()

        # Verify limited file and dir info for restricted cr /rest/datasets/<pk> without authorization
        self._assert_limited_or_no_file_dir_info(restricted_cr_json, 'no')

    def test_no_file_dir_info_for_embargoed_catalog_record_if_available_not_reached_and_no_authorization(self):
        not_available_embargoed_cr_json = self.get_embargoed_cr_with_files_and_dirs_from_api_with_file_details(
            False)

        # Verify no file and dir info for embargoed cr /rest/datasets/<pk> when embargo date has not
        # been reached without authorization
        self._assert_limited_or_no_file_dir_info(not_available_embargoed_cr_json, 'no')

    def _assert_limited_or_no_file_dir_info(self, cr_json, credentials_type):
        self._set_http_authorization(credentials_type)

        file_amt = len(cr_json['research_dataset']['files'])
        dir_amt = len(cr_json['research_dataset']['directories'])
        pk = cr_json['id']

        response = self.client.get('/rest/datasets/{0}?file_details'.format(pk))
        self.assertEqual(response.status_code, status.HTTP_200_OK)

        self.assertEqual(len(response.data['research_dataset']['files']), file_amt)
        self.assertEqual(len(response.data['research_dataset']['directories']), dir_amt)

        for f in response.data['research_dataset']['files']:
            self.assertTrue('details' in f)
            # The below assert is a bit arbitrary
            self.assertFalse('identifier' in f)
        for d in response.data['research_dataset']['directories']:
            self.assertTrue('details' in d)
            # The below assert is a bit arbitrary
            self.assertFalse('identifier' in d)

    def _assert_ok(self, cr_json, credentials_type):
        self._set_http_authorization(credentials_type)

        file_amt = len(cr_json['research_dataset']['files'])
        dir_amt = len(cr_json['research_dataset']['directories'])
        pk = cr_json['id']

        response = self.client.get('/rest/datasets/{0}?file_details'.format(pk))
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data['research_dataset']['files']), file_amt)
        self.assertEqual(len(response.data['research_dataset']['directories']), dir_amt)

        for f in response.data['research_dataset']['files']:
            self.assertTrue('details' in f)
            # The below assert is a bit arbitrary
            self.assertTrue('identifier' in f)
        for d in response.data['research_dataset']['directories']:
            self.assertTrue('details' in d)
            # The below assert is a bit arbitrary
            self.assertTrue('identifier' in d)


class CatalogRecordApiReadPreservationStateTests(CatalogRecordApiReadCommon):
    """
    preservation_state filtering
    """

    def test_read_catalog_record_search_by_preservation_state(self):
        '''
        Various simple filtering requests
        '''
        response = self.client.get('/rest/datasets?state=0')
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data['results']) > 2, True,
            'There should have been multiple results for state=0 request')

        response = self.client.get('/rest/datasets?state=10')
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data['results']), 2)

        response = self.client.get('/rest/datasets?state=40')
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data['results']), 1)

    def test_read_catalog_record_search_by_preservation_state_666(self):
        response = self.client.get('/rest/datasets?state=666')
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data['results']), 0, 'should return empty list')

    def test_read_catalog_record_search_by_preservation_state_many(self):
        response = self.client.get('/rest/datasets?state=10,40')
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data['results']), 3)
        self.assertEqual(response.data['results'][0]['preservation_state'], 10)
        self.assertEqual(response.data['results'][1]['preservation_state'], 10)
        self.assertEqual(response.data['results'][2]['preservation_state'], 40)

    def test_read_catalog_record_search_by_preservation_state_invalid_value(self):
        response = self.client.get('/rest/datasets?state=1,a')
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual('is not an integer' in response.data['state'][0], True,
                         'Error should say letter a is not an integer')


class CatalogRecordApiReadActorFilter(CatalogRecordApiReadCommon):

    def test_agents_and_actors(self):
        # set test conditions
        cr = CatalogRecord.objects.get(pk=11)
        cr.research_dataset['curator'] = []
        cr.research_dataset['curator'].append({ '@type': 'Person', 'name': 'Tarmo Termiitti' })
        cr.research_dataset['curator'].append({ '@type': 'Person', 'name': 'Keijo Kottarainen' })
        cr.research_dataset['curator'].append({ '@type': 'Person', 'name': 'Janus Järvinen' })
        cr.research_dataset['curator'].append({ '@type': 'Person', 'name': 'Laina Sakkonen' })
        cr.research_dataset['curator'].append({ '@type': 'Person', 'name': 'Kaisa Kuraattori' })
        cr.research_dataset['creator'] = []
        cr.research_dataset['creator'].append({ '@type': 'Organization', 'name': { 'en': 'Unique Organization'} })
        cr.research_dataset['creator'].append({ '@type': 'Organization', 'name': { 'en': 'Happy Organization'} })
        cr.research_dataset['creator'].append({ '@type': 'Organization', 'name': { 'en': 'Sad Organization'} })
        cr.research_dataset['creator'].append({ '@type': 'Organization', 'name': { 'en': 'Brilliant Organization'} })
        cr.research_dataset['creator'].append({ '@type': 'Organization', 'name': {'en': 'Wonderful Organization'} })
        cr.research_dataset['publisher']['name'] = {}
        cr.research_dataset['publisher']['name'] = { 'fi': 'Originaali Organisaatio' }
        cr.force_save()

        response = self.client.get('/rest/datasets?creator_organization=happy')
        self.assertEqual(len(response.data['results']), 1, response.data)

        response = self.client.get('/rest/datasets?creator_organization=Brilliant Organization')
        self.assertEqual(len(response.data['results']), 1, response.data)

        response = self.client.get('/rest/datasets?curator_person=termiitti')
        self.assertEqual(len(response.data['results']), 1, response.data)

        response = self.client.get('/rest/datasets?curator_person=Laina Sakkonen')
        self.assertEqual(len(response.data['results']), 1, response.data)

        response = self.client.get('/rest/datasets?publisher_organization=originaali Organisaatio')
        self.assertEqual(len(response.data['results']), 1, response.data)

        query = 'curator_person=notfound&creator_organization=sad organ&condition_separator=AND'
        response = self.client.get('/rest/datasets?%s' % query)
        self.assertEqual(len(response.data['results']), 0, response.data)

        query = 'curator_person=notfound&creator_organization=sad organ&condition_separator=OR'
        response = self.client.get('/rest/datasets?%s' % query)
        self.assertEqual(len(response.data['results']), 1, response.data)

        # test filter with pas filter
        """
        Both organization and pas filters use internally Q-filters which are supposed to be AND'ed together.
        """
        metax_user = settings.API_METAX_USER
        self._use_http_authorization(username=metax_user['username'], password=metax_user['password'])

        response = self.client.get('/rest/datasets?pas_filter=janus&creator_organization=sad organization')
        self.assertEqual(len(response.data['results']), 1)

        response = self.client.get('/rest/datasets?state=10&pas_filter=kaisa&creator_organization=notfound')
        self.assertEqual(len(response.data['results']), 0)


class CatalogRecordApiReadPASFilter(CatalogRecordApiReadCommon):

    def test_pas_filter(self):
        """
        Test query param pas_filter which should search from various fields using the same search term.
        """

        # set test conditions
        cr = CatalogRecord.objects.get(pk=1)
        cr.preservation_state = 10
        cr.contract_id = 1
        cr.research_dataset['title']['en'] = 'Catch me if you can'
        cr.research_dataset['title']['fi'] = 'Ota kiinni jos saat'
        cr.research_dataset['curator'] = []
        cr.research_dataset['curator'].append({ 'name': 'Seppo Hovi' })
        cr.research_dataset['curator'].append({ 'name': 'Esa Nieminen' })
        cr.research_dataset['curator'].append({ 'name': 'Aku Ankka' })
        cr.research_dataset['curator'].append({ 'name': 'Jaska Jokunen' })
        cr.force_save()

        contract = Contract.objects.get(pk=1)
        contract.contract_json['title'] = 'An Important Agreement'
        contract.save()

        metax_user = settings.API_METAX_USER
        self._use_http_authorization(username=metax_user['username'], password=metax_user['password'])

        # beging testing

        response = self.client.get('/rest/datasets?state=10&pas_filter=if you')
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data['results']), 1)

        response = self.client.get('/rest/datasets?state=10&pas_filter=kiinni jos')
        self.assertEqual(len(response.data['results']), 1)

        response = self.client.get('/rest/datasets?state=10&pas_filter=niemine')
        self.assertEqual(len(response.data['results']), 1)

        # more than 3 curators, requires typing exact case-sensitive name... see comments in related code
        response = self.client.get('/rest/datasets?state=10&pas_filter=jokunen')
        self.assertEqual(len(response.data['results']), 0)
        response = self.client.get('/rest/datasets?state=10&pas_filter=Jaska Jokunen')
        self.assertEqual(len(response.data['results']), 1)

        # contract_id 1 has several other associated test datasets
        response = self.client.get('/rest/datasets?state=10&pas_filter=agreement')
        self.assertEqual(len(response.data['results']), 3)

        response = self.client.get('/rest/datasets?state=10&pas_filter=does not exist')
        self.assertEqual(len(response.data['results']), 0)

    def test_pas_filter_is_restricted(self):
        """
        Query param is permitted to users metax and tpas.
        """
        response = self.client.get('/rest/datasets?state=10&pas_filter=hmmm')
        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN)


class CatalogRecordApiReadQueryParamsTests(CatalogRecordApiReadCommon):

    """
    query_params filtering
    """

    def test_read_catalog_record_search_by_curator_1(self):
        response = self.client.get('/rest/datasets?curator=id:of:curator:rahikainen')
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data['results']), 10)
        self.assertEqual(response.data['results'][0]['research_dataset']['curator'][0]['name'], 'Rahikainen',
                         'Curator name is not matching')
        self.assertEqual(response.data['results'][4]['research_dataset']['curator'][0]['name'], 'Rahikainen',
                         'Curator name is not matching')

    def test_read_catalog_record_search_by_curator_2(self):
        response = self.client.get('/rest/datasets?curator=id:of:curator:jarski')
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data), 4)
        self.assertEqual(response.data['results'][0]['research_dataset']['curator'][0]['name'], 'Jarski',
                         'Curator name is not matching')
        self.assertEqual(response.data['results'][3]['research_dataset']['curator'][0]['name'], 'Jarski',
                         'Curator name is not matching')

    def test_read_catalog_record_search_by_curator_not_found_1(self):
        response = self.client.get('/rest/datasets?curator=Not Found')
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data['results']), 0)

    def test_read_catalog_record_search_by_curator_not_found_case_sensitivity(self):
        response = self.client.get('/rest/datasets?curator=id:of:curator:Rahikainen')
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data['results']), 0)

    def test_read_catalog_record_search_by_curator_and_state_1(self):
        response = self.client.get('/rest/datasets?curator=id:of:curator:rahikainen&state=10')
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data['results']), 2)
        self.assertEqual(response.data['results'][0]['id'], 2)
        self.assertEqual(response.data['results'][0]['preservation_state'], 10)
        self.assertEqual(response.data['results'][0]['research_dataset']['curator'][0]['name'], 'Rahikainen',
                         'Curator name is not matching')

    def test_read_catalog_record_search_by_curator_and_state_2(self):
        response = self.client.get('/rest/datasets?curator=id:of:curator:rahikainen&state=40')
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data['results']), 1)
        self.assertEqual(response.data['results'][0]['id'], 4)
        self.assertEqual(response.data['results'][0]['preservation_state'], 40)
        self.assertEqual(response.data['results'][0]['research_dataset']['curator'][0]['name'], 'Rahikainen',
                         'Curator name is not matching')

    def test_read_catalog_record_search_by_curator_and_state_not_found(self):
        response = self.client.get('/rest/datasets?curator=id:of:curator:rahikainen&state=55')
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data['results']), 0)

    def test_read_catalog_record_search_by_owner_id(self):
        cr = CatalogRecord.objects.get(pk=1)
        cr.editor = { 'owner_id': '123' }
        cr.save()
        response = self.client.get('/rest/datasets?owner_id=123')
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data['results']), 1)
        self.assertEqual(response.data['results'][0]['editor']['owner_id'], '123')

    def test_read_catalog_record_search_by_creator_id(self):
        cr = CatalogRecord.objects.get(pk=1)
        cr.user_created = '123'
        cr.force_save()
        response = self.client.get('/rest/datasets?user_created=123')
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data['results']), 1)
        self.assertEqual(response.data['results'][0]['user_created'], '123')

    def test_read_catalog_record_search_by_editor(self):
        response = self.client.get('/rest/datasets?editor=mspaint')
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data['count'], 0)

        response = self.client.get('/rest/datasets?editor=qvain')
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        qvain_records_count = response.data['count']
        self.assertEqual(qvain_records_count > 0, True)

        response = self.client.get('/rest/datasets')
        self.assertNotEqual(response.data['count'], qvain_records_count, 'looks like filtering had no effect')

    def test_read_catalog_record_search_by_metadata_provider_user(self):
        response = self.client.get('/rest/datasets?metadata_provider_user=123')
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data['count'], 0)

        cr = CatalogRecord.objects.get(pk=1)
        cr.metadata_provider_user = '123'
        cr.force_save()

        response = self.client.get('/rest/datasets?metadata_provider_user=123')
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(response.data['count'], 1)

    def test_read_catalog_record_search_by_metadata_owner_org(self):
        owner_org = 'org_id'
        for cr in CatalogRecord.objects.filter(pk__in=[1, 2, 3]):
            cr.metadata_owner_org = owner_org
            cr.force_save()

        owner_org_2 = 'org_id_2'
        for cr in CatalogRecord.objects.filter(pk__in=[4, 5, 6]):
            cr.metadata_owner_org = owner_org_2
            cr.force_save()

        response = self.client.get('/rest/datasets?metadata_owner_org=%s' % owner_org)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data['results']), 3)

        response = self.client.get('/rest/datasets?metadata_owner_org=%s,%s' % (owner_org, owner_org_2))
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data['results']), 6)

    def test_filter_by_contract_org_identifier(self):
        """
        Test filtering by contract_org_identifier, which matches using iregex
        """
        metax_user = settings.API_METAX_USER
        self._use_http_authorization(username=metax_user['username'], password=metax_user['password'])

        response = self.client.get('/rest/datasets?contract_org_identifier=2345')
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data['results']), 10)

        response = self.client.get('/rest/datasets?contract_org_identifier=1234567-1')
        self.assertEqual(len(response.data['results']), 10)

        response = self.client.get('/rest/datasets?contract_org_identifier=1234567-123')
        self.assertEqual(len(response.data['results']), 0)

    def test_filter_by_contract_org_identifier_is_restricted(self):
        response = self.client.get('/rest/datasets?contract_org_identifier=1234')
        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN)

    def test_read_catalog_record_search_by_data_catalog_id(self):
        from metax_api.models.data_catalog import DataCatalog

        # Create a new data catalog
        dc = self._get_object_from_test_data('datacatalog', requested_index=0)
        dc_id = 'original_dc_identifier'
        dc['catalog_json']['identifier'] = dc_id
        self.client.post('/rest/datacatalogs', dc, format="json")

        # Set the new data catalog for a catalog record and store the catalog record
        cr = CatalogRecord.objects.get(pk=1)
        cr.data_catalog = DataCatalog.objects.get(catalog_json__identifier=dc_id)
        cr.force_save()

        # Verify
        response = self.client.get('/rest/datasets?data_catalog={0}'.format(dc_id))
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data['results']), 1)
        self.assertEqual(response.data['results'][0]['data_catalog']['identifier'], dc_id)


class CatalogRecordApiReadXMLTransformationTests(CatalogRecordApiReadCommon):
    """
    dataset xml transformations
    """

    def test_read_dataset_xml_format_metax(self):
        response = self.client.get('/rest/datasets/1?dataset_format=metax')
        self._check_dataset_xml_format_response(response, '<researchdataset')

    def test_read_dataset_xml_format_datacite(self):
        for id in CatalogRecord.objects.all().values_list('id', flat=True):
            response = self.client.get('/rest/datasets/%d?dataset_format=fairdata_datacite' % id)
            self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
            self._check_dataset_xml_format_response(response, '<resource')

    def test_read_dataset_xml_format_error_unknown_format(self):
        response = self.client.get('/rest/datasets/1?dataset_format=doesnotexist')
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)

    def test_read_datacite_xml_format_identifier(self):
        # Create ida data catalog
        dc = self._get_object_from_test_data('datacatalog', requested_index=0)
        dc_id = settings.IDA_DATA_CATALOG_IDENTIFIER
        dc['catalog_json']['identifier'] = dc_id
        self.client.post('/rest/datacatalogs', dc, format="json")

        # Create new cr by requesting a doi identifier
        cr_json = self.client.get('/rest/datasets/1').data
        cr_json.pop('preservation_identifier', None)
        cr_json.pop('identifier')
        cr_json['research_dataset'].pop('preferred_identifier', None)
        cr_json['research_dataset']['publisher'] = {'@type': 'Organization', 'name': {'und': 'Testaaja'}}
        cr_json['research_dataset']['issued'] = '2010-01-01'
        cr_json['data_catalog'] = dc_id
        response = self.client.post('/rest/datasets?pid_type=doi', cr_json, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)
        doi = response.data['preservation_identifier']

        # Datacite xml should contain the doi created
        response = self.client.get('/rest/datasets/%s?dataset_format=datacite' % response.data['identifier'])
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        self.assertEqual('<identifier identifierType="DOI">%s' % doi[len('doi:'):] in response.data,
                         True, response.data)

    def test_read_dataset_format_datacite_odd_lang_abbrevation(self):
        cr = CatalogRecord.objects.get(pk=1)
        cr.research_dataset['publisher'] = {'@type': 'Organization', 'name': {'zk': 'Testiorganisaatio'}}
        cr.force_save()
        response = self.client.get('/rest/datasets/1?dataset_format=fairdata_datacite')
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)

    def _check_dataset_xml_format_response(self, response, element_name):
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual('content-type' in response._headers, True, response._headers)
        self.assertEqual('application/xml' in response._headers['content-type'][1], True, response._headers)
        self.assertEqual('<?xml version' in response.data[:20], True, response.data)
        self.assertEqual(element_name in response.data[:60], True, response.data)


class CatalogRecordApiReadHTTPHeaderTests(CatalogRecordApiReadCommon):

    # If the value of the timestamp given in the header is equal or greater than the value of date_modified field,
    # 404 should be returned since nothing has been modified. If the value of the timestamp given in the header is
    # less than value of date_modified field, the object should be returned since it means the object has been
    # modified after the header timestamp

    #
    # header if-modified-since tests, metadata_version_identifiers
    #

    def test_metadata_version_identifiers_get_with_if_modified_since_header_ok(self):
        cr = CatalogRecord.objects.get(pk=self.pk)
        date_modified = cr.date_modified
        date_modified_in_gmt = timezone.localtime(date_modified, timezone=tz('GMT'))

        if_modified_since_header_value = date_modified_in_gmt.strftime('%a, %d %b %Y %H:%M:%S GMT')
        headers = {'HTTP_IF_MODIFIED_SINCE': if_modified_since_header_value}
        response = self.client.get('/rest/datasets/metadata_version_identifiers', **headers)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertTrue(len(response.data) == 6)

        if_modified_since_header_value = (date_modified_in_gmt + timedelta(seconds=1)).strftime(
            '%a, %d %b %Y %H:%M:%S GMT')
        headers = {'HTTP_IF_MODIFIED_SINCE': if_modified_since_header_value}
        response = self.client.get('/rest/datasets/metadata_version_identifiers', **headers)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertTrue(len(response.data) == 6)

        # The assert below may brake if the date_modified timestamps or the amount of test data objects are altered
        # in the test data

        if_modified_since_header_value = (date_modified_in_gmt - timedelta(seconds=1)).strftime(
            '%a, %d %b %Y %H:%M:%S GMT')
        headers = {'HTTP_IF_MODIFIED_SINCE': if_modified_since_header_value}
        response = self.client.get('/rest/datasets/metadata_version_identifiers', **headers)
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertTrue(len(response.data) > 6)


class CatalogRecordApiReadPopulateFileInfoTests(CatalogRecordApiReadCommon):

    """
    Test populating individual research_dataset.file and directory objects with their
    corresponding objects from their db tables.
    """

    def test_file_details_populated(self):
        # without the flag nothing should happen
        response = self.client.get('/rest/datasets/1')
        self.assertEqual(all('details' not in f for f in response.data['research_dataset']['files']), True)

        response = self.client.get('/rest/datasets/1?file_details')
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        # check all fiels have the extra key 'details', and all details have the key 'identifier'.
        # presumably the details were then filled in.
        self.assertEqual(all('details' in f for f in response.data['research_dataset']['files']), True)
        self.assertEqual(all('identifier' in f['details'] for f in response.data['research_dataset']['files']), True)

    def test_directory_details_populated(self):
        # id 11 is one of the example datasets with full details. they should have a couple
        # of directories attached.
        CatalogRecord.objects.get(pk=11).calculate_directory_byte_sizes_and_file_counts()

        response = self.client.get('/rest/datasets/11?file_details')
        self.assertEqual(response.status_code, status.HTTP_200_OK)

        # check all dirs have the extra key 'details', and all details have the key 'identifier'.
        # presumably the details were then filled in.
        self.assertEqual(all('details' in f for f in response.data['research_dataset']['directories']), True)
        self.assertEqual(all('identifier' in f['details'] for f in response.data['research_dataset']['directories']),
            True)

        # additionally check that file counts and total byte sizes are as expected
        self.assertEqual(response.data['research_dataset']['directories'][0]['details']['byte_size'], 21000)
        self.assertEqual(response.data['research_dataset']['directories'][1]['details']['byte_size'], 21000)
        self.assertEqual(response.data['research_dataset']['directories'][0]['details']['file_count'], 20)
        self.assertEqual(response.data['research_dataset']['directories'][1]['details']['file_count'], 20)


class CatalogRecordApiReadPopulateFileInfoAuthorizationTests(CatalogRecordApiReadCommon):
    """
    Test populating individual research_dataset.file and directory objects with their
    corresponding objects from their db tables from authorization perspective.
    """

    # THE OK TESTS

    def test_returns_all_details_for_open_catalog_record_if_no_authorization(self):
        open_cr_json = self.get_open_cr_with_files_and_dirs_from_api_with_file_details()

        # Verify all file and dir details info is returned for open cr /rest/datasets/<pk>?file_details without
        # authorization
        self._assert_ok(open_cr_json, 'no')

    def test_returns_all_details_for_login_catalog_record_if_no_authorization(self):
        open_cr_json = self.get_open_cr_with_files_and_dirs_from_api_with_file_details()

        # Verify all file and dir details info is returned for open cr /rest/datasets/<pk>?file_details without
        # authorization
        self._assert_ok(open_cr_json, 'no')

    def test_returns_all_details_for_open_catalog_record_if_service_authorization(self):
        open_cr_json = self.get_open_cr_with_files_and_dirs_from_api_with_file_details()

        # Verify all file and dir details info is returned for open cr /rest/datasets/<pk>?file_details with
        # service authorization
        self._assert_ok(open_cr_json, 'service')

    @responses.activate
    def test_returns_all_details_for_open_catalog_record_if_owner_authorization(self):
        self.create_end_user_data_catalogs()
        open_cr_json = self.get_open_cr_with_files_and_dirs_from_api_with_file_details(True)

        # Verify all file and dir details info is returned for open owner-owned cr /rest/datasets/<pk>?file_details with
        # owner authorization
        self._assert_ok(open_cr_json, 'owner')

    def test_returns_all_details_for_restricted_catalog_record_if_service_authorization(self):
        restricted_cr_json = self.get_restricted_cr_with_files_and_dirs_from_api_with_file_details()

        # Verify all file and dir details info is returned for restricted cr /rest/datasets/<pk>?file_details with
        # service authorization
        self._assert_ok(restricted_cr_json, 'service')

    @responses.activate
    def test_returns_all_details_for_restricted_catalog_record_if_owner_authorization(self):
        self.create_end_user_data_catalogs()
        restricted_cr_json = self.get_restricted_cr_with_files_and_dirs_from_api_with_file_details(True)

        # Verify all file and dir details info is returned for restricted owner-owned cr
        # /rest/datasets/<pk>?file_details with owner authorization
        self._assert_ok(restricted_cr_json, 'owner')

    def test_returns_all_details_for_embargoed_catalog_record_if_available_reached_and_no_authorization(self):
        available_embargoed_cr_json = self.get_embargoed_cr_with_files_and_dirs_from_api_with_file_details(True)

        # Verify all file and dir details info is returned for embargoed cr /rest/datasets/<pk>?file_details when
        # embargo date has been reached without authorization
        self._assert_ok(available_embargoed_cr_json, 'no')

    # THE FORBIDDEN TESTS

    def test_returns_limited_info_for_restricted_catalog_record_if_no_authorization(self):
        restricted_cr_json = self.get_restricted_cr_with_files_and_dirs_from_api_with_file_details()

        # Verify limited file and dir info for restricted cr /rest/datasets/<pk>?file_details without authorization
        self._assert_limited_or_no_file_dir_info(restricted_cr_json, 'no')

    def test_returns_limited_info_for_embargoed_catalog_record_if_available_not_reached_and_no_authorization(self):
        not_available_embargoed_cr_json = self.get_embargoed_cr_with_files_and_dirs_from_api_with_file_details(False)

        # Verify limited file and dir info for embargoed cr /rest/datasets/<pk>?file_details when embargo date has not
        # been reached without authorization
        self._assert_limited_or_no_file_dir_info(not_available_embargoed_cr_json, 'no')

    def _assert_limited_or_no_file_dir_info(self, cr_json, credentials_type):
        self._set_http_authorization(credentials_type)

        file_amt = len(cr_json['research_dataset']['files'])
        dir_amt = len(cr_json['research_dataset']['directories'])
        pk = cr_json['id']

        response = self.client.get('/rest/datasets/{0}?file_details'.format(pk))
        self.assertEqual(response.status_code, status.HTTP_200_OK)

        self.assertEqual(len(response.data['research_dataset']['files']), file_amt)
        self.assertEqual(len(response.data['research_dataset']['directories']), dir_amt)

        for f in response.data['research_dataset']['files']:
            self.assertTrue('details' in f)
            # The below assert is a bit arbitrary
            self.assertTrue(len(f['details'].keys()) < 5)
        for d in response.data['research_dataset']['directories']:
            self.assertTrue('details' in d)
            # The below assert is a bit arbitrary
            self.assertTrue(len(d['details'].keys()) < 5)

    def _assert_ok(self, cr_json, credentials_type):
        self._set_http_authorization(credentials_type)

        file_amt = len(cr_json['research_dataset']['files'])
        dir_amt = len(cr_json['research_dataset']['directories'])
        pk = cr_json['id']

        response = self.client.get('/rest/datasets/{0}?file_details'.format(pk))
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data['research_dataset']['files']), file_amt)
        self.assertEqual(len(response.data['research_dataset']['directories']), dir_amt)

        for f in response.data['research_dataset']['files']:
            self.assertTrue('details' in f)
            # The below assert is a bit arbitrary
            self.assertTrue(len(f['details'].keys()) > 5)
        for d in response.data['research_dataset']['directories']:
            self.assertTrue('details' in d)
            # The below assert is a bit arbitrary
            self.assertTrue(len(d['details'].keys()) > 5)


class CatalogRecordApiReadFiles(CatalogRecordApiReadCommon):

    """
    Test /datasets/pid/files api
    """

    def test_get_files(self):
        file_count = CatalogRecord.objects.get(pk=1).files.count()
        response = self.client.get('/rest/datasets/1/files')
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data), file_count)

    def test_get_files_specified_fields_only(self):
        """
        Test use of query parameter ?file_fields=x,y,z
        """
        response = self.client.get('/rest/datasets/1/files?file_fields=identifier,file_path')
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data[0].keys()), 2)
        self.assertEqual('identifier' in response.data[0], True)
        self.assertEqual('file_path' in response.data[0], True)

    def test_removed_query_param(self):
        """
        Test use of query parameter removed_files=bool in /datasets/pid/files, which should return
        only deleted files.
        """
        response = self.client.get('/rest/datasets/1/files')
        file_ids_before = set([ f['id'] for f in response.data ])
        obj = File.objects.get(pk=1)
        obj.removed = True
        obj.force_save()
        obj2 = File.objects.get(pk=2)
        obj2.removed = True
        obj2.force_save()

        response = self.client.get('/rest/datasets/1/files')
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.content)
        self.assertEqual(len(response.data), 0)

        response = self.client.get('/rest/datasets/1/files?removed_files=true')
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        self.assertEqual(len(response.data), len(file_ids_before))
        self.assertEqual(file_ids_before, set([ f['id'] for f in response.data ]))


class CatalogRecordApiReadFilesAuthorization(CatalogRecordApiReadCommon):
    """
    Test /datasets/pid/files api from authorization perspective
    """

    # THE OK TESTS

    def test_returns_ok_for_open_catalog_record_if_no_authorization(self):
        open_cr_json = self.get_open_cr_with_files_and_dirs_from_api_with_file_details()

        # Verify open dataset /rest/datasets/<pk>/files returns all the files even without authorization
        self._assert_ok(open_cr_json, 'no')

    def test_returns_ok_for_open_catalog_record_if_service_authorization(self):
        open_cr_json = self.get_open_cr_with_files_and_dirs_from_api_with_file_details()

        # Verify open dataset /rest/datasets/<pk>/files returns all the files with service authorization
        self._assert_ok(open_cr_json, 'service')

    @responses.activate
    def test_returns_ok_for_open_catalog_record_if_owner_authorization(self):
        self.create_end_user_data_catalogs()
        open_cr_json = self.get_open_cr_with_files_and_dirs_from_api_with_file_details(True)

        # Verify open owner-owned dataset /rest/datasets/<pk>/files returns all the files with owner authorization
        self._assert_ok(open_cr_json, 'owner')

    def test_returns_ok_for_restricted_catalog_record_if_service_authorization(self):
        restricted_cr_json = self.get_restricted_cr_with_files_and_dirs_from_api_with_file_details()

        # Verify restricted dataset /rest/datasets/<pk>/files returns all the files with service authorization
        self._assert_ok(restricted_cr_json, 'service')

    @responses.activate
    def test_returns_ok_for_restricted_catalog_record_if_owner_authorization(self):
        self.create_end_user_data_catalogs()
        restricted_cr_json = self.get_restricted_cr_with_files_and_dirs_from_api_with_file_details(True)

        # Verify restricted owner-owned dataset /rest/datasets/<pk>/files returns all the files with
        # owner authorization
        self._assert_ok(restricted_cr_json, 'owner')

    def test_returns_ok_for_embargoed_catalog_record_if_available_reached_and_no_authorization(self):
        available_embargoed_cr_json = self.get_embargoed_cr_with_files_and_dirs_from_api_with_file_details(True)

        # Verify restricted dataset /rest/datasets/<pk>/files returns ok when embargo date has been reached without
        # authorization
        self._assert_ok(available_embargoed_cr_json, 'no')

    # THE FORBIDDEN TESTS

    def test_returns_forbidden_for_restricted_catalog_record_if_no_authorization(self):
        restricted_cr_json = self.get_restricted_cr_with_files_and_dirs_from_api_with_file_details()

        # Verify restricted dataset /rest/datasets/<pk>/files returns forbidden without authorization
        self._assert_forbidden(restricted_cr_json, 'no')

    def test_returns_forbidden_for_embargoed_catalog_record_if_available_not_reached_and_no_authorization(self):
        not_available_embargoed_cr_json = self.get_embargoed_cr_with_files_and_dirs_from_api_with_file_details(False)

        # Verify restricted dataset /rest/datasets/<pk>/files returns forbidden when embargo date has not been reached
        self._assert_forbidden(not_available_embargoed_cr_json, 'no')

    def _assert_forbidden(self, cr_json, credentials_type):
        pk = cr_json['id']
        self._set_http_authorization(credentials_type)
        response = self.client.get('/rest/datasets/{0}/files'.format(pk))
        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN)

    def _assert_ok(self, cr_json, credentials_type):
        pk = cr_json['id']
        self._set_http_authorization(credentials_type)
        rd = cr_json['research_dataset']
        file_amt = len(rd['files']) + sum(int(d['details']['file_count']) for d in rd['directories'])
        response = self.client.get('/rest/datasets/{0}/files'.format(pk))
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data), file_amt)
