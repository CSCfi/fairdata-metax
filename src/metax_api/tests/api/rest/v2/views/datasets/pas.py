# This file is part of the Metax API service
#
# Copyright 2017-2020 Ministry of Education and Culture, Finland
#
# :author: CSC - IT Center for Science Ltd., Espoo Finland <servicedesk@csc.fi>
# :license: MIT

from datetime import timedelta

from django.conf import settings as django_settings
from rest_framework import status

from metax_api.models import CatalogRecordV2, Contract, DataCatalog
from metax_api.utils import get_tz_aware_now_without_micros

from .read import CatalogRecordApiReadCommon
from .write import CatalogRecordApiWriteCommon


class CatalogRecordApiReadPreservationStateTests(CatalogRecordApiReadCommon):

    """
    preservation_state filtering
    """

    def test_read_catalog_record_search_by_preservation_state(self):
        '''
        Various simple filtering requests
        '''
        response = self.client.get('/rest/v2/datasets?preservation_state=0')
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data['results']) > 2, True,
            'There should have been multiple results for preservation_state=0 request')

        response = self.client.get('/rest/v2/datasets?preservation_state=10')
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data['results']), 2)

        response = self.client.get('/rest/v2/datasets?preservation_state=40')
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data['results']), 1)

    def test_read_catalog_record_search_by_preservation_state_666(self):
        response = self.client.get('/rest/v2/datasets?preservation_state=666')
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data['results']), 0, 'should return empty list')

    def test_read_catalog_record_search_by_preservation_state_many(self):
        response = self.client.get('/rest/v2/datasets?preservation_state=10,40')
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data['results']), 3)
        self.assertEqual(response.data['results'][0]['preservation_state'], 10)
        self.assertEqual(response.data['results'][1]['preservation_state'], 10)
        self.assertEqual(response.data['results'][2]['preservation_state'], 40)

    def test_read_catalog_record_search_by_preservation_state_invalid_value(self):
        response = self.client.get('/rest/v2/datasets?preservation_state=1,a')
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual('is not an integer' in response.data['preservation_state'][0], True,
                        'Error should say letter a is not an integer')

        response = self.client.get('/rest/v2/datasets?preservation_state=1,a')
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST)
        self.assertEqual('is not an integer' in response.data['preservation_state'][0], True,
                        'Error should say letter a is not an integer')


class CatalogRecordApiReadPASFilter(CatalogRecordApiReadCommon):

    def test_pas_filter(self):
        """
        Test query param pas_filter which should search from various fields using the same search term.
        """

        # set test conditions
        cr = CatalogRecordV2.objects.get(pk=1)
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

        metax_user = django_settings.API_METAX_USER
        self._use_http_authorization(username=metax_user['username'], password=metax_user['password'])

        # beging testing

        response = self.client.get('/rest/v2/datasets?preservation_state=10&pas_filter=if you')
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        self.assertEqual(len(response.data['results']), 1)

        response = self.client.get('/rest/v2/datasets?preservation_state=10&pas_filter=kiinni jos')
        self.assertEqual(len(response.data['results']), 1)

        response = self.client.get('/rest/v2/datasets?preservation_state=10&pas_filter=niemine')
        self.assertEqual(len(response.data['results']), 1)

        # more than 3 curators, requires typing exact case-sensitive name... see comments in related code
        response = self.client.get('/rest/v2/datasets?preservation_state=10&pas_filter=jokunen')
        self.assertEqual(len(response.data['results']), 0)
        response = self.client.get('/rest/v2/datasets?preservation_state=10&pas_filter=Jaska Jokunen')
        self.assertEqual(len(response.data['results']), 1)

        # contract_id 1 has several other associated test datasets
        response = self.client.get('/rest/v2/datasets?preservation_state=10&pas_filter=agreement')
        self.assertEqual(len(response.data['results']), 3)

        response = self.client.get('/rest/v2/datasets?preservation_state=10&pas_filter=does not exist')
        self.assertEqual(len(response.data['results']), 0)

    def test_pas_filter_is_restricted(self):
        """
        Query param is permitted to users metax and tpas.
        """
        response = self.client.get('/rest/v2/datasets?preservation_state=10&pas_filter=hmmm')
        self.assertEqual(response.status_code, status.HTTP_403_FORBIDDEN)


class CatalogRecordApiWritePreservationStateTests(CatalogRecordApiWriteCommon):

    """
    Field preservation_state related tests.
    """

    def _create_pas_dataset_from_id(self, id):
        """
        Helper method to create a pas dataset by updating the given dataset's
        preservation_state to 80.
        """
        cr_data = self.client.get('/rest/v2/datasets/%d' % id, format="json").data
        self.assertEqual(cr_data['preservation_state'], 0)

        # update state to "accepted to pas" -> should create pas version
        cr_data['preservation_state'] = 80
        response = self.client.put('/rest/v2/datasets/%d' % id, cr_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        return response.data

    def setUp(self):
        super().setUp()
        dc = DataCatalog.objects.get(pk=1)
        catalog_json = dc.catalog_json
        catalog_json['identifier'] = django_settings.PAS_DATA_CATALOG_IDENTIFIER
        catalog_json['dataset_versioning'] = False
        dc = DataCatalog.objects.create(
            catalog_json=catalog_json,
            date_created=get_tz_aware_now_without_micros(),
            catalog_record_services_create='testuser,api_auth_user,metax',
            catalog_record_services_edit='testuser,api_auth_user,metax'
        )

    def test_update_catalog_record_pas_state_allowed_value(self):
        cr = self.client.get('/rest/v2/datasets/1').data
        cr['preservation_state'] = 30
        response = self.client.put('/rest/v2/datasets/1', cr, format="json")
        self.assertEqual(response.status_code, status.HTTP_200_OK)

        cr = CatalogRecordV2.objects.get(pk=1)
        self.assertEqual(cr.preservation_state_modified >= get_tz_aware_now_without_micros() - timedelta(seconds=5),
                         True, 'Timestamp should have been updated during object update')

    def test_update_pas_state_to_needs_revalidation(self):
        """
        When dataset metadata is updated, and preservation_state in (40, 50, 70), metax should
        automatically update preservation_state value to 60 ("validated metadata updated").
        """
        cr = CatalogRecordV2.objects.get(pk=1)

        for i, preservation_state_value in enumerate((40, 50, 70)):
            # set testing initial condition...
            cr.preservation_state = preservation_state_value
            cr.save()

            # retrieve record and ensure testing state was set correctly...
            cr_data = self.client.get('/rest/v2/datasets/1', format="json").data
            self.assertEqual(cr_data['preservation_state'], preservation_state_value)

            # strike and verify
            cr_data['research_dataset']['title']['en'] = 'Metadata has been updated on loop %d' % i
            response = self.client.put('/rest/v2/datasets/1', cr_data, format="json")
            self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
            self.assertEqual(response.data['preservation_state'], 60)

    def test_prevent_file_changes_when_record_in_pas_process(self):
        """
        When preservation_state > 0, changing associated files of a dataset should not be allowed.
        """
        cr = CatalogRecordV2.objects.get(pk=1)
        cr.preservation_state = 10
        cr.save()

        file_changes = {
            'files': [{ 'identifier': 'pid:urn:3' }]
        }

        response = self.client.post('/rest/v2/datasets/1/files', file_changes, format="json")
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST, response.data)
        self.assertEqual('Changing files of a published' in response.data['detail'][0], True, response.data)

    def test_non_pas_dataset_unallowed_preservation_state_values(self):
        # update non-pas dataset
        cr = self.client.get('/rest/v2/datasets/1').data

        values = [
            11, # not one of known values
            90, # value not allowed for non-pas datasets
        ]

        for invalid_value in values:
            cr['preservation_state'] = invalid_value
            response = self.client.put('/rest/v2/datasets/1', cr, format="json")
            self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST, response.data)

    def test_pas_dataset_unallowed_preservation_state_values(self):
        # create pas dataset and update with invalid values
        cr = self.client.get('/rest/v2/datasets/1').data
        cr['preservation_state'] = 80
        response = self.client.put('/rest/v2/datasets/1', cr, format="json")
        cr = self.client.get('/rest/v2/datasets/%d' % response.data['preservation_dataset_version']['id']).data

        values = [
            70,  # value not allowed for non-pas datasets
            111, # not one of known values
            150  # not one of known values
        ]

        for invalid_value in values:
            cr['preservation_state'] = invalid_value
            response = self.client.put('/rest/v2/datasets/1', cr, format="json")
            self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST, response.data)

    def test_pas_version_is_created_on_preservation_state_80(self):
        """
        When preservation_state is updated to 'accepted to pas', a copy should be created into
        designated PAS catalog.
        """
        cr_data = self.client.get('/rest/v2/datasets/1', format="json").data
        self.assertEqual(cr_data['preservation_state'], 0)

        origin_dataset = self._create_pas_dataset_from_id(1)
        self.assertEqual(origin_dataset['preservation_state'], 0)
        self.assertEqual('new_version_created' in origin_dataset, True)
        self.assertEqual(origin_dataset['new_version_created']['version_type'], 'pas')
        self.assertEqual('preservation_dataset_version' in origin_dataset, True)
        self.assertEqual('other_identifier' in origin_dataset['research_dataset'], True)
        self.assertEqual(origin_dataset['research_dataset']['other_identifier'][0]['notation'].startswith('doi'), True)

        # get pas version and verify links and other signature values are there
        pas_dataset = self.client.get(
            '/rest/v2/datasets/%d' % origin_dataset['preservation_dataset_version']['id'], format="json"
        ).data
        self.assertEqual(pas_dataset['data_catalog']['identifier'], django_settings.PAS_DATA_CATALOG_IDENTIFIER)
        self.assertEqual(pas_dataset['preservation_state'], 80)
        self.assertEqual(pas_dataset['preservation_dataset_origin_version']['id'], origin_dataset['id'])
        self.assertEqual(
            pas_dataset['preservation_dataset_origin_version']['preferred_identifier'],
            origin_dataset['research_dataset']['preferred_identifier']
        )
        self.assertEqual('deprecated' in pas_dataset['preservation_dataset_origin_version'], True)
        self.assertEqual('other_identifier' in pas_dataset['research_dataset'], True)
        self.assertEqual(pas_dataset['research_dataset']['other_identifier'][0]['notation'].startswith('urn'), True)

        # when pas copy is created, origin_dataset preservation_state should have been set back to 0
        cr_data = self.client.get('/rest/v2/datasets/1', format="json").data
        self.assertEqual(cr_data['preservation_state'], 0)

        # ensure files match between original and pas cr
        cr = CatalogRecordV2.objects.get(pk=1)
        cr_files = cr.files.filter().order_by('id').values_list('id', flat=True)
        cr_pas_files = cr.preservation_dataset_version.files.filter().order_by('id').values_list('id', flat=True)

        # note: trying to assert querysets will result in failure. must evaluate the querysets first by iterating them
        self.assertEqual([f for f in cr_files], [f for f in cr_pas_files])

    def test_origin_dataset_cant_have_multiple_pas_versions(self):
        """
        If state is update to 'accepted to pas', and relation preservation_dataset_version
        is detected, an error should be raised.
        """
        self._create_pas_dataset_from_id(1)

        cr_data = { 'preservation_state': 80 }
        response = self.client.patch('/rest/v2/datasets/1', cr_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST, response.data)
        self.assertEqual('already has a PAS version' in response.data['detail'][0], True, response.data)

    def test_dataset_can_be_created_directly_into_pas_catalog(self):
        """
        Datasets that are created directly into PAS catalog should not have any enforced
        rules about changing preservation_state value.
        """
        self.cr_test_data['data_catalog'] = django_settings.PAS_DATA_CATALOG_IDENTIFIER
        response = self.client.post('/rest/v2/datasets', self.cr_test_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_201_CREATED, response.data)
        self.assertEqual(
            response.data['research_dataset']['preferred_identifier'].startswith('doi'),
            True,
            response.data['research_dataset']['preferred_identifier']
        )

        # when created directly into pas catalog, preservation_state can be updated
        # to whatever, whenever
        ps_values = [ v[0] for v in CatalogRecordV2.PRESERVATION_STATE_CHOICES ]
        for ps in ps_values:
            cr_data = { 'preservation_state': ps }
            response = self.client.patch('/rest/v2/datasets/%d' % response.data['id'], cr_data, format="json")
            self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)

        cr_data = { 'preservation_state': 0 }
        response = self.client.patch('/rest/v2/datasets/%d' % response.data['id'], cr_data, format="json")
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)

    def test_unfreezing_files_does_not_deprecate_pas_dataset(self):
        """
        Even if the origin dataset is deprecated as a result of unfreezing its files,
        the PAS dataset should be safe from being deprecated, as the files have already
        been stored in PAS.
        """
        cr = self._create_pas_dataset_from_id(1)
        response = self.client.delete('/rest/v2/files/1', format="json")
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)

        response = self.client.get('/rest/v2/datasets/%d' % cr['preservation_dataset_version']['id'], format="json")
        self.assertEqual(response.status_code, status.HTTP_200_OK, response.data)
        self.assertEqual(response.data['deprecated'], False)
