# This file is part of the Metax API service
#
# Copyright 2017-2020 Ministry of Education and Culture, Finland
#
# :author: CSC - IT Center for Science Ltd., Espoo Finland <servicedesk@csc.fi>
# :license: MIT

from copy import deepcopy

from rest_framework import status

from metax_api.models import (
    CatalogRecord,
    CatalogRecordV2
)
from .write import CatalogRecordApiWriteCommon


CR = CatalogRecordV2

class CatalogRecordApiLock(CatalogRecordApiWriteCommon):
    """
    Tests that no datasets created or edited by v2 api can be further
    modified by using v1 api beacuse v2 is not fully compatible with v1 api.
    """

    def setUp(self):
        super().setUp()

    def _create_v1_dataset(self, cumulative=False, cr=None, draft=False):
        if not cr:
            cr = self.cr_test_data

        if cumulative:
            # not to mess up with the original test dataset
            cr = deepcopy(cr)
            cr['cumulative_state'] = 1

        if draft:
            params = 'draft'
        else:
            params = ''

        response = self.client.post(f'/rest/datasets?{params}', cr, format='json')
        self.assertEqual(response.status_code, status.HTTP_201_CREATED)
        self.assertEqual(response.data['api_meta']['version'], 1)

        cr = CatalogRecord.objects.get(pk=response.data['id'])
        self.assertEqual(cr.api_meta['version'], 1, 'api_version should be 1')

        return response.data

    def _create_v2_dataset(self, cumulative=False, cr=None, draft=False):
        if not cr:
            cr = self.cr_test_data

        if cumulative:
            # not to mess up with the original test dataset
            cr = deepcopy(cr)
            cr['cumulative_state'] = 1

        if draft:
            params = 'draft'
        else:
            params = ''

        response = self.client.post(f'/rest/v2/datasets?{params}', cr, format='json')
        self.assertEqual(response.status_code, status.HTTP_201_CREATED)
        self.assertEqual(response.data['api_meta']['version'], 2)

        cr = CatalogRecord.objects.get(pk=response.data['id'])
        self.assertEqual(cr.api_meta['version'], 2, 'api_version should be 2')

        return response.data

    def test_api_version_is_set_on_creation(self):
        """
        Test that v2 is locked on creation
        """
        response = self.client.post('/rest/v2/datasets', self.cr_test_data, format='json')
        self.assertEqual(response.status_code, status.HTTP_201_CREATED)
        self.assertEqual(response.data['api_meta']['version'], 2, 'api_version should be 2')

        cr = CatalogRecordV2.objects.get(pk=response.data['id'])
        self.assertEqual(cr.api_meta['version'], 2, 'api_version should be 2')

        response.data['research_dataset']['title']['en'] = 'some new title'

        response = self.client.post('/rest/datasets', self.cr_test_data, format='json')
        self.assertEqual(response.status_code, status.HTTP_201_CREATED)
        self.assertEqual(response.data['api_meta']['version'], 1, 'api_version should be 1')

        cr = CatalogRecordV2.objects.get(pk=response.data['id'])
        self.assertEqual(cr.api_meta['version'], 1, 'api_version should be 1')

    def test_version_blocks_all_v1_apis(self):
        """
        Test that none of the v1 update apis are working when dataset version is 2
        """

        cr_v2 = self._create_v2_dataset()

        cr_v2['research_dataset']['title']['en'] = 'totally unique changes to the english title'

        # test basic update operations

        response = self.client.put(f'/rest/datasets/{cr_v2["id"]}', cr_v2, format='json')
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST, 'v1 modifications should have been blocked')
        self.assertTrue('correct api version' in response.data['detail'][0], 'msg should be about api version')

        response = self.client.patch(f'/rest/datasets/{cr_v2["id"]}', cr_v2, format='json')
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST, 'v1 modifications should have been blocked')
        self.assertTrue('correct api version' in response.data['detail'][0], 'msg should be about api version')

        cr_v1 = self._create_v1_dataset()
        cr_v1['research_dataset']['title']['en'] = 'this title update should be file'

        response = self.client.put('/rest/datasets', [cr_v1, cr_v2], format='json')
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST, 'v1 modifications should have been blocked')
        self.assertTrue('correct api version' in response.data['detail'][0], 'msg should be about api version')

        response = self.client.patch('/rest/datasets', [cr_v1, cr_v2], format='json')
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST, 'v1 modifications should have been blocked')
        self.assertTrue('correct api version' in response.data['detail'][0], 'msg should be about api version')

        # test change_cumulative_state

        params = f'identifier={cr_v2["identifier"]}&cumulative_state=1'
        response = self.client.post(f'/rpc/datasets/change_cumulative_state?{params}', format='json')
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST, 'v1 modifications should have been blocked')
        self.assertTrue('correct api version' in response.data['detail'][0], 'msg should be about api version')

        # add directory with some files in it to test other rpc apis
        # dir contains first 5 files 'pid:urn:n'
        cr_dirs = deepcopy(self.cr_test_data)
        cr_dirs['research_dataset']['directories'] = [
            {
                "title": "dir_name",
                "identifier": "pid:urn:dir:3",
                "description": "What is in this directory",
                "use_category": {
                    "in_scheme": "http://uri.suomi.fi/codelist/fairdata/use_category",
                    "identifier": "http://uri.suomi.fi/codelist/fairdata/use_category/code/method"
                }
            }
        ]

        cr_dirs = self._create_v2_dataset(cr=cr_dirs)

        # test refresh_directory_content

        # the directory does not have anything to add but it is not relevant here, since
        # api should return error about the api version before that
        params = f'cr_identifier={cr_dirs["identifier"]}&dir_identifier=pid:urn:dir:3'
        response = self.client.post(f'/rpc/datasets/refresh_directory_content?{params}', format='json')
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST, 'v1 modifications should have been blocked')
        self.assertTrue('correct api version' in response.data['detail'][0], 'msg should be about api version')

        # test fix_deprecated

        response = self.client.delete(f'/rest/files/pid:urn:1')
        self.assertEqual(response.status_code, status.HTTP_200_OK)

        params = f'identifier={cr_dirs["identifier"]}'
        response = self.client.post(f'/rpc/datasets/fix_deprecated?{params}', format='json')
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST, 'v1 modifications should have been blocked')
        self.assertTrue('correct api version' in response.data['detail'][0], 'msg should be about api version')

    def test_version_blocks_all_v2_apis(self):
        """
        Test that none of the v2 apis work on v1 dataset
        """
        # test basic edits

        cr_v1 = self._create_v1_dataset()
        cr_v1['research_dataset']['title']['en'] = 'some new title'

        response = self.client.put(f'/rest/v2/datasets/{cr_v1["id"]}', cr_v1, format='json')
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST, response.data)
        self.assertTrue('correct api version' in response.data['detail'][0], 'msg should be about api version')

        response = self.client.patch(f'/rest/v2/datasets/{cr_v1["id"]}', cr_v1, format='json')
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST, response.data)
        self.assertTrue('correct api version' in response.data['detail'][0], 'msg should be about api version')

        # test POST /rest/v2/datasets/{PID}/files updates api version

        # make dataset cumulative so that file additions are allowed for published datasets
        cr_v1 = self._create_v1_dataset(cumulative=True)

        file_changes = {
            'files': [
                { 'identifier': 'pid:urn:1'}, # adds file, but entry is otherwise not persisted
                { 'identifier': 'pid:urn:2', 'title': 'custom title', 'use_category': { 'identifier': 'source' } },
            ]
        }

        response = self.client.post(f'/rest/v2/datasets/{cr_v1["id"]}/files', file_changes, format='json')
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST, response.data)
        self.assertTrue('correct api version' in response.data['detail'][0], 'msg should be about api version')

        # test PUT/PATCH /rest/v2/datasets/{PID}/files/user_metadata

        for http_verb in ['put', 'patch']:
            update_request = getattr(self.client, http_verb)
            cr = self.cr_test_data
            cr['research_dataset']['files'] = [
                {
                    "title": "customtitle",
                    "identifier": "pid:urn:1",
                    "use_category": {
                        "in_scheme": "http://uri.suomi.fi/codelist/fairdata/use_category",
                        "identifier": "http://uri.suomi.fi/codelist/fairdata/use_category/code/source"
                    }
                },
            ]

            cr_v1 = self._create_v1_dataset(cr)

            user_metadata = {
                'files': [
                    {
                        'identifier': 'pid:urn:1',
                        'title': 'custom title 1',
                        'use_category': {
                            'identifier': 'source'
                        }
                    }
                ]
            }

            response = update_request(
                f'/rest/v2/datasets/{cr_v1["id"]}/files/user_metadata',
                user_metadata,
                format='json'
            )
            self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST, response.data)
            self.assertTrue('correct api version' in response.data['detail'][0], 'msg should be about api version')

        # test change_cumulative_state

        # create one non-cumulative dataset without files so cumulative state could be updated
        cr_v1_non_cum = self.cr_test_data
        cr_v1_non_cum['cumulative_state'] = 0
        del cr_v1_non_cum['research_dataset']['files']
        cr_v1_non_cum = self._create_v1_dataset(cr=cr_v1_non_cum)

        params = f'identifier={cr_v1_non_cum["identifier"]}&cumulative_state=1'
        response = self.client.post(f'/rpc/v2/datasets/change_cumulative_state?{params}', format='json')
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST, response.data)
        self.assertTrue('correct api version' in response.data['detail'][0], 'msg should be about api version')

        cr_v1_cum = self._create_v1_dataset(cumulative=True)

        params = f'identifier={cr_v1_cum["identifier"]}&cumulative_state=2'
        response = self.client.post(f'/rpc/v2/datasets/change_cumulative_state?{params}', format='json')
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST, response.data)
        self.assertTrue('correct api version' in response.data['detail'][0], 'msg should be about api version')

        # test create_draft

        cr_v1 = self._create_v1_dataset()

        params = f'identifier={cr_v1["identifier"]}'
        response = self.client.post(f'/rpc/v2/datasets/create_draft?{params}', format='json')
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST, response.data)
        self.assertTrue('correct api version' in response.data['detail'][0], 'msg should be about api version')

        # test create_new_version

        cr_v1 = self._create_v1_dataset()

        params = f'identifier={cr_v1["identifier"]}'
        response = self.client.post(f'/rpc/v2/datasets/create_new_version?{params}', format='json')
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST, response.data)
        self.assertTrue('correct api version' in response.data['detail'][0], 'msg should be about api version')

        # test publish dataset

        # this test is only relevant when v1 have drafts enabled
        cr_v1 = self._create_v1_dataset(draft=True)

        params = f'identifier={cr_v1["identifier"]}'
        response = self.client.post(f'/rpc/v2/datasets/publish_dataset?{params}', format='json')
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST, response.data)
        # don't check the error message because if the drafts are not enabled on v1, it will complain that
        # dataset is already published

        # test merge draft

        # this test is only relevant when v1 have drafts enabled
        cr_v1 = self._create_v1_dataset(draft=True)

        params = f'identifier={cr_v1["identifier"]}'
        response = self.client.post(f'/rpc/v2/datasets/merge_draft?{params}', format='json')
        self.assertEqual(response.status_code, status.HTTP_400_BAD_REQUEST, response.data)
        # don't check the error message because if the drafts are not enabled on v1, it will complain that
        # dataset is already published