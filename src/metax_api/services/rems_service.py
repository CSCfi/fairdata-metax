# This file is part of the Metax API service
#
# Copyright 2019 Ministry of Education and Culture, Finland
#
# :author: CSC - IT Center for Science Ltd., Espoo Finland <servicedesk@csc.fi>
# :license: MIT
import logging
import requests

from django.conf import settings as django_settings


_logger = logging.getLogger(__name__)


class REMSException(Exception):
    pass

class REMSService():

    def __init__(self):
        if not hasattr(django_settings, 'REMS'):
            raise Exception('Missing configuration from settings.py: REMS')

        settings = django_settings.REMS

        self.api_key        = settings['API_KEY']
        self.base_url       = settings['BASE_URL']
        self.etsin_url      = settings['ETSIN_URL_TEMPLATE']
        self.metax_user     = settings['METAX_USER']
        self.auto_approver  = settings['AUTO_APPROVER']
        self.form_id        = settings['FORM_ID']

        self.headers = {
            "x-rems-api-key": self.api_key,
            "x-rems-user-id": self.metax_user,
            "Content-Type": "application/json"
        }

    def create_rems_entity(self, cr, user_info):
        """
        Creates all the necessary elements to create catalogue-item for the dataset in REMS
        """
        self.cr = cr

        # create user. Successful even if userid is already taken
        self._post_rems('user', user_info)

        wf_id = self._create_workflow(user_info['userid'])
        license_id = self._create_license()
        res_id = self._create_resource(license_id)

        self._create_catalogue_item(res_id, wf_id)

    def _create_workflow(self, user_id):
        body = {
            "organization": self.cr.metadata_owner_org,
            "title": self.cr.research_dataset['preferred_identifier'],
            "type": 'workflow/default',
            "handlers": [user_id]
        }

        response = self._post_rems('workflow', body)

        return response['id']

    def _create_license(self):
        """
        Checks if license is already found from REMS before creating new one
        """
        license = self.cr.research_dataset['access_rights']['license'][0]
        license_url = license.get('identifier') or license['license']

        # no search parameter provided for license so have to check by hand
        rems_licenses = self._get_rems('license', 'disabled=true&archived=true')
        for l in rems_licenses:
            if any( [v['textcontent'] == license_url for v in l['localizations'].values()] ):
                return l['id']

        body = {
            "licensetype": 'link',
            "localizations": {}
        }

        for lang in list(license['title'].keys()):
            body['localizations'].update({
                lang: {
                    "title": license['title'][lang],
                    "textcontent": license_url
                }
            })

        response = self._post_rems('license', body)

        return response['id']

    def _create_resource(self, license_id):
        body = {
            "resid": self.cr.research_dataset['preferred_identifier'],
            "organization": self.cr.metadata_owner_org,
            "licenses": [license_id]
        }

        response = self._post_rems('resource', body)

        return response['id']

    def _create_catalogue_item(self, res_id, wf_id):
        rd_title = self.cr.research_dataset['title']

        body = {
            "form": self.form_id,
            "resid": res_id,
            "wfid": wf_id,
            "localizations": {},
            "enabled": True
        }

        for lang in list(rd_title.keys()):
            body['localizations'].update({
                lang: {
                    "title": rd_title[lang],
                    "infourl": self.etsin_url % self.cr.identifier
                }
            })

        response = self._post_rems('catalogue-item', body)

        return response['id']

    def _post_rems(self, entity, body):
        try:
            response = requests.post(f"{self.base_url}/{entity}s/create", json=body, headers=self.headers)

        except Exception as e:
            _logger.error(f'Connection to REMS failed while creating {entity}. Error: {e}')
            raise Exception(e)

        if response.status_code != 200:
            raise REMSException(f'REMS returned bad status while creating {entity}. Error: {response.text}')

        # operation status is in body
        resp = response.json()

        if not resp['success']:
            raise REMSException(f'Could not create {entity} to REMS. Error: {resp["errors"]}')

        return resp

    def _get_rems(self, entity, params=''):
        try:
            response = requests.get(f"{self.base_url}/{entity}s?{params}", headers=self.headers)

        except Exception as e:
            _logger.error(f'Connection to REMS failed while getting {entity}. Error: {e}')
            raise Exception(e)

        if response.status_code != 200:
            raise REMSException(f'REMS returned bad status while getting {entity}. Error: {response.text}')

        # operation should be successful if status code 200
        return response.json()
