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

HANDLER_CLOSEABLE_APPLICATIONS = [
    'application.state/approved',
    'application.state/returned',
    'application.state/submitted'
]

APPLICANT_CLOSEABLE_APPLICATIONS = [
    'application.state/draft'
]

class REMSException(Exception):
    pass

class REMSService():

    def __init__(self):
        if not hasattr(django_settings, 'REMS'):
            raise Exception('Missing configuration from settings.py: REMS')

        settings = django_settings.REMS

        # only reporter_user is privileged to get all applications from REMS
        self.api_key        = settings['API_KEY']
        self.base_url       = settings['BASE_URL']
        self.etsin_url      = settings['ETSIN_URL_TEMPLATE']
        self.metax_user     = settings['METAX_USER']
        self.reporter_user  = settings['REPORTER_USER']
        self.auto_approver  = settings['AUTO_APPROVER']
        self.form_id        = settings['FORM_ID']

        self.headers = {
            "x-rems-api-key": self.api_key,
            "x-rems-user-id": self.metax_user,
            "Content-Type": "application/json"
        }

        try:
            response = requests.get(f"{self.base_url}/health", headers=self.headers)
        except Exception as e:
            raise Exception(f'Cannot connect to rems while checking its health. Error {e}')

        if not response.json()['healthy'] is True:
            raise REMSException('Rems is not healthy, request is aborted')

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

    def close_rems_entity(self, cr, reason):
        """
        Closes all applications and archives and disables all related entities
        """
        pref_id = cr.research_dataset['preferred_identifier']
        title = cr.research_dataset['title'].get('en') or cr.research_dataset['title'].get('fi')

        rems_ci = self._get_rems(
            'catalogue-item',
            f'resource={pref_id}&archived=true&disabled=true'
        )

        if len(rems_ci) < 1:
            # this should not happen but do not block the metax dataset removal
            _logger.error(f'Could not find catalogue-item for {cr.identifier} in REMS.')
            return

        self._close_applications(title, pref_id, reason)

        self._close_entity('catalogue-item',    rems_ci[0]['id'])
        self._close_entity('workflow',          rems_ci[0]['wfid'])
        self._close_entity('resource',          rems_ci[0]['resource-id'])

    def _close_applications(self, title, pref_id, reason):
        """
        Get all applications that are related to dataset and close them.
        Application state determines which user (applicant or handler) can close the application.
        Furthermore, closed, rejected or revoked applications cannot be closed.
        """
        # REMS only allows reporter_user to get all applications
        self.headers['x-rems-user-id'] = self.reporter_user

        applications = self._get_rems('application', f'query=resource:\"{pref_id}\"')

        for application in applications:
            if application['application/state'] in HANDLER_CLOSEABLE_APPLICATIONS:
                closing_user = application['application/workflow']['workflow.dynamic/handlers'][0]['userid']
            elif application['application/state'] in APPLICANT_CLOSEABLE_APPLICATIONS:
                closing_user = application['application/applicant']['userid']
            else:
                continue

            self.headers['x-rems-user-id'] = closing_user

            body = {"application-id": application['application/id'], "comment": f"Closed due to dataset {reason}"}

            self._post_rems('application', body, 'close')

        self.headers['x-rems-user-id'] = self.metax_user

    def _close_entity(self, entity, id):
        body_ar = {'id': id, 'archived': True}
        body_en = {'id': id, 'enabled': False}

        self._put_rems(entity, 'archived', body_ar)
        self._put_rems(entity, 'enabled', body_en)

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

    def _post_rems(self, entity, body, action='create'):
        """
        Send post to REMS. Action is needed as parameter because applications are closed with post.
        """
        try:
            response = requests.post(f"{self.base_url}/{entity}s/{action}", json=body, headers=self.headers)

        except Exception as e:
            raise Exception(f'Connection to REMS failed while creating {entity}. Error: {e}')

        if response.status_code != 200:
            raise REMSException(f'REMS returned bad status while creating {entity}. Error: {response.text}')

        # operation status is in body
        resp = response.json()

        if not resp['success']:
            raise REMSException(f'Could not {action} {entity} to REMS. Error: {resp["errors"]}')

        return resp

    def _put_rems(self, entity, action, body):
        """
        Edit rems entity. Possible actions: [edit, archived, enabled].
        """
        try:
            response = requests.put(f"{self.base_url}/{entity}s/{action}", json=body, headers=self.headers)

        except Exception as e:
            raise Exception(f'Connection to REMS failed while updating {entity}. Error: {e}')

        if response.status_code != 200:
            raise REMSException(f'REMS returned bad status while updating {entity}. Error: {response.text}')

        # operation status is in body
        resp = response.json()

        if not resp['success']:
            raise REMSException(f'Could not update {entity} to REMS. Error: {resp["errors"]}')

        return resp

    def _get_rems(self, entity, params=''):
        try:
            response = requests.get(f"{self.base_url}/{entity}s?{params}", headers=self.headers)

        except Exception as e:
            raise Exception(f'Connection to REMS failed while getting {entity}. Error: {e}')

        if response.status_code != 200:
            raise REMSException(f'REMS returned bad status while getting {entity}. Error: {response.text}')

        # operation should be successful if status code 200
        return response.json()
