# This file is part of the Metax API service
#
# Copyright 2017-2018 Ministry of Education and Culture, Finland
#
# :author: CSC - IT Center for Science Ltd., Espoo Finland <servicedesk@csc.fi>
# :license: MIT

import json
import requests
import yaml
import urllib3
from django.conf import settings
from django.core.management.base import BaseCommand, CommandError
from icecream import ic

from metax_api.utils import executing_test_case


urllib3.disable_warnings()


class Command(BaseCommand):

    help = 'Load initial data for Metax: Data catalogs, file storages.'

    def add_arguments(self, parser):
        # optional arguments to pass in special settings during test case execution
        parser.add_argument(
            '--test-settings',
            type=str,
            nargs='?',
            help='Set url to use during test execution',
        )

    def handle(self, *args, **options):
        if executing_test_case():
            self._set_test_confs(options)
        else:
            self._set_real_confs()

        self._load_data_catalogs()
        self._load_file_storages()

    def _set_test_confs(self, options):
        """
        For test case execution, a testserver url and test metax credentials are expected
        as parameters from the testrunner.
        """
        if 'test_settings' not in options:
            raise CommandError('test case execution requires settings as a parameter')

        test_opts = options['test_settings']
        self._metax_api_root = test_opts['metax_url']
        self._metax_api_user = test_opts['metax_credentials']['username'], test_opts['metax_credentials']['password']

        if self._metax_api_root == 'https://localhost':
            # extra precaution...
            raise CommandError('Test case tried to write into real db')

    def _set_real_confs(self):
        """
        Set metax url to localhost, and get user credentials from app_config.
        """
        self._metax_api_root = settings.METAX_API_ROOT

        for user in settings.API_USERS:
            if user['username'] == 'metax':
                self._metax_api_user = (user['username'], user['password'])
                break
        else:
            raise CommandError('Could not find metax-user from app_config ?')

    def _error_is_already_exists(self, details):
        try:
            for field_name, errors in details.items():
                if field_name == 'identifier' and 'already exists' in errors[0]:
                    return True
        except:
            pass
        return False

    def _load_data_catalogs(self):
        try:
            with open('metax_api/initialdata/datacatalogs.json', 'r') as f:
                data_catalogs = json.load(f)
        except FileNotFoundError:
            raise CommandError('File initialdata/datacatalogs.json does not exist?')
        except json.decoder.JSONDecodeError as e:
            raise CommandError('Error loading data catalog json: %s' % str(e))

        self.stdout.write('Creating %d data catalogs...' % len(data_catalogs))

        for dc in data_catalogs:
            response = requests.post('%s/rest/v2/datacatalogs' % self._metax_api_root,
                json=dc, auth=self._metax_api_user, verify=False)
            # ic(response)

            if response.status_code == 201:
                self.stdout.write('Created catalog: %s' % dc['catalog_json']['identifier'])
            else:
                # update instead
                try:
                    errors = response.json()
                except:
                    raise CommandError(response.content)

                if self._error_is_already_exists(errors.get('catalog_json', {})):
                    self.stdout.write('Catalog %s already exists, updating instead...' %
                        dc['catalog_json']['identifier'])

                    response = requests.put('%s/rest/v2/datacatalogs/%s' %
                        (self._metax_api_root, dc['catalog_json']['identifier']),
                        json=dc, auth=self._metax_api_user, verify=False)

                    if response.status_code == 200:
                        self.stdout.write('Updated catalog: %s' % dc['catalog_json']['identifier'])
                        continue

                # create or update ended in error
                raise CommandError('Failed to process catalog: %s. Reason: %s' %
                    (dc['catalog_json']['identifier'], errors))

    def _load_file_storages(self):
        try:
            with open('metax_api/initialdata/filestorages.json', 'r') as f:
                storages = json.load(f)
        except FileNotFoundError:
            raise CommandError('File initialdata/filestorages.json does not exist?')
        except json.decoder.JSONDecodeError as e:
            raise CommandError('Error loading file storage json: %s' % str(e))

        self.stdout.write('Creating %d file storages...' % len(storages))

        for fs in storages:
            response = requests.post('%s/rest/filestorages' % self._metax_api_root,
                json=fs, auth=self._metax_api_user, verify=False)

            if response.status_code == 201:
                self.stdout.write('Created file storage: %s' % fs['file_storage_json']['identifier'])
            else:
                # update instead
                try:
                    errors = response.json()
                except:
                    raise CommandError(response.content)
                if self._error_is_already_exists(errors.get('file_storage_json', {})):
                    self.stdout.write('File storage %s already exists, updating instead...' %
                        fs['file_storage_json']['identifier'])

                    response = requests.put('%s/rest/filestorages/%s' %
                        (self._metax_api_root, fs['file_storage_json']['identifier']),
                        json=fs, auth=self._metax_api_user, verify=False)

                    if response.status_code == 200:
                        self.stdout.write('Updated file storage: %s' % fs['file_storage_json']['identifier'])
                        continue

                # create or update ended in error
                raise CommandError('Failed to process storage: %s. Reason: %s' %
                    (fs['file_storage_json']['identifier'], errors))
