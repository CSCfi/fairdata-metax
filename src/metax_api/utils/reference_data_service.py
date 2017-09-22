from django.conf import settings as django_settings
from elasticsearch import Elasticsearch
from elasticsearch.client import IndicesClient
from elasticsearch.helpers import scan

from .utils import executing_test_case

import logging
_logger = logging.getLogger(__name__)
d = logging.getLogger(__name__).debug

ONE_DAY = 86400

class ReferenceDataService():

    """
    Should optimally be defined in /services/, but services __init__.py cant be loaded during app startup due to having imports from
    django app models, views etc
    """

    @classmethod
    def populate_cache_reference_data(cls, cache, settings=django_settings):
        """
        Fetch reference data from elasticsearch server and save to local cache

        cache: cache object to use for saving
        settings: override elasticsearch settings in settings.py
        """

        _logger.info('Metax API startup - populating cache with reference data...')

        if executing_test_case():
            _logger.info('(Note: populating test suite cache)')

        try:
            reference_data = cls._fetch_reference_data(settings)
        except:
            _logger.exception('Reference data fetch failed')
            reference_data = {}

        cache.set('reference_data', reference_data, ex=ONE_DAY * 2)

        reference_data_check = cache.get('reference_data')

        if 'reference_data' not in reference_data_check.keys():
            _logger.warning('Key reference_data missing from reference data - something went wrong during cache population?')

        if 'organization_data' not in reference_data_check.keys():
            _logger.warning('Key organization_data missing from reference data - something went wrong during cache population?')

        _logger.info('Metax API startup - cache populated')

    @classmethod
    def _fetch_reference_data(cls, settings):
        if not isinstance(settings, dict):
            settings = settings.ELASTICSEARCH

        connection_params = cls._get_connection_parameters(settings)
        esclient = Elasticsearch(settings['HOSTS'], **connection_params)
        indicesclient = IndicesClient(esclient)
        reference_data = {}
        for index_name, index_info in indicesclient.get_mapping().items():
            reference_data[index_name] = {}
            for type_name in index_info['mappings'].keys():
                reference_data[index_name][type_name] = []
                all_rows = scan(esclient, query={'query': {'match_all': {}}}, index=index_name, doc_type=type_name)
                for row in all_rows:
                    entry = {}
                    if row['_source'].get('uri', False):
                        entry['uri'] = row['_source']['uri']
                        if row['_source'].get('code', False):
                            entry['code'] = row['_source']['code']
                        reference_data[index_name][type_name].append(entry)

        return reference_data

    @staticmethod
    def _get_connection_parameters(settings):
        """
        https://docs.objectrocket.com/elastic_python_examples.html
        """
        if settings['HOSTS'][0] != 'localhost':
            conf = { 'send_get_body_as': 'GET' }
            if settings.get('USE_SSL', False):
                conf.update({ 'port': 443, 'use_ssl': True, 'verify_certs': True, })
            if settings.get('PORT', False):
                conf.update('port', settings['PORT'])
            return conf
        return {}
