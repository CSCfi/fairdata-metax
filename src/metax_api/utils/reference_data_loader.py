import logging

from django.conf import settings as django_settings
from elasticsearch import Elasticsearch
from elasticsearch.client import IndicesClient
from elasticsearch.helpers import scan

from .utils import executing_test_case

_logger = logging.getLogger(__name__)
d = logging.getLogger(__name__).debug


class ReferenceDataLoader():

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

        if not cache.get_or_set('reference_data_load_executing', True, ex=120):
            # d('another process is already executing reference_data load from ES')
            return 'reload_started_by_other'

        _logger.info('ReferenceDataLoader - populating cache...')

        if executing_test_case():
            _logger.info('(Note: populating test suite cache)')

        try:
            reference_data = cls._fetch_reference_data(settings)
        except:
            _logger.exception('Reference data fetch failed')
            raise

        cache.set('reference_data', reference_data)

        errors = None
        reference_data_check = cache.get('reference_data', master=True)

        if 'reference_data' not in reference_data_check.keys():
            _logger.warning('Key reference_data missing from reference data - something went wrong during cache population?')
            errors = True

        if 'organization_data' not in reference_data_check.keys():
            _logger.warning('Key organization_data missing from reference data - something went wrong during cache population?')
            errors = True

        _logger.info('ReferenceDataLoader - %s' % ('failed to populate cache' if errors else 'cache populated'))
        cache.delete('reference_data_load_executing')

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

                    #
                    # form entry that will be placed into cache
                    #

                    try:
                        # should always be present
                        entry = { 'uri': row['_source']['uri'] }
                    except KeyError:
                        _logger.warning('Elasticsearch document missing uri in index {0} type {1}: {2}'.format(index_name, type_name, row))
                        continue

                    try:
                        # should always be present
                        entry['code'] = row['_source']['code']
                    except KeyError:
                        # error, but not blocking. place key 'code' anyway so that the existence
                        # of the key does not have to be checked elsewhere
                        entry['code'] = None
                        _logger.warning('Elasticsearch document missing code in index {0} type {1}: {2}'.format(index_name, type_name, row))

                    label = row['_source'].get('label', None)

                    # dont want empty dicts loitering in the cache
                    if label:
                        entry['label'] = label

                    if type_name == 'location':
                        entry['wkt'] = row['_source'].get('wkt', None)

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
