# This file is part of the Metax API service
#
# Copyright 2017-2018 Ministry of Education and Culture, Finland
#
# :author: CSC - IT Center for Science Ltd., Espoo Finland <servicedesk@csc.fi>
# :license: MIT

import logging

from django.conf import settings as django_settings

from .utils import executing_test_case

_logger = logging.getLogger(__name__)


class ReferenceDataLoader():

    """
    Should optimally be defined in /services/, but services __init__.py cant be loaded during app
    startup due to having imports from django app models, views etc
    """

    @classmethod
    def populate_cache_reference_data(cls, cache, settings=django_settings):
        """
        Fetch reference data from elasticsearch server and save to local cache

        cache: cache object to use for saving
        settings: override elasticsearch settings in settings.py
        """

        if not cache.get_or_set('reference_data_load_executing', True, ex=120):
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
            _logger.warning('Key reference_data missing from reference data - '
                            'something went wrong during cache population?')
            errors = True

        if 'organization_data' not in reference_data_check.keys():
            _logger.warning('Key organization_data missing from reference data - '
                            'something went wrong during cache population?')
            errors = True

        if not errors:
            if not isinstance(settings, dict):
                settings = settings.ELASTICSEARCH
            cache.set('ref_data_up_to_date', True, ex=settings['REFERENCE_DATA_RELOAD_INTERVAL'])

        _logger.info('ReferenceDataLoader - %s' % ('failed to populate cache' if errors else 'cache populated'))
        cache.delete('reference_data_load_executing')

    @classmethod
    def _fetch_reference_data(cls, settings):
        if not isinstance(settings, dict):
            settings = settings.ELASTICSEARCH

        connection_params = cls._get_connection_parameters(settings)
        esclient, scan = cls._get_es_imports(settings['HOSTS'], connection_params)

        reference_data = {}
        for index_name in esclient.indices.get_mapping().keys():
            reference_data[index_name] = {}

            # a cumbersome way to fetch the types, but supposedly the only way because nginx restricts ES usage
            # if localhost is not used.
            aggr_types = esclient.search(
                index=index_name,
                body={"aggs": { "types": {"terms": {"field": "type", "size": 30}}}},
                filter_path='aggregations',
                _source='type',
                scroll='1m'
            )

            for type_name in [ b['key'] for b in aggr_types['aggregations']['types']['buckets'] ]:
                reference_data[index_name][type_name] = []
                # must use wildcard query here because organization_data does not have their 'type'
                # field indexed in the old 5.8 version.. This is fixed in the new ES cluster so this could
                # be changed after all envs is using the new version.
                all_rows = scan(
                    esclient,
                    query={'query': {'wildcard': {'id': {'value': f'{type_name}*'}}}},
                    index=index_name
                )
                for row in all_rows:

                    #
                    # form entry that will be placed into cache
                    #

                    try:
                        # should always be present
                        entry = { 'uri': row['_source']['uri'] }
                    except KeyError:
                        _logger.warning('Elasticsearch document missing uri in index {0} type {1}: {2}'.format(
                            index_name, type_name, row))
                        continue

                    try:
                        # should always be present
                        entry['code'] = row['_source']['code']
                    except KeyError:
                        # error, but not blocking. place key 'code' anyway so that the existence
                        # of the key does not have to be checked elsewhere
                        entry['code'] = None
                        _logger.warning('Elasticsearch document missing code in index {0} type {1}: {2}'.format(
                            index_name, type_name, row))

                    label = row['_source'].get('label', None)
                    scheme = row['_source'].get('scheme', None)

                    # dont want empty dicts loitering in the cache
                    if label:
                        entry['label'] = label

                    if scheme:
                        entry['scheme'] = scheme

                    if type_name == 'location':
                        entry['wkt'] = row['_source'].get('wkt', None)

                    if type_name == 'license' and 'same_as' in row['_source'] and len(row['_source']['same_as']) > 0:
                        entry['same_as'] = row['_source']['same_as'][0]

                    if type_name == 'file_format_version':
                        entry['input_file_format'] = row['_source'].get('input_file_format', None)
                        entry['output_format_version'] = row['_source'].get('output_format_version', None)

                    if type_name == 'organization' and row['_source'].get('parent_id', False):
                        entry['parent_org_code'] = row['_source']['parent_id'][len('organization_'):]

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

    @staticmethod
    def _get_es_imports(hosts, conn_params):
        """
        Returns correct version of the elasticsearch python client.
        This is needed in the transition between elasticsearch major versions because
        clients are not compatible with each other. When Elasticsearch has been updated
        in every environment, this can be removed and just import the client as usual.

        NOTE: Whenever there is an major version update of the es cluster, this incompatibility
        issue raises. It might be also good to leave this function here and utilize it on
        coming ES updates as well.
        """
        try:
            # try with the newest client first
            from elasticsearch import Elasticsearch
            from elasticsearch.helpers import scan

            es = Elasticsearch(hosts, **conn_params)
            # raises error if not compatible
            es.search('reference_data')

        except:
            from elasticsearch5 import Elasticsearch
            from elasticsearch5.helpers import scan

            es = Elasticsearch(hosts, **conn_params)

        return es, scan
