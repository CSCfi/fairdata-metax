from time import sleep

from django.apps import AppConfig
from django.conf import settings
from elasticsearch import Elasticsearch
from elasticsearch.client import IndicesClient
from elasticsearch.helpers import scan

from metax_api.utils import RedisSentinelCache

import logging
_logger = logging.getLogger(__name__)
d = logging.getLogger(__name__).debug

class OnAppStart(AppConfig):

    name = 'metax_api'
    verbose_name = "Metax API"

    def ready(self): # pragma: no cover
        """
        On application startup, populate cache with reference data from elasticsearch if cache is empty.
        Usually there are many app processes being ran by gunicorn, but this action should only be executed
        once. The key 'reference_data_fetch_running' in the cache is set to true by the fastest process,
        informing other processes to not proceed further.
        """
        cache = RedisSentinelCache(master_only=True)

        if settings.ELASTICSEARCH['ALWAYS_RELOAD_CACHE_ON_RESTART']:
            cache.set('reference_data', None)

        reference_data = cache.get('reference_data')

        if reference_data:
            # d('cache already populated')
            return

        if cache.get('reference_data_fetch_running'):
            # d('another process is already fetching reference data, skipping')
            return

        # no reference data found and no other process yet working on it yet - fetch from elasticsearch
        cache.set('reference_data_fetch_running', True)
        _logger.info('Metax API startup - populating cache with reference data...')

        # ensure other processes have stopped at reference_data_fetch_running
        # before we reset the flag.
        sleep(2)

        # reset the flag right after sleep, to ensure the reset is never prevented by any
        # unforeseen errors or cosmic rays during fetching, which might block this automatic
        # fetch on a future restart.
        cache.set('reference_data_fetch_running', False)

        esclient = Elasticsearch([{ 'host': settings.ELASTICSEARCH['HOST'], 'port': settings.ELASTICSEARCH['PORT'] }])
        indicesclient = IndicesClient(esclient)
        reference_data = {}

        for index_name, index_info in indicesclient.get_mapping().items():
            reference_data[index_name] = {}
            for type_name in index_info['mappings'].keys():
                all_rows = scan(esclient, query={'query': {'match_all': {}}}, index=index_name, doc_type=type_name)
                reference_data[index_name][type_name] = [ row['_source']['uri'] for row in all_rows if row['_source']['uri'] ]

        cache.set('reference_data', reference_data)

        reference_data_check = cache.get('reference_data')

        if 'reference_data' not in reference_data_check.keys():
            _logger.warning('Key reference_data missing from reference data - something went wrong during cache population?')

        if 'organization_data' not in reference_data_check.keys():
            _logger.warning('Key organization_data missing from reference data - something went wrong during cache population?')

        _logger.info('Metax API startup - cache populated')
