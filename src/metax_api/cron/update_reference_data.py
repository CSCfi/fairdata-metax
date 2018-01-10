from os import sys, path
sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))

import yaml
from utils import RedisSentinelCache, ReferenceDataLoader, get_tz_aware_now_without_micros

"""
Script called by cronjob to update reference data from elasticsearch to local cache
"""


def update_reference_data():
    with open('/home/metax-user/app_config') as app_config:
        app_config_dict = yaml.load(app_config)

    redis_settings = app_config_dict.get('REDIS', None)
    es_settings = app_config_dict.get('ELASTICSEARCH', None)

    cache = RedisSentinelCache(master_only=True, settings=redis_settings)
    ReferenceDataLoader.populate_cache_reference_data(cache, settings=es_settings)


def log(msg):
    print('%s %s' % (str(get_tz_aware_now_without_micros()), msg))


if __name__ == '__main__':
    log('Updating reference data...')
    try:
        update_reference_data()
    except Exception as e:
        log('Reference data update ended in an error: %s' % str(e))
    else:
        log('Reference data updated')
