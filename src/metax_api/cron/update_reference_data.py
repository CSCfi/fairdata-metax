from os import sys, path
sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))

from datetime import datetime
import yaml
from utils import RedisSentinelCache, ReferenceDataLoader

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

def get_time():
    return str(datetime.now().replace(microsecond=0))

def log(msg):
    print('%s %s' % (get_time(), msg))

if __name__ == '__main__':
    log('Updating reference data...')
    try:
        update_reference_data()
    except Exception as e:
        log('Reference data update ended in an error: %s' % str(e))
    else:
        log('Reference data updated')
