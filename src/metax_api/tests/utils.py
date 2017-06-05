from os import path
from json import load as json_load

datetime_format = '%Y-%m-%dT%H:%M:%S.%fZ'

def get_json_schema(model_name):
    with open(path.dirname(path.realpath(__file__)) + '/../api/base/schemas/json_schema_%s.json' % model_name) as f:
        return json_load(f)
