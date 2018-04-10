from jsonschema import validate as json_validate
from jsonschema.exceptions import ValidationError as JsonValidationError
from rest_framework.serializers import ValidationError

def validate_json(value, schema):
    try:
        json_validate(value, schema)
    except JsonValidationError as e:
        raise ValidationError('%s. Json path: %s. Schema: %s' % (e.message, [p for p in e.path], e.schema))