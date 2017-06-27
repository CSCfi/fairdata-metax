from jsonschema import validate as json_validate
from jsonschema.exceptions import ValidationError as JsonValidationError
from rest_framework.serializers import ValidationError

def validate_json(value, schema):
    try:
        json_validate(value, schema)
    except JsonValidationError as e:
        if 'required property' in e.message:
            # raise only error about the field missing altogether
            raise ValidationError(e.message)
        else:
            # field is present but has errors: raise more specific error what is wrong with the field
            raise ValidationError('%s. Json field: %s, schema: %s' % (e.message, e.path[0], e.schema))
