# This file is part of the Metax API service
#
# Copyright 2017-2018 Ministry of Education and Culture, Finland
#
# :author: CSC - IT Center for Science Ltd., Espoo Finland <servicedesk@csc.fi>
# :license: MIT

from jsonschema import validate as json_validate, FormatChecker
from jsonschema.exceptions import ValidationError as JsonValidationError
from rest_framework.serializers import ValidationError


def validate_json(value, schema):
    try:
        json_validate(value, schema, format_checker=FormatChecker())
    except JsonValidationError as e:
        raise ValidationError('%s. Json path: %s. Schema: %s' % (e.message, [p for p in e.path], e.schema))
