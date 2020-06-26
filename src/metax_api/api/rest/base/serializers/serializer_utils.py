# This file is part of the Metax API service
#
# Copyright 2017-2018 Ministry of Education and Culture, Finland
#
# :author: CSC - IT Center for Science Ltd., Espoo Finland <servicedesk@csc.fi>
# :license: MIT

import re

from jsonschema import validate as json_validate, FormatChecker
from jsonschema.compat import str_types
from jsonschema.exceptions import ValidationError as JsonValidationError
from rest_framework.serializers import ValidationError

date_re = re.compile(
    r'^\d{4}-\d{2}-\d{2}$'
)

datetime_re = re.compile(
    r'^\d{4}-\d{2}-\d{2}T'
    r'\d{2}:\d{2}:\d{2}(\.\d{1,9})?'
    r'(Z|[+-]\d{2}:\d{2})$'
)

def validate_json(value, schema):
    """
    Since RFC3339 dependency was removed, date and datetime formats have to be validated
    by hand. Helper methods below does the trick.
    """
    try:
        json_validate(value, schema, format_checker=FormatChecker())
    except JsonValidationError as e:
        # use parent class for output messages if possible, because jsonschema 3.2.0 introduced errors in more depth
        # which caused the error messages to be too spesific
        if e.parent:
            message = e.parent.message
            path = e.parent.path
            schema = e.parent.schema
        else:
            message = e.message
            path = e.path
            schema = e.schema

        raise ValidationError('%s. Json path: %s. Schema: %s' % (message, [p for p in path], schema))

# helper methods for date and datetime validation
@FormatChecker.checks(FormatChecker, format='date')
def date(value):
    if isinstance(value, str_types):
        match = date_re.match(value)
        if match:
            return True

    return False

@FormatChecker.checks(FormatChecker, format='date-time')
def date_time(value):
    if isinstance(value, str_types):
        match = datetime_re.match(value)
        if match:
            return True

    return False
