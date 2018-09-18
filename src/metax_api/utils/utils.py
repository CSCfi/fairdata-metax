# This file is part of the Metax API service
#
# Copyright 2017-2018 Ministry of Education and Culture, Finland
#
# :author: CSC - IT Center for Science Ltd., Espoo Finland <servicedesk@csc.fi>
# :license: MIT

import os
import sys
import datetime
from enum import Enum
from uuid import uuid4

from dateutil import parser
from django.conf import settings
from django.utils import timezone


class IdentifierType(Enum):
    URN = 'urn'
    DOI = 'doi'


def executing_test_case():
    """
    Returns True whenever code is being executed by automatic test cases
    """
    return 'test' in sys.argv


def executing_travis():
    """
    Returns True whenever code is being executed by travis
    """
    return True if os.getenv('TRAVIS', False) else False


def parse_timestamp_string_to_tz_aware_datetime(timestamp):
    """
    Parse a timestamp string to a datetime object. Timestamp such as:

    'Wed, 23 Sep 2009 22:15:29 GMT'

    https://dateutil.readthedocs.io/en/stable/parser.html#dateutil.parser.parse
    returns a timezone-aware datetime if the timestamp contains timezone information.

    https://github.com/django/django/blob/54e5c4a00e116ff4be257accdc9aa9e068c7f4ee/django/utils/timezone.py#L263
    Makes the timezone-naive datetime timezone-aware, which is how Django represents datetimes when USE_TZ = True.

    Returns time-zone aware timestamp. Caller is responsible for catching errors in parsing.
    """
    if not isinstance(timestamp, str):
        raise ValueError("Timestamp must be a string")

    timestamp = parser.parse(timestamp)
    if timezone.is_naive(timestamp):
        timestamp = timezone.make_aware(timestamp)

    return timestamp


def get_tz_aware_now_without_micros():
    return timezone.now().replace(microsecond=0)


def now_is_later_than_datetime_str(datetime_str):
    try:
        datetime_obj = parser.parse(datetime_str)
    except Exception:
        datetime_obj = None

    if type(datetime_obj) != datetime.datetime:
        raise Exception("Unable to parse datetime string: {0}".format(datetime_str))

    if timezone.is_naive(datetime_obj):
        datetime_obj = timezone.make_aware(datetime_obj)

    return datetime.datetime.now(tz=timezone.get_current_timezone()) >= datetime_obj


def generate_uuid_identifier(urn_prefix=False):
    if urn_prefix:
        return 'urn:nbn:fi:att:%s' % str(uuid4())
    return str(uuid4())


def generate_doi_identifier(doi_suffix=generate_uuid_identifier()):
    """
    Until a better mechanism for generating DOI suffix is conceived, use UUIDs.

    :param doi_suffix:
    :return: DOI identifier suitable for storing to Metax: doi:10.<doi_prefix>/<doi_suffix>
    """

    doi_prefix = None
    if hasattr(settings, 'DATACITE'):
        doi_prefix = settings.DATACITE.get('PREFIX', None)
    if not doi_prefix:
        raise Exception("PREFIX must be defined in settings DATACITE dictionary")
    if not doi_suffix:
        raise ValueError("DOI suffix must be provided in order to create a DOI identifier")
    return 'doi:{0}/{1}'.format(doi_prefix, doi_suffix)


def extract_doi_from_doi_identifier(doi_identifier):
    """
    DOI identifier is stored to database in the form 'doi:10.<doi_prefix>/<doi_suffix>'.
    This method strips away the 'doi':, which does not belong to the actual DOI in e.g. Datacite API.

    :param doi_identifier: Must start with doi:10. for this method to work properly
    :return: If the doi_identifier does not start with doi:10., return None. Otherwise return doi starting from 10.
    """
    if doi_identifier and doi_identifier.startswith('doi:10.'):
        return doi_identifier[doi_identifier.index('10.'):]
    return None


def get_identifier_type(identifier):
    if identifier.startswith('doi:'):
        return IdentifierType.DOI
    elif identifier.startswith('urn:'):
        return IdentifierType.URN
    else:
        return None


def remove_keys_recursively(obj, fields_to_remove):
    """
     Accepts as parameter either a single dict object or a list of dict objects.

    :param obj:
    :param fields_to_remove:
    :return:
    """
    if isinstance(obj, dict):
        obj = {
            key: remove_keys_recursively(value, fields_to_remove) for key, value in obj.items()
            if key not in fields_to_remove
        }
    elif isinstance(obj, list):
        obj = [
            remove_keys_recursively(item, fields_to_remove) for item in obj
            if item not in fields_to_remove
        ]
    return obj


def leave_keys_in_dict(dict_obj, fields_to_leave):
    """
    Returns a dict object having only the key-values, for which key is listed in fields_to_leave.
    NOTE: Is not recursive

    :param dict_obj:
    :param fields_to_leave:
    :return:
    """
    return {key: dict_obj[key] for key in fields_to_leave if key in fields_to_leave}