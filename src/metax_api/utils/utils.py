# This file is part of the Metax API service
#
# Copyright 2017-2018 Ministry of Education and Culture, Finland
#
# :author: CSC - IT Center for Science Ltd., Espoo Finland <servicedesk@csc.fi>
# :license: MIT

import sys
from datetime import datetime
from enum import Enum
from uuid import uuid4

import structlog
from dateutil import parser
from django.conf import settings
from django.utils import timezone


class IdentifierType(Enum):
    URN = 'urn'
    DOI = 'doi'


class DelayedLog():

    """
    A callable that can be passed to CallableService as a post_request_callable,
    when needing to log something only when the request has ended with a success code,
    and transactions to db have been successful.
    """

    def __init__(self, *args, **kwargs):
        self._log_args = kwargs

    def __call__(self, *args, **kwargs):
        json_logger.info(**self._log_args)


def executing_test_case():
    """
    Returns True whenever code is being executed by automatic test cases
    """
    return 'test' in sys.argv


def datetime_to_str(date_obj):
    if isinstance(date_obj, datetime):
        return date_obj.strftime('%Y-%m-%dT%H:%M:%SZ')
    elif datetime is None:
        return None
    else:
        assert isinstance(date_obj, datetime), 'date_obj must be datetime object or None'


def parse_timestamp_string_to_tz_aware_datetime(timestamp_str):
    """
    Parse a timestamp_str string to a datetime object. Timestamp such as:

    'Wed, 23 Sep 2009 22:15:29 GMT'

    https://dateutil.readthedocs.io/en/stable/parser.html#dateutil.parser.parse
    returns a timezone-aware datetime if the timestamp contains timezone information.

    https://github.com/django/django/blob/54e5c4a00e116ff4be257accdc9aa9e068c7f4ee/django/utils/timezone.py#L263
    Makes the timezone-naive datetime timezone-aware, which is how Django represents datetimes when USE_TZ = True.

    Returns time-zone aware timestamp. Caller is responsible for catching errors in parsing.
    """
    if not isinstance(timestamp_str, str):
        raise ValueError("Timestamp must be a string")

    timestamp_str = parser.parse(timestamp_str)
    if timezone.is_naive(timestamp_str):
        timestamp_str = timezone.make_aware(timestamp_str)

    return timestamp_str


def get_tz_aware_now_without_micros():
    return timezone.now().replace(microsecond=0)


def generate_uuid_identifier(urn_prefix=False):
    if urn_prefix:
        return 'urn:nbn:fi:att:%s' % str(uuid4())
    return str(uuid4())


def is_metax_generated_doi_identifier(identifier):
    """
    Check whether given identifier is a metax generated doi identifier

    :param identifier:
    :return: boolean
    """
    if not identifier or not hasattr(settings, 'DATACITE') or not settings.DATACITE.get('PREFIX', False):
        return False

    return identifier.startswith('doi:{0}/'.format(settings.DATACITE.get('PREFIX')))

def is_remote_doi_identifier(identifier):
    """
    Check whether given identifier is a remote doi identifier

    :param identifier:
    :return: boolean
    """
    if not identifier or not settings.DATACITE.get('PREFIX', False):
        return False

    if identifier.startswith('doi:') and not identifier.startswith('doi:{0}/'.format(settings.DATACITE.get('PREFIX'))):
        return True

def is_metax_generated_urn_identifier(identifier):
    """
    Check whether given identifier is a metax generated urn identifier

    :param identifier:
    :return: boolean
    """
    if not identifier:
        return False

    return identifier.startswith('urn:nbn:fi:att:') or identifier.startswith('urn:nbn:fi:csc')


def generate_doi_identifier(doi_suffix=None):
    """
    Until a better mechanism for generating DOI suffix is conceived, use UUIDs.

    :param doi_suffix:
    :return: DOI identifier suitable for storing to Metax: doi:10.<doi_prefix>/<doi_suffix>
    """
    if doi_suffix is None:
        doi_suffix = generate_uuid_identifier()

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
    if identifier:
        if identifier.startswith('doi:'):
            return IdentifierType.DOI
        elif identifier.startswith('urn:'):
            return IdentifierType.URN
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
        obj = [remove_keys_recursively(item, fields_to_remove) for item in obj if item not in fields_to_remove]

    return obj


def leave_keys_in_dict(dict_obj, fields_to_leave):
    """
    Removes the key-values from dict_obj, for which key is NOT listed in fields_to_leave.
    NOTE: Is not recursive

    :param dict_obj:
    :param fields_to_leave:
    :return:
    """
    for key in list(dict_obj):
        if key not in fields_to_leave:
            del dict_obj[key]


if executing_test_case():
    class TestJsonLogger():

        def info(self, *args, **kwargs):
            pass

        def error(self, *args, **kwargs):
            pass

        def warning(self, *args, **kwargs):
            pass

        def debug(self, *args, **kwargs):
            pass

    json_logger = TestJsonLogger()
else:
    json_logger = structlog.get_logger('structlog')
