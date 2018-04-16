import os
import sys
from uuid import uuid4

from dateutil import parser
from django.utils import timezone


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


def generate_identifier(urn=True):
    if urn:
        return 'urn:nbn:fi:att:%s' % str(uuid4())
    return str(uuid4())
