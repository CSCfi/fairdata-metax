import email.utils as email_utils
import os
import sys

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


def parse_http_timestamp_using_tz_and_get_tz_naive_datetime(timestamp):
    """
    Parse a timestamp string used in http headers to a datetime object. Timestamp such as:

    'Wed, 23 Sep 2009 22:15:29 GMT'

    email.utils is used for parsing, since it can deal with locale-dependent
    verbose week and month names.

    https://docs.python.org/3/library/email.util.html#email.utils.parsedate_to_datetime
    returns a timezone-aware datetime.

    https://github.com/django/django/blob/54e5c4a00e116ff4be257accdc9aa9e068c7f4ee/django/utils/timezone.py#L279
    makes the timezone-aware datetime timezone-naive, which is how Django represents
    by default datetimes. This is returned from this method.
    """
    return timezone.make_naive(email_utils.parsedate_to_datetime(timestamp))


def get_tz_aware_now_without_micros():
    return timezone.now().replace(microsecond=0)
