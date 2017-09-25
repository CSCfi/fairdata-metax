from datetime import datetime
import email.utils as email_utils
import os
import sys

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

def parse_http_timestamp(timestamp):
    """
    Parse a timestamp string used in http headers to a datetime object, such as:

    'Wed, 23 Sep 2009 22:15:29 GMT'

    email.utils is used for parsing, since it can deal with locale-dependent
    verbose week and month names.
    """
    return datetime(*email_utils.parsedate(timestamp)[:6])
