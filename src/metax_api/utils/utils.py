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
