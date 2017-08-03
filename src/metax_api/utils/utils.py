import sys

def executing_test_case():
    """
    Returns True whenever code is being executed by automatic test cases
    """
    return 'test' in sys.argv
