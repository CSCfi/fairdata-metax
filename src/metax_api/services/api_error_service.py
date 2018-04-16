from json import dump as json_dump, load as json_load
from os import listdir, remove as remove_file
from uuid import uuid4
import logging
import traceback

from django.conf import settings

from metax_api.utils import get_tz_aware_now_without_micros

_logger = logging.getLogger(__name__)


class ApiErrorService():

    @staticmethod
    def flush_errors():
        """
        Delete all error files.
        """
        error_files = listdir(settings.ERROR_FILES_PATH)
        file_count = len(error_files)
        for ef in error_files:
            remove_file('%s/%s' % (settings.ERROR_FILES_PATH, ef))
        return file_count

    @staticmethod
    def remove_error_file(error_identifier):
        """
        Delete a single error file.
        """
        remove_file('%s/%s.json' % (settings.ERROR_FILES_PATH, error_identifier))

    @staticmethod
    def retrieve_error_details(error_identifier):
        """
        Retrieve complete data about a single error
        """
        with open('%s/%s.json' % (settings.ERROR_FILES_PATH, error_identifier), 'r') as f:
            return json_load(f)

    @staticmethod
    def retrieve_error_list():
        """
        List all error files in the designated error location. Data is cleaned up a bit
        for easier browsing.
        """
        error_files = listdir(settings.ERROR_FILES_PATH)
        error_list = []

        for ef in error_files:
            with open('%s/%s' % (settings.ERROR_FILES_PATH, ef), 'r') as f:
                error_details = json_load(f)
                error_details.pop('data', None)
                error_details.pop('headers', None)
                if len(str(error_details['response'])) > 200:
                    error_details['response'] = '%s ...(first 200 characters)' % str(error_details['response'])[:200]
                error_details['traceback'] = '(last 200 characters) ...%s' % error_details['traceback'][-200:]
                error_list.append(error_details)
        return error_list

    @staticmethod
    def store_error_details(request, response, exception=None, other={}):
        """
        Store error and request details to disk to specified error file location.
        """
        current_time = str(get_tz_aware_now_without_micros()).replace(' ', 'T')

        if request.method in ('POST', 'PUT', 'PATCH'):
            # cast possible datetime objects to strings, because those cant be json-serialized...
            request_data = request.data
            for k in ('date_modified', 'date_created'):
                if isinstance(request_data, list):
                    for item in request_data:
                        if k in item:
                            item[k] = str(k)
                elif isinstance(request_data, dict) and k in request_data:
                    request_data[k] = str(request_data[k])
                else:
                    pass
        else:
            request_data = None

        error_info = {
            'method':      request.method,
            'user':        request.user.username or 'guest',
            'data':        request_data,
            'headers':     {
                k: v for k, v in request.META.items()
                if k.startswith('HTTP_') and k != 'HTTP_AUTHORIZATION'
            },
            'status_code': response.status_code,
            'response':    response.data,
            'traceback':   traceback.format_exc(),
            # during test case execution, RAW_URI is not set
            'url':         request.META.get('RAW_URI', request.META.get('PATH_INFO', '???')),
            'identifier':  '%s-%s' % (current_time[:19], str(uuid4())[:8]),
            'exception_time': current_time,
        }

        if other:
            # may contain info that the request was a bulk operation
            error_info['other'] = { k: v for k, v in other.items() }
            if 'bulk_request' in other:
                error_info['other']['data_row_count'] = len(request_data)

        try:
            with open('%s/%s.json' % (settings.ERROR_FILES_PATH, error_info['identifier']), 'w') as f:
                json_dump(error_info, f)
        except:
            _logger.exception('Failed to save error info...')
        else:
            response.data['error_identifier'] = error_info['identifier']
