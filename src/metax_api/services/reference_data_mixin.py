from time import sleep

from django.conf import settings as django_settings

from metax_api.exceptions import Http503
from metax_api.utils import ReferenceDataLoader

import logging
_logger = logging.getLogger(__name__)
d = _logger.debug


class ReferenceDataMixin():

    """
    Helper methods that other service classes can use when dealing with reference data
    """

    REF_DATA_RELOAD_MAX_RETRIES = 4

    def check_ref_data(ref_data_type, field_to_check, relation_name, errors):
        """
        Check if the given field exists in the reference data. The value of the
        field can be either the actual uri, or a shorthand code.

        If the value is found, the ref data entry is returned, so it may later be
        used to populate the received dataset.

        If the value is not found, an error is appended to the 'errors' dict.

        params:
        ref_data_type:  the ES datatype to search from
        field_to_check: the field value being checked
        relation_name:  the full relation path to the field to hand out in case of errors
        """
        try:
            return next(entry for entry in ref_data_type if field_to_check in (entry['uri'], entry['code']))
        except StopIteration:
            errors[relation_name].append('Identifier \'%s\' not found in reference data' % field_to_check)
        return None

    @classmethod
    def get_reference_data(cls, cache):
        """
        Return reference data from cache, and attempt to reload it from ES if reference_data key
        is missing. If reference data reload is in progress by another request, retry for five
        seconds, and give up.
        """
        ref_data = cache.get('reference_data')

        if ref_data:
            return ref_data
        else:
            _logger.info('reference_data missing from cache - attempting to reload')

        try:
            state = ReferenceDataLoader.populate_cache_reference_data(cache)
        except Exception as e:
            cls._raise_reference_data_reload_error(e)

        for retry in range(0, cls.REF_DATA_RELOAD_MAX_RETRIES + 1):

            # when ref data was just populated, always retrieve from master to ensure
            # data is found, since there is a delay in data flow to slaves
            ref_data = cache.get('reference_data', master=True)

            if ref_data:
                return ref_data
            elif state == 'reload_started_by_other' and retry < cls.REF_DATA_RELOAD_MAX_RETRIES:
                sleep(1)
            elif state == 'reload_started_by_other' and retry >= cls.REF_DATA_RELOAD_MAX_RETRIES:
                cls._raise_reference_data_reload_error(
                    'Reload in progress by another request. Retried max times, and gave up'
                )
            else:
                cls._raise_reference_data_reload_error(
                    'Current request tried to reload reference data, but apparently failed,'
                    ' since key reference_data is still missing'
                )

    def populate_from_ref_data(ref_entry, obj, uri_field='identifier', label_field=None):
        """
        Always populate at least uri field (even if it was alread there, no big deal).
        Label field population is necessary for some ref data types only.
        """
        obj[uri_field] = ref_entry['uri']
        if label_field and 'label' in ref_entry:
            obj[label_field] = ref_entry['label']

    def _raise_reference_data_reload_error(error):
        """
        Log exception about reference data load failure, and return a user friendly error message
        with HTTP 503.
        """
        _logger.exception(
            'Failed to reload reference_data from ES - raising 503 temporarily unavailable.'
            ' Details:\n%s' % error
        )
        error_msg = 'Reference data temporarily unavailable. Please try again later'
        if django_settings.DEBUG:
            error_msg += ' DEBUG: %s' % str(error)
        raise Http503(error_msg)
