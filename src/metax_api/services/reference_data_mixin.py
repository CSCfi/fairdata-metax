# This file is part of the Metax API service
#
# Copyright 2017-2018 Ministry of Education and Culture, Finland
#
# :author: CSC - IT Center for Science Ltd., Espoo Finland <servicedesk@csc.fi>
# :license: MIT

import logging
from time import sleep

from django.conf import settings as django_settings

from metax_api.exceptions import Http503
from metax_api.utils import ReferenceDataLoader

_logger = logging.getLogger(__name__)
d = _logger.debug


class ReferenceDataMixin():

    """
    Helper methods that other service classes can use when dealing with reference data
    """

    REF_DATA_RELOAD_MAX_RETRIES = 4

    process_cached_reference_data = None

    @staticmethod
    def check_ref_data(ref_data_type, field_to_check, relation_name, errors={}, value_not_found_is_error=True):
        """
        Check if the given field exists in the reference data. The value of the
        field can be either the actual uri, or a shorthand code.

        If the value is found, the ref data entry is returned, so it may later be
        used to populate the received dataset.

        If the value is not found and value_not_found_is_error is True, an error is appended to the 'errors' dict.

        params:
        ref_data_type:  the ES datatype to search from
        field_to_check: the field value being checked
        relation_name:  the full relation path to the field to hand out in case of errors
        """
        try:
            return next(entry for entry in ref_data_type if field_to_check in (entry['uri'], entry['code']))
        except StopIteration:
            if value_not_found_is_error:
                errors[relation_name].append('Identifier \'%s\' not found in reference data' % field_to_check)
        return None

    @classmethod
    def get_reference_data(cls, cache):
        """
        Return reference data from cache, and attempt to reload it from ES if reference_data key
        is missing. If reference data reload is in progress by another request, retry for five
        seconds, and give up.

        Once reference data has been loaded once, it is stored in the process itself, so it does not
        need to be reloaded from the distributed cache again. Reference data does not change actively
        during normal operation, so saving it in the process should be safe.
        """
        if cls.process_cached_reference_data is not None:
            return cls.process_cached_reference_data

        ref_data = cache.get('reference_data')

        if ref_data:
            cls.process_cached_reference_data = ref_data
            return cls.process_cached_reference_data
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
                cls.process_cached_reference_data = ref_data
                return cls.process_cached_reference_data
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

    def populate_from_ref_data(ref_entry, obj, uri_field='identifier', label_field=None, add_in_scheme=True):
        """
        Always populate at least uri field (even if it was alread there, no big deal).
        Label field population is necessary for some ref data types only.
        """
        obj[uri_field] = ref_entry['uri']
        if label_field and 'label' in ref_entry:
            obj[label_field] = ref_entry['label']
        if add_in_scheme and 'scheme' in ref_entry:
            obj['in_scheme'] = ref_entry['scheme']

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

    @classmethod
    def process_org_obj_against_ref_data(cls, orgdata, org_obj, org_obj_relation_name, refdata=None, errors={}):
        """
        First check if org object contains is_part_of relation, in which case recursively call this method
        until there is no is_part_of relation. After this, check whether org object has a value in identifier field.

        If there is a value in identifier field, check whether it can be found from the organization reference data.
        If the value is found, populate the name field of org obj with the ref data label and identifier field with
        the ref data uri. If the value is not found from reference data, never mind.

        Populate possible contributor_type from ref data.

        Auto-populate reference data sub org's parent org (two level org hierarchy assumed!), if org object's identifier
        is found to be a sub org and user has not given a parent org (via is_part_of relation).
        """
        if not orgdata or not org_obj:
            return

        if org_obj.get('is_part_of', False):
            nested_obj = org_obj.get('is_part_of')
            cls.process_org_obj_against_ref_data(orgdata, nested_obj,
                                                 org_obj_relation_name + '.is_part_of', refdata=refdata, errors=errors)

        if org_obj.get('identifier', False):
            ref_entry = cls.check_ref_data(orgdata, org_obj['identifier'],
                                           org_obj_relation_name + '.identifier', value_not_found_is_error=False)
            if ref_entry:
                cls.populate_from_ref_data(ref_entry, org_obj, 'identifier', 'name', add_in_scheme=False)

                if ref_entry.get('parent_org_code', False) and 'is_part_of' not in org_obj:
                    parent_ref_entry = cls.check_ref_data(orgdata, ref_entry['parent_org_code'],
                                                          org_obj_relation_name + '.is_part_of.identifier',
                                                          value_not_found_is_error=False)
                    if parent_ref_entry:
                        parent_org_obj = {}

                        if 'DataCatalog' not in cls.__name__:
                            # Datacatalogs does not allow @type field to be populated for organizations
                            parent_org_obj['@type'] = 'Organization'

                        cls.populate_from_ref_data(parent_ref_entry, parent_org_obj,
                                                   'identifier', 'name', add_in_scheme=False)
                        org_obj['is_part_of'] = parent_org_obj

        if refdata and 'contributor_type' in refdata:
            for contributor_type in org_obj.get('contributor_type', []):
                ref_entry = cls.check_ref_data(refdata['contributor_type'], contributor_type['identifier'],
                                               org_obj_relation_name + '.contributor_type.identifier', errors)
                if ref_entry:
                    cls.populate_from_ref_data(ref_entry, contributor_type, label_field='pref_label')

    @classmethod
    def process_research_agent_obj_with_type(cls, orgdata, refdata, errors, agent_obj, agent_obj_relation_name):
        if agent_obj.get('@type') == 'Person':
            member_of = agent_obj.get('member_of', None)
            if member_of:
                cls.process_org_obj_against_ref_data(orgdata, member_of, agent_obj_relation_name + '.member_of',
                                                     refdata=refdata, errors=errors)

            for contributor_role in agent_obj.get('contributor_role', []):
                ref_entry = cls.check_ref_data(refdata['contributor_role'], contributor_role['identifier'],
                                               agent_obj_relation_name + '.contributor_role.identifier', errors=errors)
                if ref_entry:
                    cls.populate_from_ref_data(ref_entry, contributor_role, label_field='pref_label')

            for contributor_type in agent_obj.get('contributor_type', []):
                ref_entry = cls.check_ref_data(refdata['contributor_type'], contributor_type['identifier'],
                                               agent_obj_relation_name + '.contributor_type.identifier', errors=errors)
                if ref_entry:
                    cls.populate_from_ref_data(ref_entry, contributor_type, label_field='pref_label')

        elif agent_obj.get('@type') == 'Organization':
            cls.process_org_obj_against_ref_data(orgdata, agent_obj, agent_obj_relation_name, refdata=refdata,
                                                 errors=errors)

    @classmethod
    def remove_language_obj_irrelevant_titles(cls, lang_obj, title_label_field):
        title_obj = lang_obj.get(title_label_field, None)
        if title_obj:
            to_delete = set(title_obj.keys()).difference(['fi', 'sv', 'en', 'und'])
            for d in to_delete:
                del title_obj[d]
