# This file is part of the Metax API service
#
# Copyright 2017-2018 Ministry of Education and Culture, Finland
#
# :author: CSC - IT Center for Science Ltd., Espoo Finland <servicedesk@csc.fi>
# :license: MIT
import logging
import urllib.parse
from collections import defaultdict
from os.path import dirname, join

import simplexquery as sxq
import xmltodict
from django.db.models import Q
from rest_framework.serializers import ValidationError

from metax_api.exceptions import Http400, Http403, Http503
from metax_api.models import CatalogRecord, Directory, File
from metax_api.models.catalog_record import ACCESS_TYPES
from metax_api.utils import \
    parse_timestamp_string_to_tz_aware_datetime, \
    get_tz_aware_now_without_micros, \
    remove_keys_recursively, \
    leave_keys_in_dict
from .common_service import CommonService
from .datacite_service import DataciteService
from .file_service import FileService
from .reference_data_mixin import ReferenceDataMixin

_logger = logging.getLogger(__name__)


class CatalogRecordService(CommonService, ReferenceDataMixin):

    @classmethod
    def get_queryset_search_params(cls, request):
        """
        Get and validate parameters from request.query_params that will be used for filtering
        in view.get_queryset()
        """
        queryset_search_params = {}

        if not request.query_params:
            return cls.filter_by_state(request, queryset_search_params)

        queryset_search_params = cls.filter_by_state(request, queryset_search_params)

        if request.query_params.get('state', False):
            state_vals = request.query_params['state'].split(',')
            for val in state_vals:
                try:
                    int(val)
                except ValueError:
                    raise Http400({ 'state': ['Value \'%s\' is not an integer' % val] })
            queryset_search_params['preservation_state__in'] = state_vals

        if request.query_params.get('preservation_state', False):
            state_vals = request.query_params['preservation_state'].split(',')
            for val in state_vals:
                try:
                    int(val)
                except ValueError:
                    raise Http400({ 'preservation_state': ['Value \'%s\' is not an integer' % val] })
            queryset_search_params['preservation_state__in'] = state_vals

        if CommonService.get_boolean_query_param(request, 'latest'):
            queryset_search_params['next_dataset_version_id'] = None

        if request.query_params.get('deprecated', None) is not None:
            queryset_search_params['deprecated'] = CommonService.get_boolean_query_param(request, 'deprecated')

        if request.query_params.get('curator', False):
            queryset_search_params['research_dataset__contains'] = \
                {'curator': [{ 'identifier': request.query_params['curator']}]}

        if request.query_params.get('owner_id', False):
            queryset_search_params['editor__contains'] = { 'owner_id': request.query_params['owner_id'] }

        if request.query_params.get('user_created', False):
            queryset_search_params['user_created'] = request.query_params['user_created']

        if request.query_params.get('editor', False):
            queryset_search_params['editor__contains'] = { 'identifier': request.query_params['editor'] }

        if request.query_params.get('metadata_provider_user', False):
            queryset_search_params['metadata_provider_user'] = request.query_params['metadata_provider_user']

        if request.query_params.get('metadata_owner_org', False):
            queryset_search_params['metadata_owner_org__in'] = request.query_params['metadata_owner_org'].split(',')

        if request.query_params.get('contract_org_identifier', False):
            if request.user.username not in ('metax', 'tpas'):
                raise Http403({ 'detail': ['query parameter pas_filter is restricted']})
            queryset_search_params['contract__contract_json__organization__organization_identifier__iregex'] = \
                request.query_params['contract_org_identifier']

        if request.query_params.get('pas_filter', False):
            cls.set_pas_filter(queryset_search_params, request)

        if CommonService.has_research_agent_query_params(request):
            cls.set_actor_filters(queryset_search_params, request)

        if request.query_params.get('data_catalog', False):
            queryset_search_params['data_catalog__catalog_json__identifier__iregex'] = \
                request.query_params['data_catalog']

        if request.query_params.get('api_version', False):
            try:
                value = int(request.query_params['api_version'])
            except ValueError:
                value = request.query_params['api_version']
                raise Http400({ 'api_version': ['Value \'%s\' is not an integer' % value] })

            queryset_search_params['api_meta__contains'] = { 'version': value }

        return queryset_search_params

    @staticmethod
    def filter_by_state(request, queryset_search_params):
        '''
        Helper method to filter returning data by state: unauthenticated
        users get only published data, and end users get only published &
        their own drafts
        '''
        state_filter = None

        if request.user.username is None: # unauthenticated user
            state_filter = Q(state='published')
        elif request.user.is_service:  # service account
            pass
        else: # enduser api
            state_filter = Q(state='published') | Q(state='draft', metadata_provider_user=request.user.username)

        if state_filter:
            if 'q_filters' in queryset_search_params:
                queryset_search_params['q_filters'].append(state_filter)
            else:
                queryset_search_params['q_filters'] = [state_filter]

        return queryset_search_params

    @staticmethod
    def set_actor_filters(queryset_search_params, request):
        """
        Set complex queries for filtering datasets by creator, curator, publisher and/or rights_holder.
        'condition_separator' -parameter defines if these are OR'ed or AND'ed (Default=AND) together.
        Organization filters also search matches from person's "member_of" field.
        Q-filters from multiple queries are AND'ed together eventually.
        """
        def _get_person_filter(agent, person):
            name_filter = Q()
            # only one publisher possible
            if agent == 'publisher':
                name_filter |= Q(**{ f'research_dataset__{agent}__name__iregex':  person })
            else:
                # having same problem as in set_pas_filter below..
                for i in range(3):
                    name_filter |= Q(**{ f'research_dataset__{agent}__{i}__name__iregex':  person })

                name_filter |= Q(**{ f'research_dataset__{agent}__contains': [{ 'name': person }] })

            # regex will find matches from organization name fields so have to disable it
            person_filter = Q(**{ f'research_dataset__{agent}__contains': [{ '@type': "Person" }] })
            name_filter.add(person_filter, 'AND')

            return name_filter

        def _get_org_filter(agent, org):
            name_filter = Q()
            # only one publisher possible
            if agent == 'publisher':
                name_filter |= (Q(**{ f'research_dataset__{agent}__name__en__iregex':  org }))
                name_filter |= (Q(**{ f'research_dataset__{agent}__name__fi__iregex':  org }))
                name_filter |= (Q(**{ f'research_dataset__{agent}__member_of__name__en__iregex':  org }))
                name_filter |= (Q(**{ f'research_dataset__{agent}__member_of__name__fi__iregex':  org }))
            else:
                for i in range(3):
                    name_filter |= (Q(**{ f'research_dataset__{agent}__{i}__name__en__iregex':  org }))
                    name_filter |= (Q(**{ f'research_dataset__{agent}__{i}__name__fi__iregex':  org }))
                    name_filter |= (Q(**{ f'research_dataset__{agent}__{i}__member_of__name__en__iregex':  org }))
                    name_filter |= (Q(**{ f'research_dataset__{agent}__{i}__member_of__name__fi__iregex':  org }))

                name_filter |= (Q(**{ f'research_dataset__{agent}__contains': [{ 'name': {'en': org} }] }))
                name_filter |= (Q(**{ f'research_dataset__{agent}__contains': [{ 'name': {'fi': org} }] }))
                name_filter |= (
                    Q(**{ f'research_dataset__{agent}__contains': [{ 'member_of': {'name': {'en': org}} }] })
                )
                name_filter |= (
                    Q(**{ f'research_dataset__{agent}__contains': [{ 'member_of': {'name': {'fi': org}} }] })
                )

            return name_filter

        q_filter = Q()
        separator = 'OR' if request.query_params.get('condition_separator', '').upper() == 'OR' else 'AND'

        for agent in ['creator', 'curator', 'publisher', 'rights_holder']:
            if request.query_params.get(f'{agent}_person'):
                person = urllib.parse.unquote(request.query_params[f'{agent}_person'])
                q_filter.add(_get_person_filter(agent, person), separator)

            if request.query_params.get(f'{agent}_organization'):
                org = urllib.parse.unquote(request.query_params[f'{agent}_organization'])
                q_filter.add(_get_org_filter(agent, org), separator)

        if 'q_filters' in queryset_search_params: # pragma: no cover
            queryset_search_params['q_filters'].append(q_filter)
        else:
            queryset_search_params['q_filters'] = [q_filter]

    @staticmethod
    def set_pas_filter(queryset_search_params, request):
        """
        A somewhat specific filter for PAS needs... The below OR query is AND'ed with any
        other possible filters from other query parameters.
        """
        if request.user.username not in ('metax', 'tpas'):
            raise Http403({ 'detail': ['query parameter pas_filter is restricted']})

        search_string = urllib.parse.unquote(request.query_params.get('pas_filter', ''))

        # dataset title, from various languages...
        q1 = Q(research_dataset__title__en__iregex=search_string)
        q2 = Q(research_dataset__title__fi__iregex=search_string)

        # contract...
        q3 = Q(contract__contract_json__title__iregex=search_string)

        # a limitation of jsonb-field queries...
        # unable to use regex search directly (wildcard) since curator is an array... and __contains only
        # matches whole string. cheating here to search from first three indexes using regex. who
        # knows how many curators datasets will actually have, but probably most cases will produce
        # a match with this approach... if not, the user will be more and more accurate and finally
        # type the whole name and get a result while cursing shitty software
        q4 = Q(research_dataset__curator__0__name__iregex=search_string)
        q5 = Q(research_dataset__curator__1__name__iregex=search_string)
        q6 = Q(research_dataset__curator__2__name__iregex=search_string)
        q7 = Q(research_dataset__curator__contains=[{ 'name': search_string }])

        q_filter = q1 | q2 | q3 | q4 | q5 | q6 | q7

        if 'q_filters' in queryset_search_params: # pragma: no cover
            # no usecase yet but leaving comment for future reference... if the need arises to
            # include Q-filters from multiple sources (query params), probably AND them together
            # by appending to list
            queryset_search_params['q_filters'].append(q_filter)
        else:
            queryset_search_params['q_filters'] = [q_filter]

    @staticmethod
    def populate_file_details(cr_json, request):
        """
        Populate individual research_dataset.file and directory objects with their
        corresponding objects from their db tables.

        Additionally, for directories, include two other calculated fields:
        - byte_size, for total size of files
        - file_count, for total number of files

        Note: Some of these results may be very useful to cache, or cache the entire dataset
        if feasible.
        """
        from metax_api.api.rest.base.serializers import LightDirectorySerializer
        from metax_api.api.rest.base.serializers import LightFileSerializer

        rd = cr_json['research_dataset']
        file_identifiers = [f['identifier'] for f in rd.get('files', [])]

        # these fields must be retrieved from the db in order to do the mapping, even if they are not
        # requested when using ?file_fields=... or ?directory_fields=... ditch those fields
        # at the end of the method if necessary. while not significant perhaps, for the user it is
        # less astonishing.
        dir_identifier_requested = True
        file_identifier_requested = True

        directory_fields, file_fields = FileService._get_requested_file_browsing_fields(request)

        if 'identifier' not in directory_fields:
            directory_fields.append('identifier')
            dir_identifier_requested = False

        if 'identifier' not in file_fields:
            file_fields.append('identifier')
            file_identifier_requested = False

        for file in File.objects.filter(identifier__in=file_identifiers).values(*file_fields):
            for f in rd['files']:
                if f['identifier'] == file['identifier']:
                    f['details'] = LightFileSerializer.serialize(file)
                    continue

        dir_identifiers = [dr['identifier'] for dr in rd.get('directories', [])]

        for directory in Directory.objects.filter(identifier__in=dir_identifiers).values(*directory_fields):
            for dr in rd['directories']:
                if dr['identifier'] == directory['identifier']:
                    dr['details'] = LightDirectorySerializer.serialize(directory)
                    continue

        if not dir_identifiers:
            return

        if not directory_fields or ('byte_size' in directory_fields or 'file_count' in directory_fields):
            # no specific fields requested -> retrieve,
            # OR byte_size or file_count among requested fields -> retrieve

            _directory_data = CatalogRecord.objects.values_list('_directory_data', flat=True) \
                .get(pk=cr_json['id'])

            for dr in rd['directories']:

                if 'details' not in dr:
                    # probably the directory did not have its details populated
                    # because the dataset is deprecated and the directory no longer exists
                    continue

                FileService.retrieve_directory_byte_sizes_and_file_counts_for_cr(dr['details'],
                    not_cr_id=None, directory_fields=directory_fields, cr_directory_data=_directory_data)

        # cleanup identifiers, if they were not actually requested
        if not dir_identifier_requested:
            for dr in rd['directories']:
                del dr['details']['identifier']

        if not file_identifier_requested:
            for f in rd['files']:
                del f['details']['identifier']

    @classmethod
    def transform_datasets_to_format(cls, catalog_records_json, target_format,
            include_xml_declaration=True, request=None):
        """
        params:
        catalog_records: a list of catalog record dicts, or a single dict
        """
        if target_format in ('datacite', 'fairdata_datacite'):

            is_strict = target_format == 'datacite'
            dummy_doi = False

            if request:
                dummy_doi = CommonService.get_boolean_query_param(request, 'dummy_doi')

            return DataciteService().convert_catalog_record_to_datacite_xml(
                catalog_records_json, include_xml_declaration, is_strict, dummy_doi=dummy_doi)

        def _preprocess_list(key, value):
            """
            Helper function to get right structure for list values. This function is called recursively.
            """
            if key not in ['item', 'researchdataset'] and isinstance(value, list) and len(value) > 1:
                value = {'item': value}
            return key, value

        if isinstance(catalog_records_json, dict):
            content_to_transform = { 'researchdataset': catalog_records_json['research_dataset'] }
        else:
            rd_list = { 'researchdataset': (cr['research_dataset'] for cr in catalog_records_json) }
            content_to_transform = { 'researchdatasets': rd_list }

        xml_str = xmltodict.unparse(content_to_transform, preprocessor=_preprocess_list)
        xml_str = xml_str.replace('\n', '', 1)

        # This is a bit ugly way to put the metax data to the datacite namespace,
        # which allows us to use the default namespace in xquery files.
        xml_str = xml_str.replace('<researchdataset>',
            '<researchdataset xmlns="http://uri.suomi.fi/datamodel/ns/mrd#">')
        if target_format == 'metax':
            # mostly for debugging purposes, the 'metax xml' can be returned as well
            return xml_str

        target_xslt_file_path = join(dirname(dirname(__file__)), 'api/rest/base/xslt/%s.xslt' % target_format)

        try:
            with open(target_xslt_file_path) as f:
                xslt = f.read()
        except OSError:
            raise Http400('Requested format \'%s\' is not available' % target_format)

        try:
            transformed_xml = sxq.execute(xslt, xml_str)
        except:
            _logger.exception('Something is wrong with the xslt file at %s:' % target_xslt_file_path)
            raise Http503('Requested format \'%s\' is currently unavailable' % target_format)

        if include_xml_declaration:
            return '<?xml version="1.0" encoding="UTF-8" ?>%s' % transformed_xml
        else:
            return transformed_xml

    @classmethod
    def validate_reference_data(cls, research_dataset, cache):
        """
        Validate certain fields from the received dataset against reference data, which contains
        the allowed values for these fields.

        If a field value is valid, some of the object's fields will also be populated from the cached
        reference data, overwriting possible values already entered. The fields that will be populated
        from the reference data are:

        - uri (usually to object's field 'identifier')
        - label (usually to object's field 'pref_label')

        """
        reference_data = cls.get_reference_data(cache)
        # ic(reference_data)
        refdata = reference_data['reference_data']
        orgdata = reference_data['organization_data']['organization']
        errors = defaultdict(list)

        for theme in research_dataset.get('theme', []):
            ref_entry = cls.check_ref_data(refdata['keyword'], theme['identifier'],
                                           'research_dataset.theme.identifier', errors)
            if ref_entry:
                cls.populate_from_ref_data(ref_entry, theme, label_field='pref_label')

        for fos in research_dataset.get('field_of_science', []):
            ref_entry = cls.check_ref_data(refdata['field_of_science'], fos['identifier'],
                                           'research_dataset.field_of_science.identifier', errors)
            if ref_entry:
                cls.populate_from_ref_data(ref_entry, fos, label_field='pref_label')

        for remote_resource in research_dataset.get('remote_resources', []):

            for license in remote_resource.get('license', []):
                license_id = license.get('identifier', False)
                license_url = license.get('license', False)

                if license_id:
                    ref_entry = cls.check_ref_data(refdata['license'], license['identifier'],
                                                'research_dataset.remote_resources.license.identifier', errors)
                    if ref_entry:
                        cls.populate_from_ref_data(ref_entry, license, label_field='title', add_in_scheme=False)
                        # Populate license field from reference data only if it is empty, i.e. not provided by the user
                        # and when the reference data license has a same_as entry
                        if not license_url and ref_entry.get('same_as', False):
                            license_url = ref_entry['same_as']

                if license_url:
                    license['license'] = license_url

            if remote_resource.get('resource_type', False):
                ref_entry = cls.check_ref_data(refdata['resource_type'], remote_resource['resource_type']['identifier'],
                                               'research_dataset.remote_resources.resource_type.identifier', errors)
                if ref_entry:
                    cls.populate_from_ref_data(ref_entry, remote_resource['resource_type'], label_field='pref_label')

            if remote_resource.get('file_type', False):
                ref_entry = cls.check_ref_data(refdata['file_type'], remote_resource['file_type']['identifier'],
                                               'research_dataset.remote_resources.file_type.identifier', errors)
                if ref_entry:
                    cls.populate_from_ref_data(ref_entry, remote_resource['file_type'], label_field='pref_label')

            if remote_resource.get('use_category', False):
                ref_entry = cls.check_ref_data(refdata['use_category'], remote_resource['use_category']['identifier'],
                                               'research_dataset.remote_resources.use_category.identifier', errors)
                if ref_entry:
                    cls.populate_from_ref_data(ref_entry, remote_resource['use_category'], label_field='pref_label')

        for language in research_dataset.get('language', []):
            ref_entry = cls.check_ref_data(refdata['language'], language['identifier'],
                                           'research_dataset.language.identifier', errors)
            if ref_entry:
                label_field = 'title'
                cls.populate_from_ref_data(ref_entry, language, label_field=label_field, add_in_scheme=False)
                cls.remove_language_obj_irrelevant_titles(language, label_field)

        access_rights = research_dataset.get('access_rights', None)
        if access_rights:
            if 'access_type' in access_rights:
                ref_entry = cls.check_ref_data(refdata['access_type'], access_rights['access_type']['identifier'],
                                               'research_dataset.access_rights.access_type.identifier', errors)
                if ref_entry:
                    cls.populate_from_ref_data(ref_entry, access_rights['access_type'], label_field='pref_label')

            for rg in access_rights.get('restriction_grounds', []):
                ref_entry = cls.check_ref_data(refdata['restriction_grounds'], rg['identifier'],
                                               'research_dataset.access_rights.restriction_grounds.identifier', errors)
                if ref_entry:
                    cls.populate_from_ref_data(ref_entry, rg, label_field='pref_label')

            for license in access_rights.get('license', []):
                license_id = license.get('identifier', False)
                license_url = license.get('license', False)

                if license_id:
                    ref_entry = cls.check_ref_data(refdata['license'], license_id,
                                                'research_dataset.access_rights.license.identifier', errors)
                    if ref_entry:
                        cls.populate_from_ref_data(ref_entry, license, label_field='title', add_in_scheme=False)
                        # Populate license field from reference data only if it is empty, i.e. not provided by the user
                        # and when the reference data license has a same_as entry
                        if not license_url and ref_entry.get('same_as', False):
                            license_url = ref_entry['same_as']

                if license_url:
                    license['license'] = license_url

        for project in research_dataset.get('is_output_of', []):
            for org_obj in project.get('source_organization', []):
                cls.process_org_obj_against_ref_data(orgdata, org_obj,
                                                     'research_dataset.is_output_of.source_organization',
                                                     refdata=refdata, errors=errors)

            for org_obj in project.get('has_funding_agency', []):
                cls.process_org_obj_against_ref_data(orgdata, org_obj,
                                                     'research_dataset.is_output_of.has_funding_agency',
                                                     refdata=refdata, errors=errors)

            if project.get('funder_type', False):
                ref_entry = cls.check_ref_data(refdata['funder_type'], project['funder_type']['identifier'],
                                               'research_dataset.is_output_of.funder_type.identifier', errors)
                if ref_entry:
                    cls.populate_from_ref_data(ref_entry, project['funder_type'], label_field='pref_label')

        for other_identifier in research_dataset.get('other_identifier', []):
            if other_identifier.get('type', False):
                ref_entry = cls.check_ref_data(refdata['identifier_type'], other_identifier['type']['identifier'],
                                               'research_dataset.other_identifier.type.identifier', errors)
                if ref_entry:
                    cls.populate_from_ref_data(ref_entry, other_identifier['type'], label_field='pref_label')

            if other_identifier.get('provider', False):
                cls.process_org_obj_against_ref_data(orgdata, other_identifier['provider'],
                                                     'research_dataset.other_identifier.provider', refdata=refdata,
                                                     errors=errors)

        for spatial in research_dataset.get('spatial', []):
            as_wkt = spatial.get('as_wkt', [])

            if spatial.get('place_uri', False):
                place_uri = spatial.get('place_uri')
                ref_entry = cls.check_ref_data(refdata['location'], place_uri['identifier'],
                                               'research_dataset.spatial.place_uri.identifier', errors)
                if ref_entry:
                    cls.populate_from_ref_data(ref_entry, place_uri, label_field='pref_label')

                    # Populate as_wkt field from reference data only if it is empty, i.e. not provided by the user
                    # and when the coordinates are available in the reference data
                    if not as_wkt and ref_entry.get('wkt', False):
                        as_wkt.append(ref_entry.get('wkt'))

            if as_wkt:
                spatial['as_wkt'] = as_wkt

        for file in research_dataset.get('files', []):
            if file.get('file_type', False):
                ref_entry = cls.check_ref_data(refdata['file_type'], file['file_type']['identifier'],
                                               'research_dataset.files.file_type.identifier', errors)
                if ref_entry:
                    cls.populate_from_ref_data(ref_entry, file['file_type'], label_field='pref_label')

            if file.get('use_category', False):
                ref_entry = cls.check_ref_data(refdata['use_category'], file['use_category']['identifier'],
                                               'research_dataset.files.use_category.identifier', errors)
                if ref_entry:
                    cls.populate_from_ref_data(ref_entry, file['use_category'], label_field='pref_label')

        for directory in research_dataset.get('directories', []):
            if directory.get('use_category', False):
                ref_entry = cls.check_ref_data(refdata['use_category'], directory['use_category']['identifier'],
                                               'research_dataset.directories.use_category.identifier', errors)
                if ref_entry:
                    cls.populate_from_ref_data(ref_entry, directory['use_category'], label_field='pref_label')

        for contributor in research_dataset.get('contributor', []):
            cls.process_research_agent_obj_with_type(orgdata, refdata, errors, contributor,
                                                     'research_dataset.contributor')

        if research_dataset.get('publisher', False):
            cls.process_research_agent_obj_with_type(orgdata, refdata, errors, research_dataset['publisher'],
                                                     'research_dataset.publisher')

        for curator in research_dataset.get('curator', []):
            cls.process_research_agent_obj_with_type(orgdata, refdata, errors, curator, 'research_dataset.curator')

        for creator in research_dataset.get('creator', []):
            cls.process_research_agent_obj_with_type(orgdata, refdata, errors, creator, 'research_dataset.creator')

        for rights_holder in research_dataset.get('rights_holder', []):
            cls.process_research_agent_obj_with_type(orgdata, refdata, errors, rights_holder,
                                                     'research_dataset.rights_holder')

        for activity in research_dataset.get('provenance', []):
            for was_associated_with in activity.get('was_associated_with', []):
                cls.process_research_agent_obj_with_type(orgdata, refdata, errors, was_associated_with,
                                                         'research_dataset.provenance.was_associated_with')

            if activity.get('spatial', False):
                spatial = activity['spatial']
                as_wkt = spatial.get('as_wkt', [])

                if spatial.get('place_uri', False):
                    place_uri = spatial.get('place_uri')
                    ref_entry = cls.check_ref_data(refdata['location'], place_uri['identifier'],
                                                   'research_dataset.provenance.spatial.place_uri.identifier', errors)
                    if ref_entry:
                        cls.populate_from_ref_data(ref_entry, place_uri, label_field='pref_label')

                        # Populate as_wkt field from reference data only if it is empty, i.e. not provided by the user
                        # and when the coordinates are available in the reference data
                        if not as_wkt and ref_entry.get('wkt', False):
                            as_wkt.append(ref_entry.get('wkt'))

                if as_wkt:
                    spatial['as_wkt'] = as_wkt

            if activity.get('lifecycle_event', False):
                ref_entry = cls.check_ref_data(refdata['lifecycle_event'],
                                               activity['lifecycle_event']['identifier'],
                                               'research_dataset.provenance.lifecycle_event.identifier', errors)

                if ref_entry:
                    cls.populate_from_ref_data(ref_entry, activity['lifecycle_event'], label_field='pref_label')

            if activity.get('preservation_event', False):
                ref_entry = cls.check_ref_data(refdata['preservation_event'],
                                               activity['preservation_event']['identifier'],
                                               'research_dataset.provenance.preservation_event.identifier', errors)

                if ref_entry:
                    cls.populate_from_ref_data(ref_entry, activity['preservation_event'], label_field='pref_label')

            if activity.get('event_outcome', False):
                ref_entry = cls.check_ref_data(refdata['event_outcome'],
                                               activity['event_outcome']['identifier'],
                                               'research_dataset.provenance.event_outcome.identifier', errors)

                if ref_entry:
                    cls.populate_from_ref_data(ref_entry, activity['event_outcome'], label_field='pref_label')
        for infra in research_dataset.get('infrastructure', []):
            ref_entry = cls.check_ref_data(refdata['research_infra'], infra['identifier'],
                                           'research_dataset.infrastructure.identifier', errors)
            if ref_entry:
                cls.populate_from_ref_data(ref_entry, infra, label_field='pref_label')

        for relation in research_dataset.get('relation', []):
            if relation.get('relation_type', False):
                ref_entry = cls.check_ref_data(refdata['relation_type'], relation['relation_type']['identifier'],
                                               'research_dataset.relation.relation_type.identifier', errors)
                if ref_entry:
                    cls.populate_from_ref_data(ref_entry, relation['relation_type'], label_field='pref_label')

            if relation.get('entity', False) and relation.get('entity').get('type', False):

                ref_entry = cls.check_ref_data(refdata['resource_type'], relation['entity']['type']['identifier'],
                                               'research_dataset.relation.entity.type.identifier', errors)
                if ref_entry:
                    cls.populate_from_ref_data(ref_entry, relation['entity']['type'], label_field='pref_label')

        if errors:
            raise ValidationError(errors)

    @classmethod
    def remove_contact_info_metadata(cls, rd):
        """
        Strip research dataset of any confidential/private contact information not supposed to be available for the
        general public

        :param rd:
        :return: research dataset with removed contact information
        """
        return remove_keys_recursively(rd, ['email', 'telephone', 'phone'])

    @classmethod
    def check_and_remove_metadata_based_on_access_type(cls, rd):
        """
        If necessary, strip research dataset of any confidential/private information not supposed to be
        available for the general public based on the research dataset's access type.

        :param rd:
        :return: research dataset with removed metadata based on access type
        """

        access_type_id = cls.get_research_dataset_access_type(rd)
        if access_type_id == ACCESS_TYPES['open']:
            pass
        elif access_type_id == ACCESS_TYPES['login']:
            pass
        elif access_type_id == ACCESS_TYPES['permit']:
            # TODO:
            # If user does not have rems permission for the catalog record, strip it:
            # cls._strip_file_and_directory_metadata(rd)

            # strip always for now. Remove this part when rems checking is implemented
            cls._strip_file_and_directory_metadata(rd)
        elif access_type_id == ACCESS_TYPES['embargo']:
            try:
                embargo_time_passed = get_tz_aware_now_without_micros() >= \
                    parse_timestamp_string_to_tz_aware_datetime(cls.get_research_dataset_embargo_available(rd))
            except Exception as e:
                _logger.error(e)
                embargo_time_passed = False

            if not embargo_time_passed:
                cls._strip_file_and_directory_metadata(rd)
        else:
            cls._strip_file_and_directory_metadata(rd)

        return rd

    @classmethod
    def _strip_file_and_directory_metadata(cls, rd):
        cls._strip_file_metadata(rd)
        cls._strip_directory_metadata(rd)

    @classmethod
    def _strip_file_metadata(cls, rd):
        """
        Keys to leave for a File object:
        - title
        - use_category
        - file_type
        - details.byte_size, if exists

        :param rd:
        """
        file_keys_to_leave = set(['title', 'use_category', 'file_type', 'details'])
        details_keys_to_leave = set(['byte_size'])

        for file in rd.get('files', []):
            leave_keys_in_dict(file, file_keys_to_leave)
            if 'details' in file:
                leave_keys_in_dict(file['details'], details_keys_to_leave)

    @classmethod
    def _strip_directory_metadata(cls, rd):
        """
        Keys to leave for a Directory object:
        - title
        - use_category
        - details.byte_size, if exists

        :param rd:
        """
        dir_keys_to_leave = set(['title', 'use_category', 'details'])
        details_keys_to_leave = set(['byte_size'])

        for dir in rd.get('directories', []):
            leave_keys_in_dict(dir, dir_keys_to_leave)
            if 'details' in dir:
                leave_keys_in_dict(dir['details'], details_keys_to_leave)

    @staticmethod
    def get_research_dataset_access_type(rd):
        return rd.get('access_rights', {}).get('access_type', {}).get('identifier', '')

    @staticmethod
    def get_research_dataset_embargo_available(rd):
        return rd.get('access_rights', {}).get('available', '')

    @staticmethod
    def get_research_dataset_license_url(rd):
        """
        Return identifier of the first license if there is a license at all
        """
        if not rd.get('access_rights', {}).get('license'):
            return {}

        license = rd['access_rights']['license'][0]

        return license.get('identifier') or license.get('license')
