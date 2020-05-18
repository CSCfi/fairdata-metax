# This file is part of the Metax API service
#
# Copyright 2017-2018 Ministry of Education and Culture, Finland
#
# :author: CSC - IT Center for Science Ltd., Espoo Finland <servicedesk@csc.fi>
# :license: MIT

import logging
from os import path

from django.conf import settings as django_settings
from rest_framework.serializers import ValidationError

from metax_api.exceptions import Http403
from metax_api.models import CatalogRecord, DataCatalog, Directory, Contract, Common, File
from metax_api.services import (
    CatalogRecordService as CRS,
    CommonService,
    DataCatalogService,
    RedisCacheService as cache,
)
from .common_serializer import CommonSerializer
from .contract_serializer import ContractSerializer
from .data_catalog_serializer import DataCatalogSerializer
from .serializer_utils import validate_json


_logger = logging.getLogger(__name__)

# when end user creates a record, strip all fields except these
END_USER_CREATE_ALLOWED_FIELDS = [
    'data_catalog',
    'research_dataset',

    # not set by the user, but are set by metax, so should not be discarded
    'date_created',
    'user_created',
    '__request'
]

# when end user updates a record, strip all fields except these
END_USER_UPDATE_ALLOWED_FIELDS = [
    'research_dataset',

    # not set by the user, but are set by metax, so should not be discarded
    'date_modified',
    'user_modified',
    'service_modified',
    '__request'
]

END_USER_ALLOWED_DATA_CATALOGS = django_settings.END_USER_ALLOWED_DATA_CATALOGS
LEGACY_CATALOGS = django_settings.LEGACY_CATALOGS


class CatalogRecordSerializer(CommonSerializer):

    class Meta:
        model = CatalogRecord
        fields = (
            'id',
            'identifier',
            'alternate_record_set',
            'contract',
            'data_catalog',
            'dataset_version_set',
            'deprecated',
            'date_deprecated',
            'metadata_owner_org',
            'metadata_provider_org',
            'metadata_provider_user',
            'research_dataset',
            'preservation_dataset_version',
            'preservation_dataset_origin_version',
            'preservation_state',
            'preservation_state_modified',
            'preservation_description',
            'preservation_reason_description',
            'preservation_identifier',
            'dataset_version_set',
            'next_dataset_version',
            'previous_dataset_version',
            'mets_object_identifier',
            'state',
            'editor',
            'cumulative_state',
            'date_cumulation_started',
            'date_cumulation_ended',
            'date_last_cumulative_addition',
            'rems_identifier',
            'access_granter'
        ) + CommonSerializer.Meta.fields

        extra_kwargs = {
            # these values are generated automatically or provide default values on creation.
            # some fields can be later updated by the user, some are generated
            'identifier':                  { 'required': False },
            'preservation_state':          { 'required': False },
            'preservation_description':    { 'required': False },
            'preservation_state_modified': { 'required': False },
            'mets_object_identifier':      { 'required': False },
            'next_dataset_version':        { 'required': False },
            'previous_dataset_version':    { 'required': False },
            'preservation_dataset_origin_version': { 'required': False },
        }

        extra_kwargs.update(CommonSerializer.Meta.extra_kwargs)

    # schemas dir is effectively ../schemas/
    _schemas_directory_path = path.join(path.dirname(path.dirname(__file__)), 'schemas')

    def is_valid(self, raise_exception=False):
        if self._request_by_end_user():
            if self._operation_is_create:
                self._end_user_create_validations(self.initial_data)
            elif self._operation_is_update:
                self._end_user_update_validations(self.instance, self.initial_data)

        if self.initial_data.get('data_catalog', False):
            self.initial_data['data_catalog'] = self._get_id_from_related_object(
                'data_catalog', self._get_data_catalog_relation)
        if self.initial_data.get('contract', False):
            self.initial_data['contract'] = self._get_id_from_related_object('contract', self._get_contract_relation)

        self.initial_data.pop('alternate_record_set', None)
        self.initial_data.pop('dataset_version_set', None)
        self.initial_data.pop('next_dataset_version', None)
        self.initial_data.pop('previous_dataset_version', None)
        self.initial_data.pop('deprecated', None)
        self.initial_data.pop('date_deprecated', None)
        self.initial_data.pop('state', None)
        self.initial_data.pop('preservation_identifier', None)
        self.initial_data.pop('preservation_dataset_version', None)
        self.initial_data.pop('preservation_dataset_origin_version', None)
        self.initial_data.pop('rems_identifier', None)

        if self._data_catalog_is_changed():
            # updating data catalog, but not necessarily research_dataset.
            # here, make sure to validate uniqueness using what is currently saved
            # in the database, and what the data catalog is being changed to.
            self._validate_research_dataset_uniqueness(self.instance.research_dataset)

        # executes other validation related code, such as validate_research_dataset()
        super(CatalogRecordSerializer, self).is_valid(raise_exception=raise_exception)

        # ensure any operation made on research_dataset during serializer.is_valid(),
        # is still compatible with the schema
        if 'research_dataset' in self.initial_data:
            self.validate_json_schema(self.initial_data['research_dataset'])
            self._validate_org_name_is_set(self.initial_data['research_dataset'])

    def update(self, instance, validated_data):
        if 'preserve_version' in self.context['request'].query_params:
            if self._request_by_end_user():
                raise Http403({ 'detail': [ 'Parameter preserve_version not permitted for end users' ]})
            # execute updates without creating new versions
            instance.preserve_version = True

        return super(CatalogRecordSerializer, self).update(instance, validated_data)

    def create(self, validated_data):
        if self._migration_override_requested():

            # any custom stuff before create that might be necessary for migration purposes
            pid = ''
            if validated_data['research_dataset'].get('preferred_identifier', False):
                # store pid, since it will be overwritten during create otherwise
                pid = validated_data['research_dataset']['preferred_identifier']

        res = super().create(validated_data)

        if self._migration_override_requested():

            # any custom stuff after create that my be necessary for migration purposes
            if pid:
                # save original pid provided by the requestor
                res.research_dataset['preferred_identifier'] = pid

                # save, while bypassing normal save-related procedures in CatalogRecord model
                super(Common, res).save()

        return res

    def _end_user_update_validations(self, instance, validated_data):
        """
        Enforce some rules related to end users when updating records.
        """
        self._check_end_user_allowed_catalogs(instance.data_catalog.catalog_json['identifier'])
        fields_to_discard = [ key for key in validated_data.keys() if key not in END_USER_UPDATE_ALLOWED_FIELDS ]
        for field_name in fields_to_discard:
            del validated_data[field_name]

    def _end_user_create_validations(self, validated_data):
        """
        Enforce some rules related to end users when creating records. End users...
        - Can put data only into specified fields.
        - Can only create records into specified catalogs (and can only use the identifier value, not id's directly!)
        - Will have some fields automatically filled for them
        """
        fields_to_discard = [ key for key in validated_data.keys() if key not in END_USER_CREATE_ALLOWED_FIELDS ]
        for field_name in fields_to_discard:
            del validated_data[field_name]

        # set some fields to whoever the authentication token belonged to.
        validated_data['metadata_provider_user'] = self.context['request'].user.username
        validated_data['metadata_provider_org'] = self.context['request'].user.token['schacHomeOrganization']
        validated_data['metadata_owner_org'] = self.context['request'].user.token['schacHomeOrganization']

        try:
            dc_identifier = validated_data['data_catalog'].catalog_json['identifier']
        except:
            try:
                if isinstance(validated_data['data_catalog'], dict):
                    dc_identifier = validated_data['data_catalog']['identifier']
                else:
                    dc_identifier = validated_data['data_catalog']
            except KeyError:
                # an error is raise later about missing required field
                return

        self._check_end_user_allowed_catalogs(dc_identifier)

    def _check_end_user_allowed_catalogs(self, dc_identifier):
        if dc_identifier not in END_USER_ALLOWED_DATA_CATALOGS:
            raise Http403({
                'detail': [
                    'You do not have access to the selected data catalog. Please use one of the following '
                    'catalogs: %s' % ', '.join(END_USER_ALLOWED_DATA_CATALOGS)
                ]
            })

    def to_representation(self, instance):
        res = super(CatalogRecordSerializer, self).to_representation(instance)

        if 'data_catalog' in res:
            if self.expand_relation_requested('data_catalog'):
                res['data_catalog'] = DataCatalogSerializer(instance.data_catalog).data
            else:
                res['data_catalog'] = {
                    'id': instance.data_catalog.id,
                    'identifier': instance.data_catalog.catalog_json['identifier'],
                }

        if 'contract' in res:
            if self.expand_relation_requested('contract'):
                res['contract'] = ContractSerializer(instance.contract).data
            else:
                res['contract'] = {
                    'id': instance.contract.id,
                    'identifier': instance.contract.contract_json['identifier']
                }

        if 'alternate_record_set' in res:
            alternate_records = instance.alternate_record_set.records.exclude(pk=instance.id)
            if len(alternate_records):
                res['alternate_record_set'] = [ ar.identifier for ar in alternate_records ]

        if 'dataset_version_set' in res:
            res['dataset_version_set'] = instance.dataset_version_set.get_listing()

        if 'next_dataset_version' in res:
            if instance.next_dataset_version.state == CatalogRecord.STATE_PUBLISHED:
                res['next_dataset_version'] = instance.next_dataset_version.identifiers_dict
            elif instance.user_is_owner(instance.request or self.context['request']):
                # include additional information to show the owner this version is actually still just a draft
                res['next_dataset_version'] = instance.next_dataset_version.identifiers_dict
                res['next_dataset_version']['state'] = CatalogRecord.STATE_DRAFT
            else:
                # if the next dataset version is still just a draft, then unauthorized users dont need to know about it.
                del res['next_dataset_version']

        if 'previous_dataset_version' in res:
            res['previous_dataset_version'] = instance.previous_dataset_version.identifiers_dict

        if 'preservation_dataset_version' in res:
            res['preservation_dataset_version'] = instance.preservation_dataset_version.identifiers_dict
            res['preservation_dataset_version']['preservation_state'] = \
                instance.preservation_dataset_version.preservation_state

        elif 'preservation_dataset_origin_version' in res:
            res['preservation_dataset_origin_version'] = instance.preservation_dataset_origin_version.identifiers_dict
            res['preservation_dataset_origin_version']['deprecated'] = \
                instance.preservation_dataset_origin_version.deprecated

        if instance.new_dataset_version_created:
            res['new_version_created'] = instance.new_dataset_version_created

        # Do the population of file_details here, since if it was done in the view, it might not know the file/dir
        # identifiers any longer, since the potential stripping of file/dir fields takes away identifier fields from
        # File and Directory objects, which are needed in populating file_details
        if 'request' in self.context and 'file_details' in self.context['request'].query_params:
            CRS.populate_file_details(res, self.context['request'])

        res = self._check_and_strip_sensitive_fields(instance, res)

        return res

    def _check_and_strip_sensitive_fields(self, instance, res):
        """
        Strip sensitive fields as necessary from the dataset that are not meant for the general public.

        While not stricly a "serializer's" job (job: converting data from one format to another), the
        serializer is a good location to do this kind of "access management" (stripping fields), since
        damn near every api endpoint who returns the research_dataset to the client, does so by using
        this serializer. Additionally, in the serializer, all required data is at hand:
        - the user is known (the http request object)
        - the target record is known and therefore its owner/permissions
        - the data itself is here and always in the same format (as opposed to list/dict/xml/origami)

        Doing the stripping at the end of a request "for all requests", for example in the dataset
        view dispatcher(), is problematic, since at the very end of a request, not all fields may
        be present in the data any longer to make decisions with (?fields=x,y was used), or when
        retrieving a list where some filter was used, the identifiers may not be known either...
        It is much more straightforward to do it here.
        """
        if 'request' in self.context:
            if not instance.user_is_privileged(self.context['request']):
                res['research_dataset'] = CRS.check_and_remove_metadata_based_on_access_type(
                    CRS.remove_contact_info_metadata(res['research_dataset']))

                res.pop('rems_identifier', None)
                res.pop('access_granter', None)

        return res

    def _populate_dir_titles(self, ds):
        """
        If dir title has been omitted, populate it with its dir name.
        """
        if 'directories' not in ds:
            return

        # make sure to not populate title for entries that already contain other dataset-specific metadata
        dirs_to_populate = [
            dr['identifier'] for dr in ds['directories']
            if dr.get('title', None) is None
            and len(dr) > 1
            and dr.get('exclude', False) is False
            and dr.get('delete', False) is False
        ]

        if dirs_to_populate:

            dirs_from_db = [
                dr for dr in
                Directory.objects.filter(identifier__in=dirs_to_populate).values('identifier', 'directory_name')
            ]

            for dr in ds['directories']:
                for i, dir_details in enumerate(dirs_from_db):
                    if dir_details['identifier'] == dr['identifier']:
                        dr['title'] = dir_details['directory_name']
                        dirs_from_db.pop(i)
                        break

    def _populate_file_titles(self, ds):
        """
        If file title has been omitted, populate it with its file name.
        """
        if 'files' not in ds:
            return

        # make sure to not populate title for entries that already contain other dataset-specific metadata
        files_to_populate = [
            f['identifier'] for f in ds['files']
            if f.get('title', None) is None
            and len(f) > 1
            and f.get('exclude', False) is False
            and f.get('delete', False) is False
        ]

        if files_to_populate:

            files_from_db = [
                f for f in
                File.objects.filter(identifier__in=files_to_populate).values('identifier', 'file_name')
            ]

            for f in ds['files']:
                for i, file_details in enumerate(files_from_db):
                    if file_details['identifier'] == f['identifier']:
                        f['title'] = file_details['file_name']
                        files_from_db.pop(i)
                        break

    def validate_research_dataset(self, value):

        self._populate_file_and_dir_titles(value)

        self.validate_json_schema(value)

        if self._operation_is_create or self._preferred_identifier_is_changed():
            self._validate_research_dataset_uniqueness(value)

        CRS.validate_reference_data(value, cache)

        return value

    def _populate_file_and_dir_titles(self, value):
        if 'directories' in value and not value['directories']:
            # remove if empty list
            del value['directories']
        else:

            self._populate_dir_titles(value)

        if 'files' in value and not value['files']:
            # remove if empty list
            del value['files']
        else:
            self._populate_file_titles(value)

    def validate_json_schema(self, value):
        self._set_dataset_schema()

        if self._operation_is_create:
            if not value.get('preferred_identifier', None):
                if DataCatalogService.is_harvested(self.initial_data['data_catalog']):
                    raise ValidationError({ 'preferred_identifier':
                        ['harvested catalog record must have preferred identifier']})

                # normally not present, but may be set by harvesters. if missing,
                # use temporary value and remove after schema validation.
                value['preferred_identifier'] = 'temp'

            if self._migration_override_requested():
                for is_output_of in value.get('is_output_of', []):
                    if 'source_organization' not in is_output_of or not is_output_of['source_organization']:
                        is_output_of['source_organization'] = [
                            {'@type': 'Organization',
                             'identifier': 'MIGRATION_OVERRIDE',
                             'name': {'und': 'temp'}}
                        ]

            validate_json(value, self.json_schema)

            if value['preferred_identifier'] == 'temp':
                value.pop('preferred_identifier')

            if self._migration_override_requested():
                for is_output_of in value.get('is_output_of', []):
                    if 'source_organization' in is_output_of and len(is_output_of['source_organization']) == 1 and \
                            is_output_of['source_organization'][0]['identifier'] == 'MIGRATION_OVERRIDE':
                        is_output_of.pop('source_organization', None)

        else:
            # update operations
            validate_json(value, self.json_schema)

    def _validate_org_name_is_set(self, obj):
        """
        Organization 'name' field is not madatory in the schema, but that is only because it does
        not make sense to end users when using an identifier from reference data, which will overwrite
        the name anyway.

        If after reference data validation name is still missing, the user is also required to enter
        a name.
        """
        if isinstance(obj, dict):
            if '@type' in obj and obj['@type'] == 'Organization' and 'name' not in obj:
                raise ValidationError({
                    'detail': [
                        'Specified organization object does not have a name. If you are using '
                        'an org identifier from reference data, then the name will be populated '
                        'automatically. If your org identifier is not from reference data, you '
                        'must provide the organization name. The object that caused the error: %s'
                        % str(obj)
                    ]
                })
            for field, value in obj.items():
                if isinstance(value, (dict, list)):
                    self._validate_org_name_is_set(value)
        elif isinstance(obj, list):
            for item in obj:
                if isinstance(item, (dict, list)):
                    self._validate_org_name_is_set(item)
        else:
            # string, int, whatever
            pass

    def _validate_research_dataset_uniqueness(self, research_dataset):
        """
        Validate research_dataset preferred_identifier uniqueness, that it is...
        - unique within the data catalog it is being saved into, when saved into harvested catalogs
        - unique globally, when being saved into ATT catalogs, i.e., when
          saving to ATT catalog, and preferred_identifier already exists in other catalogs,
          reject it. When saving to ATT catalog, and preferred_identifier is now
          appearing for the first time, permit it.

        metadata_version_identifier is always generated by the server, so no need to check its uniqueness.

        Unfortunately for unique fields inside a jsonfield, Django does not offer a neat
        http400 error with an error message, so have to do it ourselves.
        """
        if not self._catalog_enforces_unique_pids():
            return

        preferred_identifier_value = research_dataset.get('preferred_identifier', None)

        if not preferred_identifier_value:
            # during create, preferred_identifier is not necessarily set
            return

        if self._find_object_using_identifier('preferred_identifier', preferred_identifier_value):
            if self._data_catalog_supports_versioning():
                raise ValidationError([
                    'a catalog record with this research_dataset ->> preferred_identifier'
                    ' already exists in another data catalog. when saving to ATT catalog,'
                    ' the preferred_identifier must not already exist in other catalogs.'
                ])
            else:
                # harvested catalog
                raise ValidationError([
                    'a catalog record with this research_dataset ->> preferred_identifier'
                    ' already exists in this data catalog.'
                ])

        # cr not found using preferred_identifier. preferred_identifier value should never
        # be the same as metadata_version_identifier value in any catalog, so look for existing records
        # using metadata_version_identifier also
        if self._find_object_using_identifier('metadata_version_identifier', preferred_identifier_value):
            raise ValidationError([
                'a catalog record already exists which has the given preferred_identifier'
                ' value as its metadata_version_identifier value.'
            ])

    def _catalog_enforces_unique_pids(self):
        """
        Check whether the dataset's data catalog enforces dataset pid uniqueness. Currently,
        the only catalogs to not require unique pids, are listed in settings.py LEGACY_CATALOGS.
        """
        if self._operation_is_create:
            try:
                dc = DataCatalog.objects.values('catalog_json').get(pk=self.initial_data['data_catalog'])
            except DataCatalog.DoesNotExist:
                raise ValidationError({ 'detail': ['Provided data catalog does not exist']})
            except KeyError:
                # data_catalog was omitted. an approriate error is raised later.
                return
            dc_pid = dc['catalog_json']['identifier']
        else:
            dc_pid = self.instance.data_catalog.catalog_json['identifier']

        return dc_pid not in LEGACY_CATALOGS

    def _find_object_using_identifier(self, field_name, identifier):
        """
        A helper for checking research_dataset uniqueness. A standard get_object() basically,
        except that it:
        - takes into account data_catalog when searching by preferred_identifier
        - does not use select_related() to also fetch relations, since they are not needed.
        """
        params = { 'research_dataset__contains': { field_name: identifier }}

        if field_name == 'preferred_identifier' and not self._data_catalog_supports_versioning():

            # only look for hits within the same data catalog.

            if self._operation_is_create:
                # value of data_catalog in initial_data is set in is_valid()
                params['data_catalog'] = self.initial_data['data_catalog']
            else:
                # updates
                if 'data_catalog' in self.initial_data:
                    # the update operation is updating data_catalog as well,
                    # so make sure the new catalog is checked for not having
                    # the identifier currently being checked.
                    # value of data_catalog in initial_data is set in is_valid()
                    params['data_catalog'] = self.initial_data['data_catalog']
                else:
                    # a PATCH which does not contain data_catalog - get
                    # data_catalog id from the instance being updated == what
                    # is currently in db
                    params['data_catalog'] = self.instance.data_catalog.id

        else:
            # checking metadata_version_identifier, or saving to ATT catalog - in both cases, find matches
            # globally, instead of only inside a data catalog.
            pass

        if self._operation_is_create:
            return CatalogRecord.objects_unfiltered.filter(**params).exists()
        elif self._data_catalog_supports_versioning():
            # preferred_identifiers already existing in ATT catalogs are fine, so exclude
            # results from ATT catalogs. matches in other catalogs however are considered
            # an error.
            return CatalogRecord.objects_unfiltered.filter(**params) \
                .exclude(data_catalog_id=self.instance.data_catalog_id).exists()
        else:
            return CatalogRecord.objects_unfiltered.filter(**params).exclude(pk=self.instance.id).exists()

    def _data_catalog_is_changed(self):
        """
        Check if data_catalog of the record is being changed. Used to decide if
        preferred_identifier uniqueness should be checked in certain situations.
        """
        if self._operation_is_update and 'data_catalog' in self.initial_data:
            dc = self.initial_data['data_catalog']
            if isinstance(dc, int):
                return dc != self.instance.data_catalog.id
            elif isinstance(dc, str):
                return dc != self.instance.catalog_json['identifier']
            elif isinstance(dc, dict):
                return dc['identifier'] != self.instance.catalog_json['identifier']
            else: # pragma: no cover
                raise ValidationError({ 'detail': ['can not figure out the type of data_catalog'] })

    def _preferred_identifier_is_changed(self):
        """
        Check if preferred_identifier is being updated in the current request or not.
        """
        return self.initial_data['research_dataset'].get('preferred_identifier', None) \
            != self.instance.preferred_identifier

    def _data_catalog_supports_versioning(self):
        if 'data_catalog' in self.initial_data:
            # must always fetch from db, to know if it supports versioning or not
            catalog_json = DataCatalog.objects.filter(pk=self.initial_data['data_catalog']) \
                .only('catalog_json').first().catalog_json
        else:
            try:
                catalog_json = self.instance.data_catalog.catalog_json
            except AttributeError:
                raise ValidationError({ 'data_catalog': ['data_catalog is a required field']})

        return catalog_json.get('dataset_versioning', False) is True

    def _get_contract_relation(self, identifier_value):
        """
        Passed to _get_id_from_related_object() to be used when relation was a string identifier
        """
        if isinstance(identifier_value, dict):
            identifier_value = identifier_value['contract_json']['identifier']
        try:
            return Contract.objects.get(
                contract_json__contains={ 'identifier': identifier_value }
            ).id
        except Contract.DoesNotExist:
            raise ValidationError({ 'contract': ['identifier %s not found.' % str(identifier_value)]})

    def _get_data_catalog_relation(self, identifier_value):
        """
        Passed to _get_id_from_related_object() to be used when relation was a string identifier
        """
        if isinstance(identifier_value, dict):
            identifier_value = identifier_value['catalog_json']['identifier']
        try:
            return DataCatalog.objects.get(
                catalog_json__contains={ 'identifier': identifier_value }
            ).id
        except DataCatalog.DoesNotExist:
            raise ValidationError({ 'data_catalog': ['identifier %s not found' % str(identifier_value)]})

    def _migration_override_requested(self):
        """
        Check presence of query parameter ?migration_override, which enables some specific actions during
        the request, at this point useful only for migration operations.
        """
        migration_override = CommonService.get_boolean_query_param(self.context['request'], 'migration_override')
        if migration_override and self._request_by_end_user():
            raise Http403({ 'detail': [ 'Parameter migration_override not permitted for end users' ]})
        return migration_override

    def _set_dataset_schema(self):
        data_catalog = None
        if self._operation_is_create:
            try:
                data_catalog_id = self._get_id_from_related_object('data_catalog', self._get_data_catalog_relation)
                data_catalog = DataCatalog.objects.get(pk=data_catalog_id)
            except:
                # whatever error happened with data catalog handling - invalid data_catalog
                # value, not found, etc. the error about a required field is raised by django
                # elsewhere. default schema will be used for dataset validation instead
                pass
        else:
            # update operation, relation should be fetched already
            data_catalog = self.instance.data_catalog

        if data_catalog:
            schema_prefix = data_catalog.catalog_json.get('research_dataset_schema', None)
        else:
            schema_prefix = None

        self.json_schema = CommonService.get_json_schema(self._schemas_directory_path, 'dataset', schema_prefix)
