from os import path

from rest_framework.serializers import ValidationError

from metax_api.models import CatalogRecord, DataCatalog, File, Contract
from metax_api.services import CatalogRecordService as CRS, CommonService
from .data_catalog_serializer import DataCatalogSerializer
from .common_serializer import CommonSerializer
from .contract_serializer import ContractSerializer
from .serializer_utils import validate_json

import logging
_logger = logging.getLogger(__name__)
d = logging.getLogger(__name__).debug

class CatalogRecordSerializer(CommonSerializer):

    class Meta:
        model = CatalogRecord
        fields = (
            'id',
            'contract',
            'data_catalog',
            'research_dataset',
            'preservation_state',
            'preservation_state_modified',
            'preservation_description',
            'preservation_reason_description',
            'ready_status',
            'mets_object_identifier',
            'dataset_group_edit',
            'next_version_id',
            'next_version_identifier',
            'previous_version_id',
            'previous_version_identifier',
            'version_created',
            'owner_id',
            'modified_by_user_id',
            'modified_by_api',
            'created_by_user_id',
            'created_by_api',
        )
        extra_kwargs = {
            # not required during creation, or updating
            # they are overwritten by the api on save or create
            'modified_by_user_id':      { 'required': False },
            'modified_by_api':          { 'required': False },
            'created_by_user_id':       { 'required': False },
            'created_by_api':           { 'required': False },

            # these values are generated automatically or provide default values on creation.
            # some fields can be later updated by the user, some are generated
            'preservation_state':       { 'required': False },
            'preservation_description': { 'required': False },
            'preservation_state_modified':    { 'required': False },
            'ready_status':             { 'required': False },
            'mets_object_identifier':   { 'required': False },
            'owner_id':                 { 'required': False },

            'next_version_id':              { 'required': False },
            'next_version_identifier':      { 'required': False },
            'previous_version_id':          { 'required': False },
            'previous_version_identifier':  { 'required': False },
            'version_created':              { 'required': False },
        }

    def is_valid(self, raise_exception=False):
        if self.initial_data.get('data_catalog', False):
            self.initial_data['data_catalog'] = self._get_id_from_related_object('data_catalog', self._get_data_catalog_relation)
        if self.initial_data.get('contract', False):
            self.initial_data['contract'] = self._get_id_from_related_object('contract', self._get_contract_relation)
        super(CatalogRecordSerializer, self).is_valid(raise_exception=raise_exception)

    def update(self, instance, validated_data):
        instance = super(CatalogRecordSerializer, self).update(instance, validated_data)

        # for partial updates research_dataset is not necessarily set
        files_dict = validated_data.get('research_dataset', None) and validated_data['research_dataset'].get('files', None) or None

        if files_dict:
            instance.files.clear()
            instance.files.add(*self._get_file_objects(files_dict))
            instance.save()

        return instance

    def create(self, validated_data):
        instance = super(CatalogRecordSerializer, self).create(validated_data)
        files_dict = validated_data['research_dataset'].get('files', None)
        if files_dict:
            instance.files.add(*self._get_file_objects(files_dict))
            instance.save()
        return instance

    def to_representation(self, instance):
        res = super(CatalogRecordSerializer, self).to_representation(instance)
        res['data_catalog'] = DataCatalogSerializer(instance.data_catalog).data

        if res.get('contract', None):
            res['contract'] = ContractSerializer(instance.contract).data

        return res

    def validate_research_dataset(self, value):
        self._validate_json_schema(value)
        self._validate_research_dataset_uniqueness(value)
        CRS.validate_reference_data(value, self.context['view'].cache)
        return value

    def _validate_json_schema(self, value):
        self._set_dataset_schema()

        if self._operation_is_create():
            # urn_identifier cant be provided by the user, but it is a required field =>
            # add urn_identifier temporarily to pass schema validation. proper value
            # will be generated later in CatalogRecord model save().
            value['urn_identifier'] = 'temp'

            if not value.get('preferred_identifier', None):
                # mandatory, but might not be present (urn_identifier copied to it later).
                # use temporary value and remove after schema validation.
                value['preferred_identifier'] = 'temp'

            validate_json(value, self.json_schema)

            value.pop('urn_identifier')

            if value['preferred_identifier'] == 'temp':
                value.pop('preferred_identifier')

        else:
            # update operations

            if not value.get('preferred_identifier', None):
                # preferred_identifier can not be updated to empty. if the user is trying to
                # do so, copy urn_identifier to it.
                value['preferred_identifier'] = value['urn_identifier']

            validate_json(value, self.json_schema)

    def _validate_research_dataset_uniqueness(self, research_dataset):
        """
        Validate research_dataset preferred_identifier uniqueness, that it is unique
        within the data catalog it is being saved into. urn_identifier is always generated
        by the server, so no need to check its uniqueness.

        Unfortunately for unique fields inside a jsonfield, Django does not offer a neat
        http400 error with an error message, so have to do it ourselves.
        """
        preferred_identifier_value = research_dataset.get('preferred_identifier', None)

        if not preferred_identifier_value:
            # during create preferred_identifier is not necessarily set
            return

        found_obj = None
        found_using_pref_id = False
        found_using_urn_id = False

        found_obj = self._find_object_using_identifier('preferred_identifier', preferred_identifier_value)

        if found_obj:
            found_using_pref_id = True
        else:
            # cr not found using preferred_identifier. preferred_identifier value can never
            # be urn_identifier value in any catalog, so look for existing records
            # using urn_identifier also
            found_obj = self._find_object_using_identifier('urn_identifier', preferred_identifier_value)
            if found_obj:
                found_using_urn_id = True

        if not found_obj:
            return

        if self._operation_is_create() or self.instance.id != found_obj.id:
            if found_using_pref_id:
                raise ValidationError([
                    'a catalog record with this research_dataset ->> preferred_identifier'
                    ' already exists in this data catalog.'
                ])
            elif found_using_urn_id:
                raise ValidationError([
                    'a catalog record already exists which has the given preferred_identifier'
                    ' value %s as its urn_identifier value.' % preferred_identifier_value
                ])
            else: # pragma no over
                raise Exception('should never happen')

    def _find_object_using_identifier(self, field_name, identifier):
        """
        A helper for checking research_dataset uniqueness. A standard get_object() basically,
        except that it:
        - takes into account data_catalog when searching by preferred_identifier
        - does not use select_related() to also fetch relations, since they are not needed.
        """
        params = { 'research_dataset__contains': { field_name: identifier }}

        if field_name == 'preferred_identifier':

            # only look for hits within the same data catalog.

            if self._operation_is_create():
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
            # checking urn_identifier - data catalog does not need to be checked
            pass

        try:
            return CatalogRecord.objects.get(**params)
        except CatalogRecord.DoesNotExist:
            return None

    def _get_file_objects(self, files_dict):
        file_pids = [ f['identifier'] for f in files_dict ]
        return File.objects.filter(identifier__in=file_pids)

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

    def _set_dataset_schema(self):
        data_catalog = None
        if self._operation_is_create():
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

        self.json_schema = CommonService.get_json_schema(path.dirname(__file__) + '/../schemas', 'dataset', schema_prefix)
