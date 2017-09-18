from rest_framework.serializers import ValidationError

from metax_api.models import CatalogRecord, DataCatalog, File, Contract
from metax_api.services import CatalogRecordService as CRS
from metax_api.services import CommonService
from .data_catalog_serializer import DataCatalogSerializer
from .common_serializer import CommonSerializer
from .contract_serializer import ContractSerializer
from .serializer_utils import validate_json

from os import path
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
            self.initial_data['data_catalog'] = self._get_id_from_related_object('data_catalog')
        if self.initial_data.get('contract', False):
            self.initial_data['contract'] = self._get_id_from_related_object('contract')

        self._set_dataset_schema()
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
        self._validate_uniqueness(value)
        CRS.validate_reference_data(value, self.context['view'].cache)
        return value

    def _validate_json_schema(self, value):
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

    def _validate_uniqueness(self, value):
        """
        Unfortunately for unique fields inside a jsonfield, Django does not offer a neat
        http400 error with an error message, so have to do it ourselves.
        """
        field_name = 'preferred_identifier'

        if not value.get(field_name, None):
            # during create preferred_identifier is not necessarily set
            return

        found_obj = self._get_object(field_name, value[field_name])

        if not found_obj:
            found_obj = self._get_object('urn_identifier', value[field_name])

        if found_obj and (self._operation_is_create() or self.instance.id != found_obj.id):
            raise ValidationError(['catalog record with this research_dataset ->> %s already exists.' % field_name])

    def _get_object(self, field_name, identifier):
        # check cache
        from_cache = self.context['view'].cache.get(identifier)
        if from_cache:
            return CatalogRecordSerializer(from_cache).instance
        # check db
        try:
            return CatalogRecord.objects.get(**{ 'research_dataset__contains': { field_name: identifier }})
        except CatalogRecord.DoesNotExist:
            return None

    def _get_file_objects(self, files_dict):
        file_pids = [ f['identifier'] for f in files_dict ]
        return File.objects.filter(identifier__in=file_pids)

    def _get_id_from_related_object(self, relation_field):
        '''
        Use for finding out a related object's id. The related object or its id or identifier
        should be present in the initial data's relation field.

        :param relation_field:
        :return: id of the related object
        '''
        identifier_value = self.initial_data[relation_field]
        id = False
        if isinstance(identifier_value, int):
            id = identifier_value
        elif isinstance(identifier_value, str):
            # some kind of longer string (urn) identifier
            if relation_field == 'contract':
                try:
                    id = Contract.objects.get(contract_json__contains={ 'identifier': identifier_value }).id
                except Contract.DoesNotExist:
                    raise ValidationError({ 'contract': ['contract with identifier %s not found.' % str(identifier_value)]})
            elif relation_field == 'data_catalog':
                try:
                    id = DataCatalog.objects.get(catalog_json__contains={ 'identifier': identifier_value }).id
                except DataCatalog.DoesNotExist:
                    raise ValidationError({ 'data_catalog': ['data catalog with identifier %s not found' % str(identifier_value)]})
            else:
                pass
        elif isinstance(identifier_value, dict):
            id = int(identifier_value['id'])
        else:
            _logger.error('is_valid() field validation for relation %s: unexpected type: %s'
                          % (relation_field, type(identifier_value)))
            raise ValidationError('Validation error for relation %s. Data in unexpected format' % relation_field)
        return id

    def _set_dataset_schema(self):
        if self.initial_data.get('data_catalog', False):
            data_catalog_id = self.initial_data.get('data_catalog')
            try:
                catalog_json = DataCatalog.objects.get(pk=data_catalog_id).catalog_json
                self.json_schema = CommonService.get_json_schema(path.dirname(__file__) + '/../schemas', 'dataset', catalog_json.get('research_dataset_schema', False))
            except DataCatalog.DoesNotExist:
                raise ValidationError({'data_catalog': ['data catalog with identifier %s not found' % str(data_catalog_id)]})
