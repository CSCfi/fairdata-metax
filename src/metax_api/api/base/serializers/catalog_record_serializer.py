from rest_framework.serializers import ModelSerializer, ValidationError

from metax_api.models import CatalogRecord, DatasetCatalog, File, Contract
from metax_api.services import CatalogRecordService as CRS
from .dataset_catalog_serializer import DatasetCatalogSerializer
from .contract_serializer import ContractSerializer
from .serializer_utils import validate_json

import logging
_logger = logging.getLogger(__name__)
d = logging.getLogger(__name__).debug

class CatalogRecordSerializer(ModelSerializer):

    class Meta:
        model = CatalogRecord
        fields = (
            'id',
            'contract',
            'dataset_catalog',
            'research_dataset',
            'preservation_state',
            'preservation_state_modified',
            'preservation_state_description',
            'preservation_reason_description',
            'contract_identifier',
            'mets_object_identifier',
            'dataset_group_edit',
            'next_version_id',
            'next_version_identifier',
            'previous_version_id',
            'previous_version_identifier',
            'version_created',
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
            'preservation_state_description': { 'required': False },
            'preservation_state_modified':    { 'required': False },
            'ready_status':             { 'required': False },
            'contract_identifier':      { 'required': False },
            'mets_object_identifier':   { 'required': False },
            'catalog_record_modified':  { 'required': False },

            'next_version_id':              { 'required': False },
            'next_version_identifier':      { 'required': False },
            'previous_version_id':          { 'required': False },
            'previous_version_identifier':  { 'required': False },
            'version_created':              { 'required': False },
        }

    def is_valid(self, raise_exception=False):
        if self.initial_data.get('dataset_catalog', False):
            self.initial_data['dataset_catalog'] = self._get_id_from_related_object(self.initial_data, 'dataset_catalog')
        if self.initial_data.get('contract', False):
            self.initial_data['contract'] = self._get_id_from_related_object(self.initial_data, 'contract')
        super(CatalogRecordSerializer, self).is_valid(raise_exception=raise_exception)

    def update(self, instance, validated_data):
        instance = super(CatalogRecordSerializer, self).update(instance, validated_data)
        files_dict = validated_data.get('research_dataset', None) and validated_data['research_dataset'].get('files', None) or None
        if files_dict:
            file_pids = [ f['identifier'] for f in files_dict ]
            files = File.objects.filter(identifier__in=file_pids)
            instance.files.clear()
            instance.files.add(*files)
            instance.save()
        return instance

    def create(self, validated_data):
        instance = super(CatalogRecordSerializer, self).create(validated_data)
        files_dict = validated_data['research_dataset']['files'].copy()
        file_pids = [ f['identifier'] for f in files_dict ]
        files = File.objects.filter(identifier__in=file_pids)
        instance.files.add(*files)
        instance.save()
        return instance

    def to_representation(self, data):
        res = super(CatalogRecordSerializer, self).to_representation(data)
        # todo this is an extra query... (albeit qty of storages in db is tiny)
        # get FileStorage dict from context somehow ? self.initial_data ?
        fsrs = DatasetCatalogSerializer(DatasetCatalog.objects.get(id=res['dataset_catalog']))
        contract_serializer = ContractSerializer(Contract.objects.get(id=res['contract']))
        res['dataset_catalog'] = fsrs.data
        res['contract'] = contract_serializer.data
        return res

    def validate_research_dataset(self, value):
        # add urn_identifier temporarily to pass schema validation. proper value
        # will be generated later in CatalogRecord model save().
        value['urn_identifier'] = 'temp'
        validate_json(value, self.context['view'].json_schema)
        value.pop('urn_identifier')

        self._validate_uniqueness(value)
        CRS.validate_reference_data(value, self.context['view'].cache)
        return value

    def _validate_uniqueness(self, value):
        """
        Unfortunately for unique fields inside a jsonfield, Django does not offer a neat
        http400 error with an error message, so have to do it ourselves.
        """
        field_name = 'preferred_identifier'
        found_obj = self._get_object(field_name, value[field_name])
        if found_obj:
            http_method = self.context['view'].request.stream.method
            if http_method == 'POST' or self.instance.id != found_obj.id:
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

    def _get_id_from_related_object(self, initial_data, relation_field):
        id = False
        if type(initial_data[relation_field]) in (int, str):
            id = initial_data[relation_field]
        elif isinstance(initial_data[relation_field], dict):
            id = int(initial_data[relation_field]['id'])
        else:
            _logger.error('is_valid() field validation for relation %s: unexpected type: %s'
                          % (relation_field, type(initial_data[relation_field])))
            raise ValidationError('Validation error for relation %s. Data in unexpected format' % relation_field)
        return id
