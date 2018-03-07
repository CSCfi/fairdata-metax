import logging
from os import path

from rest_framework.serializers import ValidationError

from metax_api.models import CatalogRecord, DataCatalog, Contract
from metax_api.services import CatalogRecordService as CRS, CommonService
from .common_serializer import CommonSerializer
from .contract_serializer import ContractSerializer
from .data_catalog_serializer import DataCatalogSerializer
from .serializer_utils import validate_json

_logger = logging.getLogger(__name__)
d = logging.getLogger(__name__).debug

class CatalogRecordSerializer(CommonSerializer):

    class Meta:
        model = CatalogRecord
        fields = (
            'id',
            'alternate_record_set',
            'contract',
            'data_catalog',
            'deprecated',
            'research_dataset',
            'preservation_state',
            'preservation_state_modified',
            'preservation_description',
            'preservation_reason_description',
            'mets_object_identifier',
            'dataset_group_edit',
            'next_version',
            'previous_version',
            'version_set',
            'editor',
        ) + CommonSerializer.Meta.fields

        extra_kwargs = {
            # these values are generated automatically or provide default values on creation.
            # some fields can be later updated by the user, some are generated
            'preservation_state':       { 'required': False },
            'preservation_description': { 'required': False },
            'preservation_state_modified':    { 'required': False },
            'mets_object_identifier':   { 'required': False },
            'next_version':             { 'required': False },
            'previous_version':         { 'required': False },
        }

        extra_kwargs.update(CommonSerializer.Meta.extra_kwargs)

    def is_valid(self, raise_exception=False):
        if self.initial_data.get('data_catalog', False):
            self.initial_data['data_catalog'] = self._get_id_from_related_object(
                'data_catalog', self._get_data_catalog_relation)
        if self.initial_data.get('contract', False):
            self.initial_data['contract'] = self._get_id_from_related_object('contract', self._get_contract_relation)

        self.initial_data.pop('alternate_record_set', None)
        self.initial_data.pop('version_set', None)
        self.initial_data.pop('next_version', None)
        self.initial_data.pop('previous_version', None)

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
            self._validate_json_schema(self.initial_data['research_dataset'])

    def update(self, instance, validated_data):
        if 'preserve_version' in self.context['view'].request.query_params:
            # execute updates without creating new versions
            instance.preserve_version = True
        return super(CatalogRecordSerializer, self).update(instance, validated_data)

    def create(self, validated_data):
        return super(CatalogRecordSerializer, self).create(validated_data)

    def to_representation(self, instance):
        res = super(CatalogRecordSerializer, self).to_representation(instance)
        res['data_catalog'] = DataCatalogSerializer(instance.data_catalog).data

        if 'contract' in res:
            res['contract'] = ContractSerializer(instance.contract).data

        if 'alternate_record_set' in res:
            alternate_records = instance.alternate_record_set.records.exclude(pk=instance.id)
            if len(alternate_records):
                res['alternate_record_set'] = [ ar.metadata_version_identifier for ar in alternate_records ]

        if 'version_set' in res:
            version_set = instance.version_set.records.exclude(pk=instance.id).order_by('date_created')
            if len(version_set):
                res['version_set'] = [{
                    'metadata_version_identifier': v.metadata_version_identifier,
                    'date_created': v.date_created.astimezone().isoformat()
                } for v in version_set]

        if 'next_version' in res:
            res['next_version'] = {
                'id': instance.next_version.id,
                'metadata_version_identifier': instance.next_version.metadata_version_identifier,
                'preferred_identifier': instance.next_version.preferred_identifier,
            }

        if 'previous_version' in res:
            res['previous_version'] = {
                'id': instance.previous_version.id,
                'metadata_version_identifier': instance.previous_version.metadata_version_identifier,
                'preferred_identifier': instance.previous_version.preferred_identifier,
            }

        if instance.next_version_created_in_current_request:
            # inform the view that next_version should be published as a new dataset.
            # the view should also remove the key __actions once it is done.
            res['__actions'] = {
                'publish_next_version': { 'next_version': self.to_representation(instance.next_version) }
            }

        return res

    def validate_research_dataset(self, value):
        self._validate_json_schema(value)
        if self._operation_is_create() or self._preferred_identifier_is_changed():
            self._validate_research_dataset_uniqueness(value)
        CRS.validate_reference_data(value, self.context['view'].cache)
        return value

    def _validate_json_schema(self, value):
        self._set_dataset_schema()

        if self._operation_is_create():
            # metadata_version_identifier cant be provided by the user, but it is a required field =>
            # add metadata_version_identifier temporarily to pass schema validation. proper value
            # will be generated later in CatalogRecord model save().
            value['metadata_version_identifier'] = 'temp'

            if not value.get('preferred_identifier', None):
                # mandatory, but might not be present (metadata_version_identifier copied to it later).
                # use temporary value and remove after schema validation.
                value['preferred_identifier'] = 'temp'

            validate_json(value, self.json_schema)

            value.pop('metadata_version_identifier')

            if value['preferred_identifier'] == 'temp':
                value.pop('preferred_identifier')

        else:
            # update operations

            if not value.get('preferred_identifier', None):
                # preferred_identifier can not be updated to empty. if the user is trying to
                # do so, copy metadata_version_identifier to it.
                # note: if metadata_version_identifier was missing too, validate_json will raise an error
                value['preferred_identifier'] = value.get('metadata_version_identifier', None)

            validate_json(value, self.json_schema)

    def _validate_research_dataset_uniqueness(self, research_dataset):
        """
        Validate research_dataset preferred_identifier uniqueness, that it is...
        - unique within the data catalog it is being saved into, when not being saved
          into ATT catalog
        - unique globally, when being saved into ATT catalog, i.e., when
          saving to ATT catalog, and preferred_identifier already exists in other catalogs,
          reject it. When saving to ATT catalog, and preferred_identifier is now
          appearing for the first time, permit it.

        metadata_version_identifier is always generated by the server, so no need to check its uniqueness.

        Unfortunately for unique fields inside a jsonfield, Django does not offer a neat
        http400 error with an error message, so have to do it ourselves.
        """
        preferred_identifier_value = research_dataset.get('preferred_identifier', None)

        if not preferred_identifier_value:
            # during create, preferred_identifier is not necessarily set
            return

        found_objs = None
        found_using_pref_id = False
        found_using_metadata_id = False

        found_objs = self._find_object_using_identifier('preferred_identifier', preferred_identifier_value)

        if found_objs:
            found_using_pref_id = True
        else:
            # cr not found using preferred_identifier. preferred_identifier value can never
            # be metadata_version_identifier value in any catalog, so look for existing records
            # using metadata_version_identifier also
            found_objs = self._find_object_using_identifier('metadata_version_identifier', preferred_identifier_value)
            if found_objs:
                found_using_metadata_id = True

        if not found_objs:
            return

        if found_using_pref_id:
            if self._data_catalog_supports_versioning():
                raise ValidationError([
                    'a catalog record with this research_dataset ->> preferred_identifier'
                    ' already exists in another data catalog. when saving to ATT catalog,'
                    ' the preferred_identifier must not already exist in other catalogs.'
                ])
            else:
                raise ValidationError([
                    'a catalog record with this research_dataset ->> preferred_identifier'
                    ' already exists in this data catalog.'
                ])
        elif found_using_metadata_id:
            raise ValidationError([
                'a catalog record already exists which has the given preferred_identifier'
                ' value as its metadata_version_identifier value.'
            ])
        else:
            # good case
            pass

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
            # checking metadata_version_identifier, or saving to ATT catalog - in both cases, find matches
            # globally, instead of only inside a data catalog.
            pass

        if self._operation_is_create():
            return CatalogRecord.objects.filter(**params)
        elif self._data_catalog_supports_versioning():
            # preferred_identifiers already existing in ATT catalog are fine, so exclude
            # results from ATT catalog. matches in other catalogs however are considered
            # an error.
            return CatalogRecord.objects.filter(**params).exclude(data_catalog_id=1)
        else:
            return CatalogRecord.objects.filter(**params).exclude(pk=self.instance.id)

    def _data_catalog_is_changed(self):
        """
        Check if data_catalog of the record is being changed. Used to decide if
        preferred_identifier uniqueness should be checked in certain situations.
        """
        if self._operation_is_update() and 'data_catalog' in self.initial_data:
            dc = self.initial_data['data_catalog']
            if isinstance(dc, int):
                return dc != self.instance.data_catalog.id
            elif isinstance(dc, str):
                return dc != self.instance.catalog_json['identifier']
            elif isinstance(dc, dict):
                return dc['identifier'] != self.instance.catalog_json['identifier']
            else: # pragma: no cover
                raise ValidationError({ 'detail': ['cant figure out the type of data_catalog'] })

    def _preferred_identifier_is_changed(self):
        """
        Check if preferred_identifier is being updated in the current request or not.

        For PUT, all fields are always present, so checking is easy. for PATCH, first check
        if the field is even present, and only then check if it being changed.
        """
        if self._operation_is_update('PUT'):
            return self.initial_data['research_dataset']['preferred_identifier'] \
                != self.instance.preferred_identifier
        elif self._operation_is_update('PATCH'):
            if 'preferred_identifier' in self.initial_data['research_dataset']:
                return self.initial_data['research_dataset']['preferred_identifier'] \
                    != self.instance.preferred_identifier
        else:
            return False

    def _data_catalog_supports_versioning(self):
        if 'data_catalog' in self.initial_data:
            # must always fetch from db, to know if it supports versioning or not
            catalog_json = DataCatalog.objects.filter(pk=self.initial_data['data_catalog']) \
                .only('catalog_json').first().catalog_json
        else:
            catalog_json = self.instance.data_catalog.catalog_json

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

        self.json_schema = CommonService.get_json_schema(
            path.dirname(__file__) + '/../schemas', 'dataset', schema_prefix)
