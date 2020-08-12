# This file is part of the Metax API service
#
# Copyright 2017-2018 Ministry of Education and Culture, Finland
#
# :author: CSC - IT Center for Science Ltd., Espoo Finland <servicedesk@csc.fi>
# :license: MIT

from copy import deepcopy
from collections import OrderedDict
from datetime import datetime
import logging

from django.db import transaction
from django.db.models.query import QuerySet
from rest_framework.fields import SkipField
from rest_framework.relations import PKOnlyObject
from rest_framework.serializers import ModelSerializer
from rest_framework.serializers import ValidationError
from metax_api.exceptions import Http400

from metax_api.models import Common


_logger = logging.getLogger(__name__)


class CommonSerializer(ModelSerializer):

    # when query parameter ?fields=x,y is used, will include a list of fields to return
    requested_fields = []

    class Meta:
        model = Common
        fields = (
            'user_modified',
            'date_modified',
            'user_created',
            'date_created',
            'service_modified',
            'service_created',
            'removed',
            'date_removed'
        )
        extra_kwargs = {
            # not required during creation, or updating
            # they would be overwritten by the api anyway.
            # except for user_modified can and should
            # be given by the requestor if possible.
            'user_modified':       { 'required': False },
            'date_modified':       { 'required': False },
            'user_created':        { 'required': False },
            'date_created':        { 'required': False },
            'service_modified':    { 'required': False },
            'service_created':     { 'required': False },
        }

    _operation_is_update = False
    _operation_is_create = False

    def __init__(self, *args, **kwargs):
        """
        For most usual GET requests, the fields to retrieve for an object can be
        specified via the query param ?fields=x,y,z. Retrieve those fields from the
        implicitly passed request object for processing in the to_representation() method.

        The list of fields can also be explicitly passed to the serializer as a list
        in the kw arg 'only_fields', when serializing objects outside of the common GET
        api's.
        """

        super(CommonSerializer, self).__init__(*args, **kwargs)

        if hasattr(self, 'initial_data') and self.initial_data is not None:
            self.initial_data.pop('removed', None)
            self.initial_data.pop('date_removed', None)

        if hasattr(self, 'instance') and self.instance is not None:
            self._operation_is_update = True
        else:
            self._operation_is_create = True

        if self._operation_is_update and self._request_by_end_user():
            # its better to consider requests by end users as partial (= all update
            # requests are considere as PATCH requests), since they may not be
            # permitted to modify some fields. it is very handy
            # to be able to just discard non-permitted fields, instead of observing
            # whether some field value is being changed or not.
            #
            # downside: end users may try to intentionally change some field they are actually
            # not able to change, but then dont get any kind of error message about failing
            # to do so. solution: read the docs and be aware of it.
            self.partial = True

        if 'only_fields' in kwargs:
            self.requested_fields = kwargs.pop('only_fields')

        elif 'request' in self.context and 'fields' in self.context['request'].query_params:
            self.requested_fields = self.context['view'].fields

    @transaction.atomic
    def save(self, *args, **kwargs):
        """
        Inherited to:

        1) Pass the http request object to the instance. Note: Update operations only,
        when the instance exists before saving! i.e.: NOT, when creating.

        2) Use @transaction.atomic, which creates a "save point" in the larger scope
        transaction that lasts during the entire http request, to allow rolling back individual
        serializer.save() operations.

        This is necessary for bulk operations to NOT save changes when an object executes its
        save() multiple times, and there is an irrecoverable failure between those saves.

        Most realistic example: CatalogRecord, which executes multiple saves to deal with
        with metadata_version_identifier generation, file changes handling, alternate_record_set and
        versions handling.
        """
        if hasattr(self, 'instance') and self.instance is not None:
            # update operation.
            # request cant be passed as __request inside kwargs (as is done
            # when creating records), due to some 'known fields only' validations
            # along the way... this seems to be most convenient.
            self.instance.request = self.context.get('request', None)
        super().save(*args, **kwargs)

    def create(self, validated_data):
        # for create, the instance obviously does not exist before creating it. so
        # the request object needs to be passed along the general parameters.
        # luckily, the validate_data is just passed to the Model __init__
        # as **validated_data, and all our extra kwargs can ride along.
        validated_data['__request'] = self.context.get('request', None)
        return super().create(validated_data)

    def to_representation(self, instance):
        """
        Copy-pasta / overrided from rest_framework code with the following modifications:
        - Only return fields which have a non-null value
        - When only specific fields are requested, skip fields accordingly

        Object instance -> Dict of primitive datatypes.
        """
        ret = OrderedDict()
        fields = self._readable_fields

        for field in fields:

            if self.requested_fields and field.field_name not in self.requested_fields:
                continue

            try:
                attribute = field.get_attribute(instance)
            except SkipField:
                continue

            # We skip `to_representation` for `None` values so that fields do
            # not have to explicitly deal with that case.
            #
            # For related fields with `use_pk_only_optimization` we need to
            # resolve the pk value.
            check_for_none = attribute.pk if isinstance(attribute, PKOnlyObject) else attribute
            if check_for_none is None:
                # this is an overrided block. dont return nulls
                # ret[field.field_name] = None
                pass
            else:
                ret[field.field_name] = field.to_representation(attribute)

        return ret

    def expand_relation_requested(self, relation_name):
        """
        Check presense of query parameter 'expand_relation', which is used by serializer
        to decide whether or not to include the complete relation object in the API response
        or not.
        """
        if 'view' in self.context and 'expand_relation' in self.context['view'].request.query_params:
            return relation_name in self.context['view'].request.query_params['expand_relation']
        return False

    def _get_id_from_related_object(self, relation_field, string_relation_func):
        '''
        Use for finding out a related object's id, which Django needs to save the relation
        to the database. The related object, or its id, or identifier
        should be present in the initial data's relation field.

        :param relation_field:
        :return: id of the related object
        :string_relation_func: a function which will be called to retrieve the related object
            in case the relation is a string identifier.
        '''
        identifier_value = self.initial_data[relation_field]

        if isinstance(identifier_value, int):
            # is a db pk
            return identifier_value
        elif isinstance(identifier_value, str):
            try:
                # from some ui's the pk can be disguised as a string
                return int(identifier_value)
            except:
                pass
            # is a string identifier such as urn
            return string_relation_func(identifier_value)
        elif isinstance(identifier_value, dict):
            # the actual related object as a dict. it is expected to be
            # in un-tampered form with normal fields present, since
            # relation fields can not be updated through another object
            if 'id' in identifier_value:
                try:
                    return int(identifier_value['id'])
                except:
                    raise ValidationError({relation_field: ['Validation error for relation id field. '
                                                            'Data in unexpected format']})
            else:
                # try to look for identifier field in the dict
                return string_relation_func(identifier_value['identifier'])
            raise ValidationError({ relation_field: [
                'Relation dict does not have any fields to identify relation with (id or identifier)'] })
        else:
            _logger.error('is_valid() field validation for relation %s: unexpected type: %s'
                          % (relation_field, type(identifier_value)))
            raise ValidationError('Validation error for relation %s. Data in unexpected format' % relation_field)

    def _request_by_end_user(self):
        return 'request' in self.context and not self.context['request'].user.is_service

    def _request_by_service(self):
        return 'request' in self.context and self.context['request'].user.is_service


class LightSerializer():

    """
    LightSerializer is optimized for speed for read-only serializing of a
    dict-form queryset result or result-set (Model.objects.values(*fields).filter()).

    Since by default a queryset.values() returns relation fields only as { relation_field: id },
    some extra code is required to make the output look the same as from a normal serializer.

    Before creating the queryset, the fields for .values(*fields) should ALWAYS be
    retrieved first by calling ls_field_list(), optionally providing list of fields
    to retrieve. This will transform fields such as 'parent_directory' to retrieve more
    relevant fields such as parent_directory__identifier, instead of just parent_directory_id,
    which by itself is useless. The method ls_field_list() should over customized by the
    inheriting serializer to pick whatever field it needs, if the default is not good enough.

    Example use:
        lfs_field_list = LightFileSerializer.ls_field_list()
        queryset = File.objects.values(*lfs_field_list).filter(project_identifier="xyz")
        lfs_output = LightFileSerializer.serialize(queryset)

    The common solution provided by the serializer
    - copies values to fields of same name for basic key-value -type fields
    - transforms relation_field__attribute fields to { relation_field: { attribute: value }},
      as normal serializer outputs them
    - by default for relation fields, only fields id and identifier are fetched, unless
      more are manually specified in ls_field_list().
    - special case: relations where the only interesting field is a json field: the identifier
      from whatever_json is copied, and the end result still looks as from a normal serializer:
      { relation_field: { identifier: value, id: value }}

    Serialization for any field can be overrided in serialize_special_field(). List such
    fields inside cls.special_fields in an inheriting serializer.
    """

    # the serializer will only output these fields.
    # inheriting serializer should set value to a set().
    allowed_fields = None

    # fields that require special handling, if any. can be empty, but must be specified.
    # when not empty, must implement method serialize_special_field(file, field, value).
    # inheriting serializer should set value to a set().
    special_fields = None

    # list names of relation fields for error checking.
    # inheriting serializer should set value to a set().
    relation_fields = None

    # transform field names from cls.relation_fields. for example parent_directory
    # -> parent_directory_id. if the serializer meets a field like this, then that
    # is a programming error, since the queryset was not queried by first calling
    # cls.ls_field_list to get LightSerializer-compatible fields.
    _relation_id_fields = set()

    @staticmethod
    def serialize_special_field(item, field, value):
        """
        If an inheriting serializer needs to handle some field in a special way, this method
        should be implemented. This method gets called when a field name is specified in
        cls.special_fields.
        """
        raise NotImplementedError

    @classmethod
    def ls_field_list(cls, received_field_list=[]):
        """
        Form list of fields to retrieve to be used in a query, where query .values() result
        will be compatible with LightSerializer use.

        For example, when retrieving identifier for field parent_directory, instead of
        querying entirety of parent_directory, query just for parent_directory__identifier.
        Serializer will then recognize the field name.

        This default method only filters out fields that are not ine the allowed_fields list.
        """
        assert isinstance(cls.allowed_fields, set), 'light serializer must specify allowed_fields as a set()'

        if received_field_list:
            # ensure only allowed fields and no rubbish ends up in the query - unknown
            # fields to the db will cause crash.
            field_list = [ field for field in received_field_list if field in cls.allowed_fields ]
            if not field_list:
                raise Http400({ 'detail': ['uh oh, none of the fields you requested are listed in allowed_fields. '
                    'received fields: %s' % str(received_field_list)] })

        else:
            # get all fields
            # deepcopy prevents inheriting classes from modifying the allowed_fields -set which should be static
            field_list = deepcopy(cls.allowed_fields)

        return field_list

    @classmethod
    def serialize(cls, unserialized_data):
        """
        A light serializer for files, intended to serialize results for read-only purposes from a
        Model.objects.filter().values(*fields) query. Serializes either a single result (dictionary)
        or a queryset.

        The end result is supposed to look the same as normally from a serializer.
        """
        assert type(unserialized_data) in (QuerySet, dict, list), 'unserialized_data type must be QuerySet or dict'
        assert isinstance(cls.special_fields, set), 'light serializer must specify special_fields as a set()'
        assert isinstance(cls.relation_fields, set), 'light serializer must specify relation_fields as a set()'

        special_fields = cls.special_fields
        relation_fields = cls.relation_fields
        serialize_special_field = cls.serialize_special_field
        _relation_id_fields = cls._relation_id_fields

        for field in relation_fields:
            _relation_id_fields.add('%s_id' % field)

        if isinstance(unserialized_data, (QuerySet, list)):
            unserialized_data = unserialized_data
            multi = True
        if isinstance(unserialized_data, dict):
            unserialized_data = [unserialized_data]
            multi = False

        serialized_data = []
        data_append = serialized_data.append

        for row in unserialized_data:
            item = {}
            if not row:
                return [] if multi else {}
            for field, value in row.items():
                if value is None:
                    continue
                elif field in special_fields:
                    serialize_special_field(item, field, value)
                elif '__' in field:
                    # fields from relations, such as parent_directory__identifier
                    field_sections = field.split('__')
                    field_name = field_sections[0]
                    if field.endswith('_json'):
                        # _json -ending fields are a special case. only handles
                        # fields identifier and id.
                        if field_name not in item:
                            item[field_name] = {}
                        if isinstance(value, dict):
                            # take identifier from inside the json-field
                            item[field_name] = { 'identifier': value['identifier'] }
                        else:
                            # id
                            item[field_name][field_sections[-1]] = value
                    else:
                        # traditional relation field. can take any field from relation
                        try:
                            item[field_name][field_sections[-1]] = value
                        except KeyError:
                            item[field_name] = { field_sections[-1]: value }
                elif field in _relation_id_fields:
                    raise ValueError(
                        'LightSerializer received field: %s. Expected: %s__attrname. '
                        'Did you forget to pass result of ls_field_list() to queryset '
                        '.values(*field_list)?'
                        % (field, field[:-3])
                    )
                else:
                    if isinstance(value, datetime):
                        value = value.astimezone().isoformat()
                    item[field] = value
            data_append(item)
        return serialized_data if multi else serialized_data[0]
