# This file is part of the Metax API service
#
# Copyright 2017-2018 Ministry of Education and Culture, Finland
#
# :author: CSC - IT Center for Science Ltd., Espoo Finland <servicedesk@csc.fi>
# :license: MIT

import logging
from collections import OrderedDict

from django.db import transaction
from rest_framework.fields import SkipField
from rest_framework.relations import PKOnlyObject
from rest_framework.serializers import ModelSerializer
from rest_framework.serializers import ValidationError

from metax_api.models import Common

_logger = logging.getLogger(__name__)
d = _logger.debug


class CommonSerializer(ModelSerializer):

    # when query parameter ?fields=x,y is used, will include a list of fields to return
    requested_fields = None

    class Meta:
        model = Common
        fields = (
            'user_modified',
            'date_modified',
            'user_created',
            'date_created',
            'service_modified',
            'service_created',
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
        if 'only_fields' in kwargs:
            self.requested_fields = kwargs.pop('only_fields')

        super(CommonSerializer, self).__init__(*args, **kwargs)

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

        if not self.requested_fields and 'request' in self.context and 'fields' in self.context['request'].query_params:
            self.requested_fields = self.context['request'].query_params['fields'].split(',')

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
                return int(identifier_value['id'])
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
