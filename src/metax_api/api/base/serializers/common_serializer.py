from collections import OrderedDict

from rest_framework.serializers import ModelSerializer
from rest_framework.fields import SkipField
from rest_framework.relations import PKOnlyObject
from rest_framework.serializers import ValidationError

import logging
_logger = logging.getLogger(__name__)
d = _logger.debug

class CommonSerializer(ModelSerializer):

    def to_representation(self, instance):
        """
        Copy-pasta / overrided from rest_framework code. Only return fields which have a non-null value

        Object instance -> Dict of primitive datatypes.
        """
        ret = OrderedDict()
        fields = self._readable_fields

        for field in fields:
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
                # this is the overrided block. dont return nulls
                # ret[field.field_name] = None
                pass
            else:
                ret[field.field_name] = field.to_representation(attribute)

        return ret

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
            raise ValidationError({ relation_field: ['Relation dict does not have any fields to identify relation with (id or identifier)'] })
        else:
            _logger.error('is_valid() field validation for relation %s: unexpected type: %s'
                          % (relation_field, type(identifier_value)))
            raise ValidationError('Validation error for relation %s. Data in unexpected format' % relation_field)

    def _operation_is_create(self):
        return self.context['view'].request.stream.method == 'POST'

    def _operation_is_update(self, method=None):
        """
        Check if current operation is of a specific update type, or a generic update operation
        """
        methods = (method, ) if method else ('PUT', 'PATCH')
        try:
            # request.stream.method is not always set!
            return self.context['view'].request.stream.method in methods
        except AttributeError:
            return False
