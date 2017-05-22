from datetime import datetime
from json import load as json_load
from jsonschema import validate as json_validate
from os import path

from django.core.exceptions import FieldDoesNotExist
from rest_framework.response import Response
from rest_framework.viewsets import ModelViewSet

import logging
_logger = logging.getLogger(__name__)
d = logging.getLogger(__name__).debug

def get_schema(view_file, model_name):
    """
    view_file is a __file___ variable
    """
    with open(path.dirname(view_file) + '/../schemas/json_schema_%s.json' % model_name) as f:
        return json_load(f)

class CommonViewSet(ModelViewSet):

    """
    Using this viewset assumes its model has been inherited from the Common model,
    which include fields like modified and created timestamps, uuid, active flags etc.
    """

    json_schema = False
    validate_json_schema = False

    def update(self, request, *args, **kwargs):
        request = self._update_common_info(request)
        if self.validate_json_schema:
            self._do_validate_json_schema(request)
        return super(CommonViewSet, self).update(request, *args, **kwargs)

    def partial_update(self, request, *args, **kwargs):
        request = self._update_common_info(request)
        if self.validate_json_schema:
            self._do_validate_json_schema(request)
        return super(CommonViewSet, self).partial_update(request, *args, **kwargs)

    def create(self, request, *args, **kwargs):
        request = self._update_common_info(request)
        if self.validate_json_schema:
            self._do_validate_json_schema(request)
        return super(CommonViewSet, self).create(request, *args, **kwargs)

    def destroy(self, request, *args, **kwargs):
        """
        Mark record as removed, never delete it completely.
        """
        request = self._update_common_info(request, delete=True)
        request.data.update({ 'removed': True })

        # filter is usually either json__identifier, if searching primarily from the model's json-field,
        # or pk, if model does not contain a json-field, and search happens from standard id-field of the model.
        filter = { self.lookup_field: kwargs[self.lookup_field] }
        ser = self.serializer_class(self.queryset.filter(**filter).update(**request.data))
        return Response(ser.data)

    def _update_common_info(self, request, delete=False):
        """
        Update fields common for all tables and most actions:
        - last modified timestamp and user
        - created on timestamp and user

        flags:
        - delete: Makes sure to update last modified timestamp as well, because
        by default (django field options) it is updated only on regular update.

        Note: returns request to be more obvious, even though request is
        modified in-place as a reference
        """
        user_id = request.user.id or None

        if not user_id:
            _logger.warning("User id not set; unknown user")

        if delete:
            # delete
            request.data.update({ 'modified_by_user_id': user_id, 'modified_by_api': datetime.now() })
        elif request.data.get(self.lookup_field, False):
            # update
            request.data.update({ 'modified_by_user_id': user_id })
        else:
            # create - lookup field was not present in request.data
            request.data.update({ 'created_by_user_id': user_id })

        return request

    def use_json_schema_validation(self, view_file):
        """
        An inheriting class can call this method in its constructor if it wants to do
        json schema validation automatically for update and create methods.

        view_file is __file__ of the inheriting object. This way get_schema()
        always looks for the schema from a directory relative to the view's location,
        taking into account its version.
        """
        self.json_schema = get_schema(view_file, self.__class__.__name__.lower()[:-(len('viewset'))])
        self.validate_json_schema = True

    def _do_validate_json_schema(self, request):
        if not self.json_schema:
            raise Exception("schema missing")
        json_validate(request.data['json'], self.json_schema)
