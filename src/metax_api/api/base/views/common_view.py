from datetime import datetime
from json import load as json_load
from os import path

from rest_framework import status
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

    lookup_field_internal = None

    def update(self, request, *args, **kwargs):
        self._update_common_info(request)
        res = super(CommonViewSet, self).update(request, *args, **kwargs)
        res.data = {}
        res.status_code = status.HTTP_204_NO_CONTENT
        return res

    def partial_update(self, request, *args, **kwargs):
        self._update_common_info(request)
        kwargs['partial'] = True
        return super(CommonViewSet, self).update(request, *args, **kwargs)

    def create(self, request, *args, **kwargs):
        self._update_common_info(request)

        is_many = isinstance(request.data, list)

        if is_many:

            # dont fail the entire request if only some inserts fail.
            # successfully created rows are added to 'successful', and
            # failed inserts are added to 'failed', with a related error message.
            results = { 'success': [], 'failed': []}

            for row in request.data:

                serializer = self.get_serializer(data=row)

                try:
                    serializer.is_valid(raise_exception=True)
                except Exception as e:
                    results['failed'].append({ 'object': serializer.data, 'errors': serializer.errors })
                else:
                    serializer.save()
                    results['success'].append({ 'object': serializer.data, 'errors': serializer.errors })

            if len(results['success']):
                # if even one insert was successful, general status of the request is success
                http_status = status.HTTP_201_CREATED
            else:
                # only if all inserts have failed, return a general failure for the whole request
                http_status = status.HTTP_400_BAD_REQUEST

        else:
            serializer = self.get_serializer(data=request.data)
            serializer.is_valid(raise_exception=True)
            serializer.save()
            http_status = status.HTTP_201_CREATED

        headers = self.get_success_headers(serializer.data)
        return Response(results if is_many else serializer.data, status=http_status, headers=headers)

    def destroy(self, request, *args, **kwargs):
        self._update_common_info(request)
        return super(CommonViewSet, self).destroy(request, *args, **kwargs)

    def _update_common_info(self, request):
        """
        Update fields common for all tables and most actions:
        - last modified timestamp and user
        - created on timestamp and user
        """
        user_id = request.user.id or None

        if not user_id:
            _logger.warning("User id not set; unknown user")

        method = request.stream and request.stream.method or False
        current_time = datetime.now()
        common_info = {}

        if method in ('PUT', 'PATCH', 'DELETE'):
            common_info.update({
                'modified_by_user_id': user_id,
                'modified_by_api': current_time
            })
        elif method == 'POST':
            common_info.update({
                'created_by_user_id': user_id,
                'created_by_api': current_time,
            })
        else:
            pass

        if common_info:

            if isinstance(request.data, list):
                for row in request.data:
                    row.update(common_info)
            else:
                request.data.update(common_info)

    def set_json_schema(self, view_file):
        """
        An inheriting class can call this method in its constructor to get its json
        schema. The validation is done in the serializer's validate_<field> method.

        Parameters:
        - view_file: __file__ of the inheriting object. This way get_schema()
        always looks for the schema from a directory relative to the view's location,
        taking into account its version.
        """
        self.json_schema = get_schema(view_file, self.__class__.__name__.lower()[:-(len('viewset'))])
