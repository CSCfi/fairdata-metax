from datetime import datetime
from json import load as json_load
from os import path

from django.http import Http404
from rest_framework import status
from rest_framework.generics import get_object_or_404
from rest_framework.response import Response
from rest_framework.viewsets import ModelViewSet

import logging
_logger = logging.getLogger(__name__)
d = logging.getLogger(__name__).debug

def get_schema(view_file, model_name):
    """
    view_file is a __file__ variable
    """
    with open(path.dirname(view_file) + '/../schemas/json_schema_%s.json' % model_name) as f:
        return json_load(f)

class CommonViewSet(ModelViewSet):

    """
    Using this viewset assumes its model has been inherited from the Common model,
    which include fields like modified and created timestamps, uuid, active flags etc.
    """

    lookup_field_internal = None

    def __init__(self, *args, **kwargs):
        self.queryset = self.object.objects.filter(active=True, removed=False)
        super(CommonViewSet, self).__init__(*args, **kwargs)

    def get_object(self):
        """
        Overrided from rest_framework generics.py method to also allow searching by the field
        lookup_field_other.
        """
        if self.is_primary_key(self.kwargs.get(self.lookup_field, False)) or not hasattr(self, 'lookup_field_other'):
            # lookup by originak lookup_field. standard django procedure
            lookup_url_kwarg = self.lookup_url_kwarg or self.lookup_field
        else:
            # lookup by alternative field lookup_field_other
            lookup_url_kwarg = self.lookup_field_other

            # replace original field name with field name in lookup_field_other
            self.kwargs[lookup_url_kwarg] = self.kwargs.pop(self.lookup_field)

        queryset = self.filter_queryset(self.get_queryset())

        assert lookup_url_kwarg in self.kwargs, (
            'Expected view %s to be called with a URL keyword argument '
            'named "%s". Fix your URL conf, or set the `.lookup_field` '
            'attribute on the view correctly.' %
            (self.__class__.__name__, lookup_url_kwarg)
        )

        filter_kwargs = { lookup_url_kwarg: self.kwargs[lookup_url_kwarg] }

        try:
            obj = get_object_or_404(queryset, **filter_kwargs)
        except Exception as e:
            _logger.debug('get_object(): could not find an object with field and value: %s: %s' % (lookup_url_kwarg, filter_kwargs[lookup_url_kwarg]))
            raise Http404

        # May raise a permission denied
        self.check_object_permissions(self.request, obj)

        return obj


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

                # todo test with single serializer but change data (performance)
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

    def is_primary_key(self, received_lookup_value):
        if not received_lookup_value:
            return False
        elif ':' in received_lookup_value:
            # probably urn
            return False
        else:
            return True
