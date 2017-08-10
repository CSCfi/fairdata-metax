from django.http import Http404
from rest_framework import status
from rest_framework.generics import get_object_or_404
from rest_framework.response import Response
from rest_framework.viewsets import ModelViewSet

from metax_api.services import CommonService
from metax_api.utils import RabbitMQ, RedisSentinelCache

import logging
_logger = logging.getLogger(__name__)
d = logging.getLogger(__name__).debug


class CommonViewSet(ModelViewSet):

    """
    Using this viewset assumes its model has been inherited from the Common model,
    which include fields like modified and created timestamps, uuid, active flags etc.
    """

    lookup_field_internal = None
    cache = RedisSentinelCache()

    def get_object(self, search_params=None):
        """
        Overrided from rest_framework generics.py method to also allow searching by the field
        lookup_field_other.

        param search_params: pass a custom filter instead of using the default search mechanism
        """
        if search_params:
            filter_kwargs = search_params
        else:
            if CommonService.is_primary_key(self.kwargs.get(self.lookup_field, False)) or not hasattr(self, 'lookup_field_other'):
                # lookup by originak lookup_field. standard django procedure
                lookup_url_kwarg = self.lookup_url_kwarg or self.lookup_field
            else:
                # lookup by alternative field lookup_field_other
                lookup_url_kwarg = self.lookup_field_other

                # replace original field name with field name in lookup_field_other
                self.kwargs[lookup_url_kwarg] = self.kwargs.get(self.lookup_field)

            assert lookup_url_kwarg in self.kwargs, (
                'Expected view %s to be called with a URL keyword argument '
                'named "%s". Fix your URL conf, or set the `.lookup_field` '
                'attribute on the view correctly.' %
                (self.__class__.__name__, lookup_url_kwarg)
            )

            filter_kwargs = { lookup_url_kwarg: self.kwargs[lookup_url_kwarg] }

        queryset = self.filter_queryset(self.get_queryset())

        try:
            obj = get_object_or_404(queryset, **filter_kwargs)
        except Exception:
            raise Http404

        # May raise a permission denied
        self.check_object_permissions(self.request, obj)

        return obj

    def get_removed_object(self, search_params=None):
        """
        Find object using object.objects_unfiltered to find objects that have removed=True.

        Looks using the identifier that was used in the original request, similar
        to how get_object() works, if no search_params are passed.

        Does not check permissions, because currently only used for notification after delete.
        """
        if not search_params:
            lookup_value = self.kwargs.get(self.lookup_field)
            if CommonService.is_primary_key(lookup_value):
                search_params = { 'pk': lookup_value }
            elif hasattr(self, 'lookup_field_other'):
                search_params = { self.lookup_field_other: lookup_value }
            else:
                raise Http404

        try:
            return self.object.objects_unfiltered.get(active=True, **search_params)
        except self.object.DoesNotExist:
            raise Http404

    def update(self, request, *args, **kwargs):
        CommonService.update_common_info(request)
        res = super(CommonViewSet, self).update(request, *args, **kwargs)

        # the normal case is that update (PUT) does not return the updated content.
        # save the updated data in case someone (i.e. datasets) wants to do something with
        # the updated data before returning, so that they are spared from the additional query.
        self._updated_request_data = res.data

        res.data = {}
        res.status_code = status.HTTP_204_NO_CONTENT
        return res

    def partial_update(self, request, *args, **kwargs):
        CommonService.update_common_info(request)
        kwargs['partial'] = True
        return super(CommonViewSet, self).update(request, *args, **kwargs)

    def create(self, request, *args, **kwargs):
        serializer_class = self.get_serializer_class()
        kwargs['context'] = self.get_serializer_context()
        results, http_status = CommonService.create_bulk(request, serializer_class, **kwargs)
        return Response(results, status=http_status)

    def destroy(self, request, *args, **kwargs):
        CommonService.update_common_info(request)
        return super(CommonViewSet, self).destroy(request, *args, **kwargs)

    def _publish_message(self, body, routing_key='', exchange=''):
        rabbitmq = RabbitMQ()
        rabbitmq.publish(body, routing_key=routing_key, exchange=exchange)

    def set_json_schema(self, view_file):
        """
        An inheriting class can call this method in its constructor to get its json
        schema. The validation is done in the serializer's validate_<field> method, which
        has a link to the view it is being used from.

        Parameters:
        - view_file: __file__ of the inheriting object. This way get_schema()
        always looks for the schema from a directory relative to the view's location,
        taking into account its version.
        """
        self.json_schema = CommonService.get_json_schema(view_file, self.__class__.__name__.lower()[:-(len('viewset'))])
