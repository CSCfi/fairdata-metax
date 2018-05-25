from os import path
import logging

from django.http import Http404
from rest_framework import status
from rest_framework.generics import get_object_or_404
from rest_framework.response import Response
from rest_framework.viewsets import ModelViewSet

from metax_api.exceptions import Http403
from metax_api.services import CommonService as CS, ApiErrorService
from metax_api.utils import RedisSentinelCache

_logger = logging.getLogger(__name__)
d = logging.getLogger(__name__).debug


class CommonViewSet(ModelViewSet):

    """
    Using this viewset assumes its model has been inherited from the Common model,
    which include fields like modified and created timestamps, uuid, active flags etc.
    """

    lookup_field_internal = None
    cache = RedisSentinelCache()

    # get_queryset() automatically includes these in .select_related(field1, field2...) when returning
    # queryset to the caller
    select_related = []

    # assigning the create_bulk method here allows for other views to assing their other,
    # customized method to be called instead instead of the generic one.
    create_bulk_method = CS.create_bulk

    def __init__(self, *args, **kwargs):
        super(CommonViewSet, self).__init__(*args, **kwargs)
        if (hasattr(self, 'object') and self.object) and (not hasattr(self, 'queryset') or self.queryset is None):
            # ^ must have attribute 'object' set, AND queryset not set.

            # the primary location where a queryset is initialized for
            # any inheriting viewset, in case not already specified in their ViewSet class.
            # avoids having to specify queryset in each ViewSet separately.
            self.queryset = self.object.objects.all()
            self.queryset_unfiltered = self.object.objects_unfiltered.all()

    def handle_exception(self, exc):
        """
        Store request and response data to disk for later inspection
        """
        response = super(CommonViewSet, self).handle_exception(exc)
        if type(exc) not in (Http403, Http404):
            ApiErrorService.store_error_details(self.request, response, exc)
        return response

    def paginate_queryset(self, queryset):
        if CS.get_boolean_query_param(self.request, 'no_pagination'):
            return None
        return super(CommonViewSet, self).paginate_queryset(queryset)

    def get_queryset(self):
        additional_filters = {}

        if CS.get_boolean_query_param(self.request, 'removed'):
            additional_filters.update({'removed': True})
            self.queryset = self.queryset_unfiltered

        if self.request.query_params.get('fields', False):
            # only specified fields are requested to be returned

            fields = self.request.query_params['fields'].split(',')

            # causes only requested fields to be loaded from the db
            self.queryset = self.queryset.only(*fields)

            # check if requested fields are relations, so that we know to include them in select_related.
            # if no fields is relation, select_related will be made empty.
            self.select_related = [ rel for rel in self.select_related if rel in fields ]

        return super(CommonViewSet, self).get_queryset() \
            .select_related(*self.select_related) \
            .filter(**additional_filters)

    def get_object(self, search_params=None):
        """
        Overrided from rest_framework generics.py method to also allow searching by the field
        lookup_field_other.

        param search_params: pass a custom filter instead of using the default search mechanism
        """
        if CS.get_boolean_query_param(self.request, 'removed'):
            return self.get_removed_object(search_params=search_params)
        elif search_params:
            filter_kwargs = search_params
        else:
            if CS.is_primary_key(self.kwargs.get(self.lookup_field, False)) or not hasattr(self, 'lookup_field_other'):
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

        CS.check_if_unmodified_since(self.request, obj)

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
            if CS.is_primary_key(lookup_value):
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
        CS.update_common_info(request)
        res = super(CommonViewSet, self).update(request, *args, **kwargs)
        return res

    def update_bulk(self, request, *args, **kwargs):
        serializer_class = self.get_serializer_class()
        kwargs['context'] = self.get_serializer_context()
        results, http_status = CS.update_bulk(request, self.object, serializer_class, **kwargs)
        response = Response(results, status=http_status)
        self._check_and_store_bulk_error(request, response)
        return response

    def partial_update(self, request, *args, **kwargs):
        CS.update_common_info(request)
        kwargs['partial'] = True
        res = super(CommonViewSet, self).update(request, *args, **kwargs)
        return res

    def partial_update_bulk(self, request, *args, **kwargs):
        serializer_class = self.get_serializer_class()
        kwargs['context'] = self.get_serializer_context()
        kwargs['partial'] = True
        results, http_status = CS.update_bulk(request, self.object, serializer_class, **kwargs)
        response = Response(results, status=http_status)
        self._check_and_store_bulk_error(request, response)
        return response

    def create(self, request, *args, **kwargs):
        serializer_class = self.get_serializer_class()
        kwargs['context'] = self.get_serializer_context()
        results, http_status = self.create_bulk_method(request, serializer_class, **kwargs)
        response = Response(results, status=http_status)
        self._check_and_store_bulk_error(request, response)
        return response

    def destroy(self, request, *args, **kwargs):
        CS.update_common_info(request)
        return super(CommonViewSet, self).destroy(request, *args, **kwargs)

    def destroy_bulk(self, request, *args, **kwargs):
        # not allowed... for now
        return Response({}, status=status.HTTP_405_METHOD_NOT_ALLOWED)

    def initialize_request(self, request, *args, **kwargs):
        """
        Overrided from rest_framework to preserve the username set during
        identifyapicaller middleware.
        """
        username = request.user.username if hasattr(request.user, 'username') else None
        drf_req = super(CommonViewSet, self).initialize_request(request, *args, **kwargs)
        drf_req.user.username = username
        return drf_req

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
        self.json_schema = CS.get_json_schema(path.dirname(view_file) + '/../schemas',
                                              self.__class__.__name__.lower()[:-(len('viewset'))])

    def _check_and_store_bulk_error(self, request, response):
        """
        Unless ?atomic=true is used in bulk operations, the request does not entirely fail, and
        error data is not saved. Separately check presence of failures in bulk operations responses,
        and save data if necessary.
        """
        if 'failed' in response.data and len(response.data['failed']):
            ApiErrorService.store_error_details(request, response, other={ 'bulk_request': True })
