# This file is part of the Metax API service
#
# Copyright 2017-2018 Ministry of Education and Culture, Finland
#
# :author: CSC - IT Center for Science Ltd., Espoo Finland <servicedesk@csc.fi>
# :license: MIT

from os import path
import logging

from django.http import HttpResponse, Http404
from rest_framework import status
from rest_framework.exceptions import PermissionDenied, MethodNotAllowed, APIException
from rest_framework.generics import get_object_or_404
from rest_framework.response import Response
from rest_framework.views import set_rollback
from rest_framework.viewsets import ModelViewSet

from metax_api.exceptions import Http403, Http500
from metax_api.permissions import EndUserPermissions, ServicePermissions
from metax_api.services import CommonService as CS, ApiErrorService, CallableService, RedisCacheService

_logger = logging.getLogger(__name__)

RESPONSE_SUCCESS_CODES = (200, 201, 204)
WRITE_OPERATIONS = ('PUT', 'PATCH', 'POST')


class CommonViewSet(ModelViewSet):

    """
    Using this viewset assumes its model has been inherited from the Common model,
    which include fields like modified and created timestamps, uuid, active flags etc.
    """

    api_type = 'rest'
    authentication_classes = ()
    permission_classes = [EndUserPermissions, ServicePermissions]

    cache = RedisCacheService

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

    def dispatch(self, request, **kwargs):
        CallableService.clear_callables()
        res = super().dispatch(request, **kwargs)

        if res.status_code in RESPONSE_SUCCESS_CODES:
            if CS.get_boolean_query_param(self.request, 'dryrun'):
                # with dryrun parameter:
                # - nothing must be saved into db
                # - no events must escape from metax to other services, such as rabbitmq
                #   messages, or doi identifier requests
                set_rollback()
            else:
                try:
                    CallableService.run_post_request_callables()
                except Exception as e:
                    res = self.handle_exception(e)
                    # normally .dispatch() does this. sets response.accepted_renderer among other things
                    res = self.finalize_response(request, res, **kwargs)
        return res

    def get_permissions(self):
        """
        Instantiates and returns the list of permissions that this view requires.
        """
        return [
            permission() for permission in self.permission_classes
            if permission.service_permission == self.request.user.is_service
        ]

    def handle_exception(self, exc):
        """
        Every error returned from the api goes through here.

        Catch all unhandled exceptions and convert them to 500's, in a way that
        stores error data. Since 500 error's are usually bugs or similar, well,
        "unhandled" errors, the response should not include too detailed information
        to the end user, in risk of exposing something sensitive.

        Catch and log all raised errors, store request and response data to disk
        for later inspection.
        """

        # APIException base of DRF exceptions
        # HttpResponse base of django http responses / exceptions
        # Http404 a django 404 error that for whatever reason inherits from just Exception...
        if not issubclass(exc.__class__, (APIException, HttpResponse, Http404)):
            # must convert any standard python exceptions to framework-recognized
            # exceptions, so that the following handle_exception() goes the expected
            # path, and prepares for db dollback, sets some headers etc.
            _logger.exception('Internal Server Error')
            exc = Http500({ 'detail': ['Internal Server Error'] })

        try:
            # fyi: when an error occurs during a request, and ATOMIC_REQUESTS=True,
            # set_rollback() is normally called inside below method. the actual
            # rollback does not take place immediately, only the NEED for it is signaled.
            response = super(CommonViewSet, self).handle_exception(exc)
        except:
            # for when absolutely everything has gone wrong...
            _logger.exception('Exception while trying to handle original exception: %s' % str(exc))
            set_rollback()
            response = Response({ 'detail': ['Internal Server Error'] }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

        if type(exc) not in (Http403, Http404, PermissionDenied, MethodNotAllowed):
            ApiErrorService.store_error_details(self.request, response, exc)

        return response

    def paginate_queryset(self, queryset):
        if CS.get_boolean_query_param(self.request, 'no_pagination'):
            return None

        return super(CommonViewSet, self).paginate_queryset(queryset)

    def get_queryset(self):
        """
        Apply parameters received in the request to the queryset, and return it.

        Inheriting methods can still further filter and modify the queryset, since it is
        not immediately evaluated.
        """
        additional_filters = {}
        q_filters = []

        CS.set_if_modified_since_filter(self.request, additional_filters)

        if hasattr(self, 'queryset_search_params'):
            additional_filters.update(**self.queryset_search_params)

        if 'q_filters' in additional_filters:
            # Q-filter objects, which can contain more complex filter options such as OR-clauses
            q_filters = additional_filters.pop('q_filters')

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

        queryset = super().get_queryset().filter(*q_filters, **additional_filters)

        if self.request.META['REQUEST_METHOD'] in WRITE_OPERATIONS:
            # for update operations, do not select relations in the original queryset
            # so that select_for_update() can be used to lock the row for the duration
            # of the update-operation. when the full object is returned, it is possible
            # that additional queres need to be executed to the db to retrieve relation
            # data, but that seems to be the price to pay to be able the lock rows being
            # written to.
            queryset = queryset.select_for_update(nowait=False, of=('self',))
        else:
            queryset = queryset.select_related(*self.select_related)

        return queryset

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
        Overrided from rest_framework to preserve the username and other variables
        set during identifyapicaller middleware.
        """
        username = request.user.username if hasattr(request.user, 'username') else None
        is_service = request.user.is_service if hasattr(request.user, 'is_service') else False
        token = request.user.token if hasattr(request.user, 'token') else None

        drf_req = super(CommonViewSet, self).initialize_request(request, *args, **kwargs)

        drf_req.user.username = username
        drf_req.user.is_service = is_service
        drf_req.user.token = token
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

    def get_api_name(self):
        """
        Return api name, example: DatasetViewSet -> datasets.
        Some views where the below formula does not produce a sensible result
        (for example, directories-api), will inherit this and return a customized
        result.
        """
        return '%ss' % self.__class__.__name__.split('ViewSet')[0].lower()
