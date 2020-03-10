# This file is part of the Metax API service
#
# Copyright 2017-2018 Ministry of Education and Culture, Finland
#
# :author: CSC - IT Center for Science Ltd., Espoo Finland <servicedesk@csc.fi>
# :license: MIT

import logging
import re

from django.db import transaction
from django.http import Http404
from django.utils.decorators import method_decorator
from rest_framework import status
from rest_framework.decorators import detail_route, list_route
from rest_framework.exceptions import ValidationError
from rest_framework.renderers import JSONRenderer
from rest_framework.response import Response

from metax_api.exceptions import Http400, Http403
from metax_api.models import File, XmlMetadata
from metax_api.renderers import XMLRenderer
from metax_api.services import AuthService, CommonService, FileService
from .common_view import CommonViewSet
from ..serializers import FileSerializer, XmlMetadataSerializer


_logger = logging.getLogger(__name__)

# i.e. /rest/v6/files, but must NOT end in /
# or: /rest/files, but must NOT end in /
RE_PATTERN_FILES_CREATE = re.compile(r'^/rest/(v\d/)?files(?!/)')


# none of the methods in this class use atomic requests by default! see method dispatch()
@transaction.non_atomic_requests
class FileViewSet(CommonViewSet):

    serializer_class = FileSerializer
    object = File
    select_related = ['file_storage', 'parent_directory']

    lookup_field = 'pk'

    # allow search by external identifier (urn, or whatever string in practice) as well
    lookup_field_other = 'identifier'

    # customized create_bulk which handles both directories and files in the same
    # bulk_create request.
    create_bulk_method = FileService.create_bulk

    def __init__(self, *args, **kwargs):
        self.set_json_schema(__file__)
        super(FileViewSet, self).__init__(*args, **kwargs)

    def _use_transaction(self, request):
        # todo add checking of ?atomic parameter too?
        if CommonService.get_boolean_query_param(self.request, 'dryrun'):
            return True
        elif request.method == 'POST' and RE_PATTERN_FILES_CREATE.match(request.META['PATH_INFO']):
            # for POST /files only (creating), do not use a transaction !
            return False
        return True

    @method_decorator(transaction.non_atomic_requests)
    def dispatch(self, request, **kwargs):
        """
        In order to decorate a class-based view method with non_atomic_requests, some
        more effort is required.

        For POST /files requests, do not wrap the request inside a transaction, in order to enable
        closing and re-opening the db connection during the request in an effort to keep the
        process from being dramatically slowed down during large file inserts.

        Literature:
        https://docs.djangoproject.com/en/2.0/topics/class-based-views/intro/#decorating-the-class
        https://docs.djangoproject.com/en/2.0/topics/db/transactions/#django.db.transaction.non_atomic_requests
        """
        if self._use_transaction(request):
            with transaction.atomic():
                _logger.debug('Note: Request in transaction')
                return super().dispatch(request, **kwargs)
        else:
            _logger.debug('Note: Request not in transaction')
            return super().dispatch(request, **kwargs)

    def list(self, request, *args, **kwargs):
        self.queryset_search_params = FileService.get_queryset_search_params(request)
        if not request.user.is_service:
            # end users can only retrieve their own files
            user_projects = AuthService.get_user_projects(request)
            self.queryset_search_params['project_identifier__in'] = user_projects
        return super().list(request, *args, **kwargs)

    def update(self, request, *args, **kwargs):
        #This have to be checked before updating common info
        if not isinstance(self.request.data, dict):
            raise Http400('request message body must be a single json object')

        return super().update(request, *args, **kwargs)

    def partial_update(self, request, *args, **kwargs):
        #This have to be checked before updating common info
        if not isinstance(self.request.data, dict):
            raise Http400('request message body must be a single json object')

        return super().partial_update(request, *args, **kwargs)

    def get_object(self, search_params=None):
        """
        Deals with allowed_projects query parameter. This is done here to avoid multiple
        get_object calls in single request.
        """
        obj = super().get_object(search_params)
        if self.request.user.is_service:
            allowed_projects = CommonService.get_list_query_param(self.request, 'allowed_projects')
            if allowed_projects is not None and obj.project_identifier not in allowed_projects:
                raise Http403('You do not have permission to update this file')

        return obj

    def update_bulk(self, request, *args, **kwargs):
        """
        Checks that all files belongs to project in allowed_projects query parameter
        if given.
        """
        if self.request.user.is_service:
            FileService.check_allowed_projects(request)

        return super().update_bulk(request, *args, **kwargs)

    def partial_update_bulk(self, request, *args, **kwargs):
        """
        Checks that all files belongs to project in allowed_projects query parameter
        if given.
        """
        if self.request.user.is_service:
            FileService.check_allowed_projects(request)

        return super().partial_update_bulk(request, *args, **kwargs)

    @list_route(methods=['post'], url_path="datasets")
    def datasets(self, request):
        """
        Find out which datasets a list of files belongs to, and return their
        metadata_version_identifiers as a list.

        The method is invoked using POST, because there are limits to length of query
        parameters in GET. Also, some clients forcibly shove parameters in body in GET
        requests to query parameters, so using POST instead is more guaranteed to work.
        """

        if CommonService.get_boolean_query_param(request, 'detailed'):
            return FileService.get_detailed_datasets_where_file_belongs_to(request.data)

        return FileService.get_datasets_where_file_belongs_to(request.data)

    @list_route(methods=['post'], url_path="restore")
    def restore_files(self, request):
        """
        Restore removed files.
        """
        return FileService.restore_files(request, request.data)

    def destroy(self, request, pk, **kwargs):
        return FileService.destroy_single(self.get_object())

    def destroy_bulk(self, request, *args, **kwargs):
        return FileService.destroy_bulk(request.data)

    @detail_route(methods=['get', 'post', 'put', 'delete'], url_path='xml')
    def xml_handler(self, request, pk=None):
        file = self.get_object()

        if request.method == 'GET':
            return self._get_xml(request, file)
        else:
            if 'namespace' not in request.query_params:
                raise Http400('namespace is a required query parameter')

            if request.method == 'PUT':
                return self._update_xml(request, file)
            elif request.method == 'POST':
                return self._create_xml(request, file)
            elif request.method == 'DELETE':
                return self._delete_xml(request, file)
            else:
                raise Http404

    def _get_xml(self, request, file):
        if 'namespace' in request.query_params:
            # get single requested xml metadata by namespace
            try:
                xml_metadata = file.xmlmetadata_set.get(namespace=request.query_params['namespace'])
            except XmlMetadata.DoesNotExist:
                raise Http404
            request.accepted_renderer = XMLRenderer()
            return Response(data=xml_metadata.xml, status=status.HTTP_200_OK)

        else:
            # return list of namespaces of xml metadatas associated with the file
            xml_namespaces = file.xmlmetadata_set.all().values_list('namespace', flat=True)
            request.accepted_renderer = JSONRenderer()
            return Response(data=[ ns for ns in xml_namespaces ], status=status.HTTP_200_OK)

    def _create_xml(self, request, file):
        try:
            file.xmlmetadata_set.get(namespace=request.query_params['namespace'])
        except XmlMetadata.DoesNotExist:
            # good - create for the first time
            pass
        else:
            raise Http400('xml metadata with namespace %s already exists' % request.query_params['namespace'])

        new_xml_metadata = self._xml_request_to_dict_data(request, file)
        serializer = XmlMetadataSerializer(data=new_xml_metadata)

        try:
            serializer.is_valid(raise_exception=True)
        except ValidationError:
            return Response(data=serializer.errors, status=status.HTTP_400_BAD_REQUEST)

        serializer.save()
        request.accepted_renderer = XMLRenderer()
        return Response(data=serializer.instance.xml, status=status.HTTP_201_CREATED)

    def _update_xml(self, request, file):
        try:
            xml_metadata = file.xmlmetadata_set.get(namespace=request.query_params['namespace'])
        except XmlMetadata.DoesNotExist:
            raise Http404

        new_xml_metadata = self._xml_request_to_dict_data(request, file)
        serializer = XmlMetadataSerializer(xml_metadata, data=new_xml_metadata)

        try:
            serializer.is_valid(raise_exception=True)
        except ValidationError:
            return Response(data=serializer.errors, status=status.HTTP_400_BAD_REQUEST)

        serializer.save()
        return Response(data=None, status=status.HTTP_204_NO_CONTENT)

    def _delete_xml(self, request, file):
        try:
            xml_metadata = file.xmlmetadata_set.get(namespace=request.query_params['namespace'])
        except XmlMetadata.DoesNotExist:
            raise Http404
        xml_metadata.delete()
        return Response(data=None, status=status.HTTP_204_NO_CONTENT)

    def _xml_request_to_dict_data(self, request, file):
        """
        Take the original request, and its associated file, and return them in a form
        that is digestable by the XmlMetadataSerializer.
        """
        common_info = CommonService.update_common_info(request, return_only=True)
        new_xml_metadata = {
            'file': file.id,
            'xml': request.data,
            'namespace': request.query_params['namespace']
        }
        new_xml_metadata.update(common_info)
        return new_xml_metadata

    @list_route(methods=['post'], url_path="flush_project")
    def flush_project(self, request): # pragma: no cover
        # todo remove api when comfortable
        raise ValidationError({ 'detail': ['API has been moved to RPC API: /rpc/files/flush_project'] })
