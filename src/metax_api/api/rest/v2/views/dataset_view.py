# This file is part of the Metax API service
#
# Copyright 2017-2018 Ministry of Education and Culture, Finland
#
# :author: CSC - IT Center for Science Ltd., Espoo Finland <servicedesk@csc.fi>
# :license: MIT

import logging

from django.http import Http404
from rest_framework import status
from rest_framework.decorators import detail_route
from rest_framework.response import Response

from metax_api.exceptions import Http400, Http403
from metax_api.models import CatalogRecordV2
from metax_api.api.rest.base.views import DatasetViewSet
from metax_api.api.rest.base.serializers import LightFileSerializer
from metax_api.api.rest.v2.serializers import CatalogRecordSerializer
from metax_api.services import CommonService as CS, CatalogRecordServiceV2

_logger = logging.getLogger(__name__)


UNAUTHORIZED_TO_SEE_FILES_MSG = \
    'You do not have permission to see this information because the dataset access type ' \
    'is not open and you are not the owner of the catalog record.'


class DatasetViewSet(DatasetViewSet):

    service_class = CatalogRecordServiceV2
    serializer_class = CatalogRecordSerializer
    object = CatalogRecordV2

    @detail_route(methods=['get'], url_path="projects")
    def projects_list(self, request, pk=None):

        # note: checks permissions
        cr = self.get_object()

        if not cr.user_is_privileged(request):
            raise Http403

        projects = [
            p for p in cr.files.all().values_list('project_identifier', flat=True).distinct('project_identifier')
        ]

        return Response(data=projects, status=status.HTTP_200_OK)

    # GET /rest/v2/datasets/{PID}/files/{FILE_PID}
    def files_retrieve(self, request, pk=None, file_pk=None):
        """
        Retrieve technical metadata of a single file associated with a dataset.
        """
        _logger.debug('Retrieving metadata of single file: %r' % file_pk)

        # note: checks permissions
        cr = self.get_object()

        if not cr.authorized_to_see_catalog_record_files(request):
            raise Http403(UNAUTHORIZED_TO_SEE_FILES_MSG)

        try:
            params = { 'pk': int(file_pk) }
        except ValueError:
            params = { 'identifier': file_pk }

        manager = 'objects'

        if CS.get_boolean_query_param(request, 'removed_files'):
            params['removed'] = True
            manager = 'objects_unfiltered'

        file_fields = []
        if 'file_fields' in request.query_params:
            file_fields = request.query_params['file_fields'].split(',')

        file_fields = LightFileSerializer.ls_field_list(file_fields)

        try:
            file = cr.files(manager=manager).filter(**params).values(*file_fields)[0]
        except:
            raise Http404

        file_serialized = LightFileSerializer.serialize(file)

        return Response(data=file_serialized, status=status.HTTP_200_OK)

    # GET /rest/v2/datasets/{PID}/files
    def files_list(self, request, pk=None):
        """
        Get technical metadata of all files associated with a dataset. Can be used to retrieve a list of only
        deleted files by providing the query parameter removed_files=true.

        NOTE! Take a look at api/rest/v2/router.py to see how this method is mapped to HTTP verb
        """
        params = {}
        manager = 'objects'
        # TODO: This applies only to IDA files, not remote resources.
        # TODO: Should this apply also to remote resources?
        cr = self.get_object()

        if not cr.authorized_to_see_catalog_record_files(request):
            raise Http403(UNAUTHORIZED_TO_SEE_FILES_MSG)

        if CS.get_boolean_query_param(request, 'removed_files'):
            params['removed'] = True
            manager = 'objects_unfiltered'

        file_fields = []
        if 'file_fields' in request.query_params:
            file_fields = request.query_params['file_fields'].split(',')

        file_fields = LightFileSerializer.ls_field_list(file_fields)
        queryset = cr.files(manager=manager).filter(**params).values(*file_fields)
        files = LightFileSerializer.serialize(queryset)

        return Response(data=files, status=status.HTTP_200_OK)

    # POST /rest/v2/datasets/{PID}/files
    def files_post(self, request, pk=None):
        """
        Change the files associated with a dataset (add and exclude files).

        NOTE! Take a look at api/rest/v2/router.py to see how this method is mapped to HTTP verb
        """
        if not request.data:
            raise Http400('No data received')

        # note: checks permissions
        cr = self.get_object()

        result = cr.change_files(request.data)

        return Response(data=result, status=status.HTTP_200_OK)

    # GET /rest/v2/datasets/{PID}/files/user_metadata
    def files_user_metadata_list(self, request, pk=None):
        """
        Get user-provided dataset-specific metadata associated with a dataset.
        """

        # note: checks permissions
        cr = self.get_object()

        if not cr.authorized_to_see_catalog_record_files(request):
            raise Http403(UNAUTHORIZED_TO_SEE_FILES_MSG)

        data = {}

        for object_type in ('files', 'directories'):
            if object_type in cr.research_dataset:
                data[object_type] = cr.research_dataset[object_type]

        return Response(data=data, status=status.HTTP_200_OK)

    # PUT/PATCH /rest/v2/datasets/{PID}/files/user_metadata
    def files_user_metadata_update(self, request, pk=None):
        """
        Change user-provided dataset-specific metadata associated with a dataset.

        This API does not add or remove files! Only updates metadata.
        """
        if not request.data:
            raise Http400('No data received')

        # note: checks permissions
        cr = self.get_object()

        cr.update_files_dataset_specific_metadata(request.data)

        return Response(data=None, status=status.HTTP_200_OK)

    @detail_route(methods=['get'], url_path="files/(?P<obj_identifier>.+)/user_metadata")
    def files_user_metadata_retrieve(self, request, pk=None, obj_identifier=None):
        """
        Retrieve user-provided dataset-specific metadata for a file or a directory associated with a dataset.
        """

        # note: checks permissions
        cr = self.get_object()

        if not cr.authorized_to_see_catalog_record_files(request):
            raise Http403(UNAUTHORIZED_TO_SEE_FILES_MSG)

        if CS.get_boolean_query_param(request, 'directory'):
            # search from directories only if special query parameter is given
            object_type = 'directories'
        else:
            object_type = 'files'

        for obj in cr.research_dataset.get(object_type, []):
            if obj['identifier'] == obj_identifier:
                return Response(data=obj, status=status.HTTP_200_OK)

        raise Http404
