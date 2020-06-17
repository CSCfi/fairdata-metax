# This file is part of the Metax API service
#
# Copyright 2017-2018 Ministry of Education and Culture, Finland
#
# :author: CSC - IT Center for Science Ltd., Espoo Finland <servicedesk@csc.fi>
# :license: MIT

import logging

from django.http import Http404
from rest_framework import status
from rest_framework.decorators import action
from rest_framework.response import Response

from metax_api.api.rpc.base.views import DatasetRPC
from metax_api.api.rest.v2.serializers import CatalogRecordSerializerV2
from metax_api.exceptions import Http400, Http501
from metax_api.models import CatalogRecordV2


_logger = logging.getLogger(__name__)


class DatasetRPC(DatasetRPC):

    serializer_class = CatalogRecordSerializerV2
    object = CatalogRecordV2

    def get_object(self):
        """
        RPC api does not handle parameters the exact same way as REST api, so need to re-define get_object()
        to work properly.
        """
        if not self.request.query_params.get('identifier', False):
            raise Http400('Query param \'identifier\' missing. Please specify ?identifier=<catalog record identifier>')

        params = {}

        try:
            params['pk'] = int(self.request.query_params['identifier'])
        except ValueError:
            params['identifier'] = self.request.query_params['identifier']

        try:
            cr = self.object.objects.get(**params)
        except self.object.DoesNotExist:
            raise Http404

        self.check_object_permissions(self.request, cr)

        cr.request = self.request

        return cr

    @action(detail=False, methods=['post'], url_path="change_cumulative_state")
    def change_cumulative_state(self, request):

        state_value = request.query_params.get('cumulative_state', False)

        if not state_value:
            raise Http400('Query param \'cumulative_state\' missing')

        cr = self.get_object()

        cr.request = request

        cr.change_cumulative_state(state_value)

        return Response(status=status.HTTP_204_NO_CONTENT)

    @action(detail=False, methods=['post'], url_path="create_draft")
    def create_draft(self, request):

        cr = self.get_object()

        cr.create_draft()

        return Response(
            data={
                'id': cr.next_draft.id,
                'identifier': cr.next_draft.identifier
            },
            status=status.HTTP_201_CREATED
        )

    @action(detail=False, methods=['post'], url_path="create_new_version")
    def create_new_version(self, request):

        cr = self.get_object()

        cr.create_new_version()

        return Response(
            data={
                'id': cr.next_dataset_version.id,
                'identifier': cr.next_dataset_version.identifier
            },
            status=status.HTTP_201_CREATED
        )

    @action(detail=False, methods=['post'], url_path="publish_dataset")
    def publish_dataset(self, request):

        cr = self.get_object()

        cr.publish_dataset()

        return Response(
            data={ 'preferred_identifier': cr.preferred_identifier },
            status=status.HTTP_200_OK
        )

    @action(detail=False, methods=['post'], url_path="merge_draft")
    def merge_draft(self, request):

        cr = self.get_object()

        # on successful publish, the record behind the variable 'cr' should now be deleted.
        # the identifier points to the original published dataset.
        identifier_of_published = cr.merge_draft()

        return Response(
            data={ 'identifier': identifier_of_published },
            status=status.HTTP_200_OK
        )

    # ensure some v1 api endpoints cant be called in v2 api

    @action(detail=False, methods=['post'], url_path="refresh_directory_content")
    def refresh_directory_content(self, request):
        raise Http501()

    @action(detail=False, methods=['post'], url_path="fix_deprecated")
    def fix_deprecated(self, request):
        raise Http501()
