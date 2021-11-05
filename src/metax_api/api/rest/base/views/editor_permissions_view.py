# This file is part of the Metax API service
#
# Copyright 2017-2018 Ministry of Education and Culture, Finland
#
# :author: CSC - IT Center for Science Ltd., Espoo Finland <servicedesk@csc.fi>
# :license: MIT
import datetime
import logging

from django.core.validators import EMPTY_VALUES

from django.shortcuts import get_object_or_404
from rest_framework import status

from rest_framework.response import Response

from metax_api.models import CatalogRecord
from metax_api.models.catalog_record import PermissionRole, EditorUserPermission
from metax_api.services import CommonService

from ..serializers import EditorPermissionsSerializer
from .common_view import CommonViewSet

_logger = logging.getLogger(__name__)


class EditorPermissionViewSet(CommonViewSet):
    lookup_field = "user_id"

    serializer_class = EditorPermissionsSerializer

    def __init__(self, *args, **kwargs):
        super(EditorPermissionViewSet, self).__init__(*args, **kwargs)

    def get_queryset(self):
        if CommonService.is_primary_key(self.kwargs['cr_identifier']):
            cr = get_object_or_404(CatalogRecord, pk=int(self.kwargs['cr_identifier']))
        else:
            cr = get_object_or_404(CatalogRecord, identifier=self.kwargs['cr_identifier'])
        return cr.editor_permissions.users

    def list(self, request, *args, **kwargs):
        users = self.get_queryset()
        editorserializer = EditorPermissionsSerializer(users.all(), many=True)
        return Response(editorserializer.data)

    def create(self, request, *args, **kwargs):
        data = request.data

        perms_id = self.get_queryset().instance.id
        if "user_id" not in data:
            return Response({"user_id": "Missing user_id"}, status=status.HTTP_400_BAD_REQUEST)

        editorserializer = None
        removed_user = EditorUserPermission.objects_unfiltered.filter(
            user_id=data.get('user_id'), editor_permissions_id=perms_id).first()
        data['verified'] = False
        if removed_user not in EMPTY_VALUES and removed_user.removed is True:
            data['verification_token'] = None
            data['date_modified'] = datetime.datetime.now()
            data['date_removed'] = None
            data['removed'] = False
            editorserializer = EditorPermissionsSerializer(removed_user, data=data, partial=True)
        elif removed_user not in EMPTY_VALUES and removed_user.removed is False:
            return Response(
                {'user_id': "User_id already exists"}, status=status.HTTP_400_BAD_REQUEST
            )
        else:
            data['editor_permissions'] = perms_id
            data['date_created'] = datetime.datetime.now()
            editorserializer = EditorPermissionsSerializer(data=data)
        if editorserializer.is_valid():
            editorserializer.save()
            return Response(editorserializer.data, status=status.HTTP_201_CREATED)
        else:
            return Response(editorserializer.errors, status=status.HTTP_400_BAD_REQUEST)

    def destroy(self, request, *args, **kwargs):
        user = self.get_object()
        users = self.get_queryset()

        creators = users.filter(role=PermissionRole.CREATOR, removed=False).count()
        if user.role == PermissionRole.CREATOR and creators < 2:
            return Response(
                {"error": "Can't delete last creator"}, status=status.HTTP_400_BAD_REQUEST
            )
        else:
            user.remove()
        return Response(status=status.HTTP_200_OK)

    def partial_update(self, request, **kwargs):
        data = request.data
        user = self.get_object()
        users = self.get_queryset()

        if 'role' in data and user.role == PermissionRole.CREATOR:
            creators = users.filter(role=PermissionRole.CREATOR, removed=False).count()
            if creators < 2 and data.get('role') != PermissionRole.CREATOR:
                return Response({"error": "Can't change last creator"}, status=status.HTTP_400_BAD_REQUEST)

        data['date_modified'] = datetime.datetime.now()
        serializer = EditorPermissionsSerializer(user, data=data, partial=True)
        if serializer.is_valid():
            serializer.save()
            return Response(serializer.data)
        else:
            return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)

    def update_bulk(self, request, *args, **kwargs):
        return Response({}, status=status.HTTP_405_METHOD_NOT_ALLOWED)

    def partial_update_bulk(self, request, *args, **kwargs):
        return Response({}, status=status.HTTP_405_METHOD_NOT_ALLOWED)

    def destroy_bulk(self, request, *args, **kwargs):
        return Response({}, status=status.HTTP_405_METHOD_NOT_ALLOWED)

