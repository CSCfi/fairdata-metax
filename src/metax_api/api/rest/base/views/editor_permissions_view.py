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

from ..serializers import EditorPermissionsSerializer
from .common_view import CommonViewSet

_logger = logging.getLogger(__name__)


class EditorPermissionViewSet(CommonViewSet):

    def __init__(self, *args, **kwargs):
        # As opposed to other views, do not set json schema here
        # It is done in the serializer
        super(EditorPermissionViewSet, self).__init__(*args, **kwargs)

    def list(self, request, *args, **kwargs):
        cr = get_object_or_404(CatalogRecord, pk=kwargs['cr_identifier'])
        if 'user_id' in kwargs:
            try:
                user = cr.editor_permissions.users.get(user_id=kwargs['user_id'])
            except Exception as exc:
                return Response({'error': 'Unknown user'}, status=status.HTTP_400_BAD_REQUEST)
            editorserializer = EditorPermissionsSerializer(user)
        else:
            editorserializer = EditorPermissionsSerializer(cr.editor_permissions.users.all(), many=True)
        return Response(editorserializer.data)

    def create(self, request, *args, **kwargs):
        data = request.data
        cr = get_object_or_404(CatalogRecord, pk=kwargs['cr_identifier'])
        if 'user_id' in data:
            editorserializer = None
            removed_user = EditorUserPermission.objects_unfiltered.filter(user_id=data.get('user_id'),
                                                                          editor_permissions_id=cr.editor_permissions_id).first()
            data['verified'] = False
            if removed_user not in EMPTY_VALUES and removed_user.removed is True:
                data['verification_token'] = None
                data['date_modified'] = datetime.datetime.now()
                data['date_removed'] = None
                data['removed'] = False
                editorserializer = EditorPermissionsSerializer(removed_user, data=data, partial=True)
            elif removed_user not in EMPTY_VALUES and removed_user.removed is False:
                return Response({'user_id': "User_id already exists"}, status=status.HTTP_400_BAD_REQUEST)
            else:
                data['editor_permissions'] = cr.editor_permissions.pk
                data['date_created'] = datetime.datetime.now()
                editorserializer = EditorPermissionsSerializer(data=data)
            if editorserializer.is_valid():
                editorserializer.save()
                return Response(editorserializer.data, status=status.HTTP_201_CREATED)
            else:
                return Response(editorserializer.errors, status=status.HTTP_400_BAD_REQUEST)
        else:
            return Response({'user_id': 'Missing user_id'}, status=status.HTTP_400_BAD_REQUEST)

    def destroy_bulk(self, request, *args, **kwargs):
        cr = get_object_or_404(CatalogRecord, pk=kwargs['cr_identifier'])
        try:
            user = cr.editor_permissions.users.get(user_id=kwargs['user_id'])
        except Exception as exc:
            return Response({'error': 'Unknown user'}, status=status.HTTP_400_BAD_REQUEST)

        creators = cr.editor_permissions.users.filter(role=PermissionRole.CREATOR, removed=False).count()
        if user.role == PermissionRole.CREATOR and creators < 2:
            return Response({"error": "Can't delete last creator"}, status=status.HTTP_400_BAD_REQUEST)
        else:
            user.remove()
        return Response(status=status.HTTP_200_OK)

    def partial_update_bulk(self, request, **kwargs):
        data = request.data
        cr = get_object_or_404(CatalogRecord, pk=kwargs['cr_identifier'])
        try:
            user = cr.editor_permissions.users.get(user_id=kwargs['user_id'])
        except Exception as exc:
            return Response({'error': 'Unknown user'}, status=status.HTTP_400_BAD_REQUEST)
        data['date_modified'] = datetime.datetime.now()
        serializer = EditorPermissionsSerializer(user, data=data, partial=True)
        if serializer.is_valid():
            serializer.save()
            return Response(serializer.data)
        else:
            return Response(serializer.errors, status=status.HTTP_400_BAD_REQUEST)
