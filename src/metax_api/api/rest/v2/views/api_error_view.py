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

from metax_api.api.rest.base.views import CommonViewSet
from metax_api.exceptions import Http403, Http501
from metax_api.models import ApiError
from metax_api.permissions import ServicePermissions

from ..serializers.api_error_serializer import ApiErrorSerializerV2

_logger = logging.getLogger(__name__)

class ApiErrorViewSetV2(CommonViewSet):

    permission_classes = [ServicePermissions]
    queryset = ApiError.objects.all()
    serializer_class = ApiErrorSerializerV2

    def initial(self, request, *args, **kwargs):
        if request.user.username != "metax":
            raise Http403
        return super().initial(request, *args, **kwargs)

    @action(detail=False, methods=["post"], url_path="flush")
    def flush_errors(self, request):
        """
        Delete all errors from database.
        """
        _logger.info("%s called by %s" % (request.META["PATH_INFO"], request.user.username))
        errors = ApiError.objects.all()
        errors_deleted_count = len(errors)
        errors.delete()
        return Response(data={"errors_deleted": errors_deleted_count}, status=200)

    def destroy(self, request, *args, **kwargs):
        """
        Delete a single error from database.
        """
        _logger.info("DELETE %s called by %s" % (request.META["PATH_INFO"], request.user.username))
        try:
            error = ApiError.objects.get(pk=kwargs["pk"])
            error.delete()
            return Response(status=204)
        except:
            pass
        try:
            error = ApiError.objects.get(identifier=kwargs["pk"])
            error.delete()
            return Response(status=204)
        except:
            raise Http404

    def retrieve(self, request, *args, **kwargs):
        """
        Retrieve complete data about a single error.
        """
        try:
            error_details = ApiError.objects.get(pk=kwargs["pk"])
            serializer = ApiErrorSerializerV2(error_details)
            return Response(data=serializer.data, status=200)
        except:
            pass
        try:
            error_details = ApiError.objects.get(identifier=kwargs["pk"])
            serializer = ApiErrorSerializerV2(error_details)
            return Response(data=serializer.data, status=200)
        except:
            raise Http404

    def list(self, request, *args, **kwargs):
        """
        List all errors. Data is cleaned up a bit for easier browsing.
        """
        errors = ApiError.objects.all()
        error_list = []
        for error in errors:
            error_details = error.error
            error_details.pop("data", None)
            error_details.pop("headers", None)
            if error_details.get("response") is not None:
                if len(str(error_details["response"])) > 200:
                    print(error_details["response"])
                    error_details["response"] = (
                        "%s ...(first 200 characters)" % str(error_details["response"])[:200]
                    )
                    print(error_details["response"])
            if error_details.get("traceback") is not None:
                error_details["traceback"] = (
                    "(last 200 characters) ...%s" % error_details["traceback"][-200:]
                )
            error_list.append(error)
        serializer = ApiErrorSerializerV2(error_list, many=True)
        return Response(data=serializer.data, status=status.HTTP_200_OK)

    def update(self, request, *args, **kwargs):
        raise Http501()

    def update_bulk(self, request, *args, **kwargs):
        raise Http501()

    def partial_update(self, request, *args, **kwargs):
        raise Http501()

    def partial_update_bulk(self, request, *args, **kwargs):
        raise Http501()

    def create(self, request, *args, **kwargs):
        raise Http501()