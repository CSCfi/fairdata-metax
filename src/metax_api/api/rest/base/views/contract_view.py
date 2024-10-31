# This file is part of the Metax API service
#
# Copyright 2017-2018 Ministry of Education and Culture, Finland
#
# :author: CSC - IT Center for Science Ltd., Espoo Finland <servicedesk@csc.fi>
# :license: MIT

from rest_framework import status
from rest_framework.decorators import action
from rest_framework.response import Response
from rest_framework.serializers import ValidationError

from metax_api.exceptions import Http400
from metax_api.models import Contract
from metax_api.services import CommonService

from ..serializers import CatalogRecordSerializer, ContractSerializer
from .common_view import CommonViewSet


class ContractViewSet(CommonViewSet):

    serializer_class = ContractSerializer
    object = Contract

    lookup_field = "pk"

    def __init__(self, *args, **kwargs):
        self.set_json_schema(__file__)
        super(ContractViewSet, self).__init__(*args, **kwargs)

    def get_object(self):
        lookup_value = self.kwargs.get(self.lookup_field, False)
        search_params = None
        if not CommonService.is_primary_key(lookup_value):
            search_params = {"contract_json__contains": {"identifier": lookup_value}}
        return super(ContractViewSet, self).get_object(search_params=search_params)

    def get_queryset(self):
        if not self.request.query_params:
            return super(ContractViewSet, self).get_queryset()
        else:
            query_params = self.request.query_params
            additional_filters = {}
            if query_params.get("organization", False):
                additional_filters["contract_json__contains"] = {
                    "organization": {"organization_identifier": query_params["organization"]}
                }
            return super(ContractViewSet, self).get_queryset().filter(**additional_filters)

    @action(detail=True, methods=["get"], url_path="datasets")
    def datasets_get(self, request, pk=None):
        contract = self.get_object()
        catalog_records = [CatalogRecordSerializer(f).data for f in contract.records.all()]
        return Response(data=catalog_records, status=status.HTTP_200_OK)

    @action(detail=False, methods=["post"], url_path="sync_from_v3")
    def sync_from_v3(self, request, pk=None):
        """Endpoint for synchronizing contract updates from V3.

        Creates, updates or removes contracts."""
        from metax_api.api.rest.base.serializers.contract_sync_serializer import (
            ContractSyncFromV3Serializer,
        )

        if not request.user.is_metax_v3:
            raise Http400("Endpoint is supported only for metax_service user")

        serializer = ContractSyncFromV3Serializer(
            data=request.data, context={"request": request, "view": self}, many=True
        )
        try:
            serializer.is_valid(raise_exception=True)
            serializer.save()
        except ValidationError as err:
            # Common error handling does not like lists, so take
            # first non-empty error value from list
            if isinstance(err.detail, list):
                err.detail = next((v for v in err.detail if v), None)
            raise err
        return Response(data=serializer.data, status=status.HTTP_200_OK)
