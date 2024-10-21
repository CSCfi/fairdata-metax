# This file is part of the Metax API service
#
# Copyright 2017-2024 Ministry of Education and Culture, Finland
#
# :author: CSC - IT Center for Science Ltd., Espoo Finland <servicedesk@csc.fi>
# :license: MIT
import logging
from typing import List

from django.conf import settings


from metax_api.exceptions import Http503
from metax_api.models import Contract

from .common_service import CommonService
from .reference_data_mixin import ReferenceDataMixin

DEBUG = settings.DEBUG
_logger = logging.getLogger(__name__)


class ContractService(CommonService, ReferenceDataMixin):

    @classmethod
    def sync_to_v3_from_identifier_list(cls, data: list):
        if not settings.METAX_V3["INTEGRATION_ENABLED"]:
            return
        ids = cls.identifiers_to_ids(data, "noparams")
        files = Contract.objects.filter(id__in=ids)
        cls.sync_to_v3(files)

    @classmethod
    def sync_to_v3(cls, contracts: List[Contract]):
        if not settings.METAX_V3["INTEGRATION_ENABLED"] or not contracts:
            return

        from metax_api.services.metax_v3_service import MetaxV3Service, MetaxV3UnavailableError
        from metax_api.api.rest.base.serializers import ContractSerializer

        v3_service = MetaxV3Service()
        serializer = ContractSerializer()
        try:
            _logger.info(f"Syncing {len(contracts)} contracts to V3")
            contracts_json = [serializer.to_representation(contract) for contract in contracts]
            v3_service.sync_contracts(contracts_json)
        except MetaxV3UnavailableError:
            raise Http503({"detail": ["Metax V3 temporarily unavailable, please try again later."]})

    @classmethod
    def create_bulk(cls, request, serializer_class, **kwargs):
        return super().create_bulk(
            request, serializer_class, post_create_callback=cls.sync_to_v3, **kwargs
        )
