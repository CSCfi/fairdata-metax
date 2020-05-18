# This file is part of the Metax API service
#
# Copyright 2017-2018 Ministry of Education and Culture, Finland
#
# :author: CSC - IT Center for Science Ltd., Espoo Finland <servicedesk@csc.fi>
# :license: MIT
import logging

from .catalog_record_service import CatalogRecordService


_logger = logging.getLogger(__name__)


class CatalogRecordServiceV2(CatalogRecordService):

    @classmethod
    def get_queryset_search_params(cls, request):
        """
        v1 API has query params state and preservation_state doing the same thing. v2 API will properly
        use state to filter by field state.
        """
        state = request.query_params.get('state', None)

        if state:
            # request.query_params is an immutable QueryDict object. replace request.query_params
            # with a new, ordinary dict where state is missing
            query_params = { k: v for k, v in request.query_params if k != 'state' }
            request.query_params = query_params

        queryset_search_params = super().get_queryset_search_params(request)

        if state:
            queryset_search_params['state'] = state

        return queryset_search_params
