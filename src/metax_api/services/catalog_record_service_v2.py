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
            # note: request.query_params is a @property of the request object. can not be directly edited.
            # see rest_framework/request.py
            request._request.GET._mutable = True
            request.query_params.pop('state', None)
            request._request.GET._mutable = False

        queryset_search_params = super().get_queryset_search_params(request)

        if state:
            queryset_search_params['state'] = state

        return queryset_search_params
