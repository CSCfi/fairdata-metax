# This file is part of the Metax API service
#
# Copyright 2017-2018 Ministry of Education and Culture, Finland
#
# :author: CSC - IT Center for Science Ltd., Espoo Finland <servicedesk@csc.fi>
# :license: MIT

import logging

from rest_framework.decorators import list_route
from rest_framework.response import Response

from .common_rpc import CommonRPC
from metax_api.exceptions import Http400
from metax_api.services import StatisticService, CommonService as CS


_logger = logging.getLogger(__name__)


class StatisticRPC(CommonRPC):

    @list_route(methods=['get'], url_path='all_datasets_cumulative')
    def all_datasets_cumulative(self, request):
        if not request.query_params.get('from_date', None) or not request.query_params.get('to_date', None):
            raise Http400('from_date and to_date parameters are required')

        params = {
            'from_date': request.query_params.get('from_date', None),
            'to_date':   request.query_params.get('to_date', None),
        }
        return Response(StatisticService.total_datasets(**params))

    @list_route(methods=['get'], url_path='catalog_datasets_cumulative')
    def catalog_datasets_cumulative(self, request):
        if not request.query_params.get('from_date', None) or not request.query_params.get('to_date', None):
            raise Http400('from_date and to_date parameters are required')

        params = {
            'from_date':    request.query_params.get('from_date', None),
            'to_date':      request.query_params.get('to_date', None),
            'data_catalog': request.query_params.get('data_catalog', None),
        }
        return Response(StatisticService.total_data_catalog_datasets(**params))

    @list_route(methods=['get'], url_path='count_datasets')
    def count_datasets(self, request):
        str_params = [
            'from_date',
            'to_date',
            'access_type',
            'data_catalog',
            'metadata_owner_org',
            'metadata_provider_org',
            'metadata_provider_user',
            'preservation_state'
        ]

        params = { param: request.query_params.get(param, None) for param in str_params }

        for boolean_param in ['deprecated', 'latest', 'harvested']:
            if boolean_param in request.query_params:
                params[boolean_param] = CS.get_boolean_query_param(request, boolean_param)

        return Response(StatisticService.count_datasets(**params))

    @list_route(methods=['get'], url_path='deprecated_datasets_cumulative')
    def deprecated_datasets_cumulative(self, request):
        if not request.query_params.get('from_date', None) or not request.query_params.get('to_date', None):
            raise Http400('from_date and to_date parameters are required')

        params = {
            'from_date': request.query_params.get('from_date', None),
            'to_date':   request.query_params.get('to_date', None),
        }
        return Response(StatisticService.deprecated_datasets_cumulative(**params))

    @list_route(methods=['get'], url_path='end_user_datasets_cumulative')
    def end_user_datasets_cumulative(self, request):
        if not request.query_params.get('from_date', None) or not request.query_params.get('to_date', None):
            raise Http400('from_date and to_date parameters are required')

        params = {
            'from_date': request.query_params.get('from_date', None),
            'to_date':   request.query_params.get('to_date', None),
        }
        return Response(StatisticService.total_end_user_datasets(**params))

    @list_route(methods=['get'], url_path='harvested_datasets_cumulative')
    def harvested_datasets_cumulative(self, request):
        if not request.query_params.get('from_date', None) or not request.query_params.get('to_date', None):
            raise Http400('from_date and to_date parameters are required')

        params = {
            'from_date': request.query_params.get('from_date', None),
            'to_date':   request.query_params.get('to_date', None),
        }
        return Response(StatisticService.total_harvested_datasets(**params))

    @list_route(methods=['get'], url_path='organization_datasets_cumulative')
    def organization_datasets_cumulative(self, request):
        if not request.query_params.get('from_date', None) or not request.query_params.get('to_date', None):
            raise Http400('from_date and to_date parameters are required')

        params = {
            'from_date':          request.query_params.get('from_date', None),
            'to_date':            request.query_params.get('to_date', None),
            'metadata_owner_org': request.query_params.get('metadata_owner_org', None),
        }
        return Response(StatisticService.total_organization_datasets(**params))

    @list_route(methods=['get'], url_path='unused_files')
    def unused_files(self, request):
        return Response(StatisticService.unused_files())
