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
from metax_api.services import StatisticService


_logger = logging.getLogger(__name__)


class StatisticRPC(CommonRPC):

    @list_route(methods=['get'], url_path='something')
    def something(self, request):
        return Response(StatisticService.something())
