# This file is part of the Metax API service
#
# Copyright 2017-2018 Ministry of Education and Culture, Finland
#
# :author: CSC - IT Center for Science Ltd., Espoo Finland <servicedesk@csc.fi>
# :license: MIT

import logging

# from django.db import connection

# from metax_api.exceptions import Http400


_logger = logging.getLogger(__name__)


class StatisticService():

    @staticmethod
    def something():
        _logger.info('Something something')

        # sql = 'select * from metax_api_catalogrecord'

        # with connection.cursor() as cr:
        #     cr.execute(sql)
        #     # if cr.rowcount == 0:
        #     #     # stuff
        #     #     pass
        #     results = [ row[0] for row in cr.fetchall() ]

        return {}
