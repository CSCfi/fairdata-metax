# This file is part of the Metax API service
#
# Copyright 2017-2018 Ministry of Education and Culture, Finland
#
# :author: CSC - IT Center for Science Ltd., Espoo Finland <servicedesk@csc.fi>
# :license: MIT

import logging

from django.conf import settings as django_settings
from rest_framework import status
from rest_framework.decorators import action
from rest_framework.response import Response

from .common_rpc import CommonRPC
from metax_api.exceptions import Http400
from metax_api.utils.reference_data_loader import ReferenceDataLoader as RDL

_logger = logging.getLogger(__name__)


class ElasticsearchRPC(CommonRPC):

    @action(detail=False, methods=['get'], url_path="map_refdata")
    def map_refdata(self, request):

        if not isinstance(django_settings, dict):
            settings = django_settings.ELASTICSEARCH

        connection_params = RDL.get_connection_parameters(settings)
        # returns scan object as well but that is not needed here
        esclient = RDL.get_es_imports(settings['HOSTS'], connection_params)[0]

        if not self.request.query_params:
            return Response(status=status.HTTP_200_OK)

        elif '_mapping' in self.request.query_params:
            try:
                res = esclient.indices.get_mapping()
            except Exception as e:
                raise Http400(f'Error when accessing elasticsearch. {e}')

            return Response(data=res, status=status.HTTP_200_OK)

        params = {}
        for k, v in self.request.query_params.items():
            # python dict.items() keeps order so the url is always the first one
            if '_data/' in k:
                splitted = k.split('/')

                if len(splitted) < 3:
                    _logger.info('Bad url to elasticsearch proxy')
                    return Response(data=None, status=status.HTTP_204_NO_CONTENT)

                idx = splitted[0]
                type = splitted[1]

                if '?' in splitted[2]:
                    action, first_param = splitted[2].split('?')
                else:
                    action, first_param = splitted[2], None

                if action != '_search':
                    _logger.info('Bad action to elasticsearch proxy')
                    return Response(data=None, status=status.HTTP_204_NO_CONTENT)

                if first_param:
                    params[first_param] = v

            elif k == 'pretty':
                params[k] = 'true' if v else 'false'

            elif k == 'q':
                try:
                    # new ES client separates filters with a space
                    v = v.replace('+AND+', ' AND ')
                    v = v.replace('+OR+', ' OR ')
                    if 'type:' not in v:
                        params[k] = v + f' AND type:{type}'
                    else:
                        params[k] = v
                except:
                    _logger.info('Elasticsearch proxy has missing type. This should not happen')
                    return Response(data=None, status=status.HTTP_204_NO_CONTENT)

            else:
                params[k] = v

        if 'q' not in params:
            params['q'] = f'type:{type}'

        try:
            res = esclient.search(index=idx, params=params)
        except Exception as e:
            raise Http400(f'Error when accessing elasticsearch. {e}')

        return Response(data=res, status=status.HTTP_200_OK)