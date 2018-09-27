# This file is part of the Metax API service
#
# Copyright 2017-2018 Ministry of Education and Culture, Finland
#
# :author: CSC - IT Center for Science Ltd., Espoo Finland <servicedesk@csc.fi>
# :license: MIT

from json import load

from django.conf import settings as django_settings
from rest_framework.decorators import list_route
from rest_framework.response import Response

from metax_api.exceptions import Http400
from .common_rpc import CommonRPC


class DatasetRPC(CommonRPC):

    @list_route(methods=['get'], url_path="get_minimal_dataset_template")
    def get_minimal_dataset_template(self, request):
        if request.query_params.get('type', None) not in ['service', 'enduser']:
            raise Http400({
                'detail': ['query param \'type\' missing or wrong. please specify ?type= as one of: service, enduser']
            })

        with open('metax_api/exampledata/dataset_minimal.json', 'rb') as f:
            example_ds = load(f)

        example_ds['data_catalog'] = django_settings.END_USER_ALLOWED_DATA_CATALOGS[0]

        if request.query_params['type'] == 'enduser':
            example_ds.pop('metadata_provider_org', None)
            example_ds.pop('metadata_provider_user', None)

        return Response(example_ds)
