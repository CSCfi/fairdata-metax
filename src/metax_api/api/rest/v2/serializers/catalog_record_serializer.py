# This file is part of the Metax API service
#
# Copyright 2017-2018 Ministry of Education and Culture, Finland
#
# :author: CSC - IT Center for Science Ltd., Espoo Finland <servicedesk@csc.fi>
# :license: MIT

import logging

from metax_api.api.rest.base.serializers import CatalogRecordSerializer
from metax_api.models import CatalogRecordV2
from metax_api.services import CatalogRecordService as CRS, CommonService as CS


_logger = logging.getLogger(__name__)


class CatalogRecordSerializer(CatalogRecordSerializer):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.Meta.model = CatalogRecordV2

    def to_representation(self, instance):

        res = super().to_representation(instance)

        if 'request' in self.context:

            if CS.get_boolean_query_param(self.context['request'], 'include_user_metadata'):
                if CS.get_boolean_query_param(self.context['request'], 'file_details'):
                    CRS.populate_file_details(res, self.context['request'])
            else:
                res['research_dataset'].pop('files', None)
                res['research_dataset'].pop('directories', None)

        return res
