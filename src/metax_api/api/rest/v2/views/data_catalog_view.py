# This file is part of the Metax API service
#
# Copyright 2017-2018 Ministry of Education and Culture, Finland
#
# :author: CSC - IT Center for Science Ltd., Espoo Finland <servicedesk@csc.fi>
# :license: MIT

import logging

from metax_api.api.rest.base.views import DataCatalogViewSet as DCVS
from ..serializers import DataCatalogSerializerV2

_logger = logging.getLogger(__name__)


class DataCatalogViewSet(DCVS):
    """
    This allows separation in data catalog schemas between API versions.
    All the other functions are inherited from V1.
    """

    serializer_class = DataCatalogSerializerV2

    def __init__(self, *args, **kwargs):
        self.set_json_schema(__file__)
        super(DCVS, self).__init__(*args, **kwargs)
