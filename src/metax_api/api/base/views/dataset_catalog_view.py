from metax_api.models import DatasetCatalog
from .common_view import CommonViewSet
from ..serializers import DatasetCatalogReadSerializer

import logging
_logger = logging.getLogger(__name__)
d = logging.getLogger(__name__).debug

class DatasetCatalogViewSet(CommonViewSet):

    authentication_classes = ()
    permission_classes = ()

    # note: override get_queryset() to get more control
    queryset = DatasetCatalog.objects.all()
    serializer_class = DatasetCatalogReadSerializer
    object = DatasetCatalog

    lookup_field = 'pk'

    # allow search by external identifier (urn, or whatever string in practice) as well
    lookup_field_other = 'identifier'

    def __init__(self, *args, **kwargs):
        self.set_json_schema(__file__)
        super(DatasetCatalogViewSet, self).__init__(*args, **kwargs)
