from metax_api.models import Dataset
from .common_view import CommonViewSet
from ..serializers import DatasetReadSerializer

import logging
_logger = logging.getLogger(__name__)
d = logging.getLogger(__name__).debug

class DatasetViewSet(CommonViewSet):

    authentication_classes = ()
    permission_classes = ()

    # note: override get_queryset() to get more control
    queryset = Dataset.objects.all()
    serializer_class = DatasetReadSerializer
    object = Dataset

    lookup_field = 'pk'

    # allow search by external identifier (urn, or whatever string in practice) as well
    lookup_field_other = 'identifier'

    def __init__(self, *args, **kwargs):
        self.set_json_schema(__file__)
        super(DatasetViewSet, self).__init__(*args, **kwargs)

    def get_object(self):
        """
        todo:
        - look also by json field otherIdentifier, if no match is found?
        """
        return super(DatasetViewSet, self).get_object()
