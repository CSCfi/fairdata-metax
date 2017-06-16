from collections import OrderedDict

from rest_framework.fields import SkipField
from rest_framework.relations import PKOnlyObject

from metax_api.models import CatalogRecord
from .catalog_record_serializer import CatalogRecordSerializer

import logging
_logger = logging.getLogger(__name__)
d = logging.getLogger(__name__).debug

class DatasetSerializer(CatalogRecordSerializer):

    class Meta:
        model = CatalogRecord
        fields = (
            'id',
            'identifier',
            'research_dataset',
            'modified_by_user_id',
            'modified_by_api',
            'created_by_user_id',
            'created_by_api',
        )
        extra_kwargs = {
            # not required during creation, or updating
            # they would be overwritten by the api anyway
            'modified_by_user_id': { 'required': False },
            'modified_by_api': { 'required': False },
            'created_by_user_id': { 'required': False },
            'created_by_api': { 'required': False },
        }

    def to_representation(self, instance):
        """
        copy-pasta from REST framework to_representation method, because we want to skip the direct
        parent method
        """
        ret = OrderedDict()

        for field in self._readable_fields:
            try:
                attribute = field.get_attribute(instance)
            except SkipField:
                continue

            check_for_none = attribute.pk if isinstance(attribute, PKOnlyObject) else attribute
            if check_for_none is None:
                ret[field.field_name] = None
            else:
                ret[field.field_name] = field.to_representation(attribute)

        return ret
