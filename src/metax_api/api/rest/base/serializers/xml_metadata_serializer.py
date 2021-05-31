# This file is part of the Metax API service
#
# Copyright 2017-2018 Ministry of Education and Culture, Finland
#
# :author: CSC - IT Center for Science Ltd., Espoo Finland <servicedesk@csc.fi>
# :license: MIT

from metax_api.models import XmlMetadata

from .common_serializer import CommonSerializer


class XmlMetadataSerializer(CommonSerializer):
    class Meta:
        model = XmlMetadata
        fields = (
            "id",
            "namespace",
            "xml",
            "file",
        ) + CommonSerializer.Meta.fields

        extra_kwargs = CommonSerializer.Meta.extra_kwargs

    def to_representation(self, instance):
        res = super(XmlMetadataSerializer, self).to_representation(instance)
        res["file"] = {"identifier": self.instance.file.identifier}
        return res

    def validate_xml(self, value):
        # try:
        #     json_validate(value, self.context['view'].json_schema)
        # except JsonValidationError as e:
        #     raise ValidationError('%s. Json field: %s, schema: %s' % (e.message, e.path[0], e.schema))
        return value
