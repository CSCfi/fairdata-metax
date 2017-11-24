from metax_api.models import XmlMetadata

from .common_serializer import CommonSerializer


class XmlMetadataSerializer(CommonSerializer):

    class Meta:
        model = XmlMetadata
        fields = (
            'id',
            'namespace',
            'xml',
            'file',
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
        res = super(XmlMetadataSerializer, self).to_representation(instance)
        res['file'] = {
            'identifier': self.instance.file.identifier
        }
        return res

    def validate_xml(self, value):
        # try:
        #     json_validate(value, self.context['view'].json_schema)
        # except JsonValidationError as e:
        #     raise ValidationError('%s. Json field: %s, schema: %s' % (e.message, e.path[0], e.schema))
        return value
