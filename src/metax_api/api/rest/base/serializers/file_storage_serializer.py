# This file is part of the Metax API service
#
# Copyright 2017-2018 Ministry of Education and Culture, Finland
#
# :author: CSC - IT Center for Science Ltd., Espoo Finland <servicedesk@csc.fi>
# :license: MIT

from rest_framework.serializers import ValidationError

from metax_api.models import FileStorage

from .common_serializer import CommonSerializer
from .serializer_utils import validate_json


class FileStorageSerializer(CommonSerializer):
    class Meta:
        model = FileStorage

        fields = (
            "id",
            "file_storage_json",
        ) + CommonSerializer.Meta.fields

        extra_kwargs = CommonSerializer.Meta.extra_kwargs

    def validate_file_storage_json(self, value):
        validate_json(value, self.context["view"].json_schema)
        if self._operation_is_create:
            self._validate_identifier_uniqueness(value)
        return value

    def _validate_identifier_uniqueness(self, file_storage_json):
        if FileStorage.objects.filter(
            file_storage_json__identifier=file_storage_json["identifier"]
        ).exists():
            raise ValidationError(
                {"identifier": ["identifier %s already exists" % file_storage_json["identifier"]]}
            )
