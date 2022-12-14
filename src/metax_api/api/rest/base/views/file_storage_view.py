# This file is part of the Metax API service
#
# Copyright 2017-2018 Ministry of Education and Culture, Finland
#
# :author: CSC - IT Center for Science Ltd., Espoo Finland <servicedesk@csc.fi>
# :license: MIT

from os import path

from django.http import Http404

from metax_api.models import FileStorage
from metax_api.services import CommonService as CS

from ..serializers import FileStorageSerializer
from .common_view import CommonViewSet


class FileStorageViewSet(CommonViewSet):

    serializer_class = FileStorageSerializer
    object = FileStorage
    lookup_field = "pk"

    def __init__(self, *args, **kwargs):
        self.set_json_schema(__file__)
        super(FileStorageViewSet, self).__init__(*args, **kwargs)

    def get_object(self):
        try:
            return super(FileStorageViewSet, self).get_object()
        except Http404:
            pass

        # was not a pk - try identifier
        lookup_value = self.kwargs.pop(self.lookup_field)
        return super(FileStorageViewSet, self).get_object(
            search_params={"file_storage_json__contains": {"identifier": lookup_value}}
        )

    def set_json_schema(self, view_file):
        self.json_schema = CS.get_json_schema(path.dirname(view_file) + "/../schemas", "file")
