# This file is part of the Metax API service
#
# Copyright 2017-2018 Ministry of Education and Culture, Finland
#
# :author: CSC - IT Center for Science Ltd., Espoo Finland <servicedesk@csc.fi>
# :license: MIT

import logging
from copy import deepcopy
from os import path

from jsonschema import Draft4Validator, RefResolver
from jsonschema.exceptions import ValidationError as JsonValidationError
from rest_framework.serializers import ValidationError

from metax_api.api.rest.base.serializers import CatalogRecordSerializer
from metax_api.api.rest.base.serializers.catalog_record_serializer import DFT_CATALOG
from metax_api.models import CatalogRecordV2
from metax_api.services import (
    CatalogRecordService as CRS,
    CommonService as CS,
    RedisCacheService as cache,
)

_logger = logging.getLogger(__name__)


class CatalogRecordSerializerV2(CatalogRecordSerializer):
    class Meta:
        # deepcopied, so that changes in this model don't affect
        # the V1 model
        fields = deepcopy(CatalogRecordSerializer.Meta.fields)
        extra_kwargs = deepcopy(CatalogRecordSerializer.Meta.extra_kwargs)

    # define separately for inherited class, so that schemas are searched
    # from api/rest/v2/schemas, instead of api/rest/v1/schemas
    _schemas_directory_path = path.join(path.dirname(path.dirname(__file__)), "schemas")

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.Meta.model = CatalogRecordV2
        self.Meta.fields += (
            "draft_of",
            "next_draft",
        )
        self.Meta.extra_kwargs.update(
            {
                "draft_of": {"required": False},
                "next_draft": {"required": False},
            }
        )

    def is_valid(self, raise_exception=False):
        if CS.get_boolean_query_param(
            self.context["request"], "draft"
        ) and not self.initial_data.get("data_catalog"):
            self.initial_data["data_catalog"] = DFT_CATALOG

        self.initial_data.pop("draft_of", None)
        self.initial_data.pop("next_draft", None)
        super().is_valid(raise_exception=raise_exception)

    def to_representation(self, instance):

        res = super().to_representation(instance)

        if "request" in self.context:

            if CS.get_boolean_query_param(self.context["request"], "include_user_metadata"):
                # keep user metadata and possible file_details that have been populated in super().to_representation()
                pass
            else:
                res.get("research_dataset", {}).pop("files", None)
                res.get("research_dataset", {}).pop("directories", None)

        if "draft_of" in res:
            if instance.user_is_privileged(instance.request or self.context.get("request")):
                res["draft_of"] = instance.draft_of.identifiers_dict
            else:
                del res["draft_of"]

        if "next_draft" in res:
            if instance.user_is_privileged(instance.request or self.context.get("request")):
                res["next_draft"] = instance.next_draft.identifiers_dict
            else:
                del res["next_draft"]

        return res

    def validate_research_dataset_files(self, value):
        """
        Validate only files and directories of a research_dataset.
        - populate titles of files and dirs
        - validate and populate ref data
        - validate received file and dir entries against schema
            - there is a special schema file dataset_files_schema.json, which uses
              objects defined in ida dataset schema. the RefResolver object is necessary
              to make the json schema external file links work.
        """
        self._populate_file_and_dir_titles(value)

        CRS.validate_reference_data(value, cache, request=self.context.get("request"))

        rd_files_schema = CS.get_json_schema(self._schemas_directory_path, "dataset_files")

        resolver = RefResolver(
            # at some point when jsonschema package is updated, probably need to switch to
            # using the below commented out parameter names instead
            # schema_path='file:{}'.format(path.dirname(path.dirname(__file__)) + '/schemas/dataset_files_schema.json'),
            # schema=rd_files_schema
            base_uri="file:{}".format(
                path.join(self._schemas_directory_path, "dataset_files_schema.json")
            ),
            referrer=rd_files_schema,
        )

        # for debugging, below may be useful
        # Draft4Validator.check_schema(rd_files_schema)

        validator = Draft4Validator(rd_files_schema, resolver=resolver, format_checker=None)

        if not value:
            _logger.info(
                "Validating files and/or directories with empty value. Nothing to validate."
            )
            return
        try:
            validator.validate(value)
        except JsonValidationError as e:
            raise ValidationError(
                {
                    "detail": [
                        "%s. Json path: %s. Schema: %s" % (e.message, [p for p in e.path], e.schema)
                    ]
                }
            )

    def _set_dataset_schema(self):
        if self._validate_as_draft():
            # drafts only exists for V2 records, otherwise normal rules apply
            schema_prefix = "dft"
            self.json_schema = CS.get_json_schema(
                self._schemas_directory_path, "dataset", schema_prefix
            )

        else:
            super()._set_dataset_schema()

    def _validate_as_draft(self):
        if self._operation_is_create and "request" in self.context:
            return CS.get_boolean_query_param(self.context["request"], "draft")

        if CS.request_is_create_operation(self.instance.request):
            return self.instance._save_as_draft()

        return True
