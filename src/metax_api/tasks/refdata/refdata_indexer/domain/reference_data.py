# SPDX-FileCopyrightText: Copyright (c) 2018-2019 Ministry of Education and Culture, Finland
#
# SPDX-License-Identifier: GPL-3.0-or-later
import json

from metax_api.tasks.refdata.refdata_indexer.domain.indexable_data import IndexableData


class ReferenceData(IndexableData):
    """
    Model class for reference data that can be indexed into Metax Elasticsearch
    """

    DATA_TYPE_FIELD_OF_SCIENCE = "field_of_science"
    DATA_TYPE_LANGUAGE = "language"
    DATA_TYPE_LOCATION = "location"
    DATA_TYPE_KEYWORD = "keyword"

    DATA_TYPE_RESEARCH_INFRA = "research_infra"

    DATA_TYPE_MIME_TYPE = "mime_type"

    DATA_TYPE_ACCESS_TYPE = "access_type"
    DATA_TYPE_RESOURCE_TYPE = "resource_type"
    DATA_TYPE_IDENTIFIER_TYPE = "identifier_type"
    DATA_TYPE_RESTRICTION_GROUNDS = "restriction_grounds"
    DATA_TYPE_CONTRIBUTOR_ROLE = "contributor_role"
    DATA_TYPE_CONTRIBUTOR_TYPE = "contributor_type"
    DATA_TYPE_FUNDER_TYPE = "funder_type"
    DATA_TYPE_LICENSES = "license"
    DATA_TYPE_FILE_TYPE = "file_type"
    DATA_TYPE_USE_CATEGORY = "use_category"
    DATA_TYPE_LIFECYCLE_EVENT = "lifecycle_event"
    DATA_TYPE_PRESERVATION_EVENT = "preservation_event"
    DATA_TYPE_FILE_FORMAT_VERSION = "file_format_version"
    DATA_TYPE_EVENT_OUTCOME = "event_outcome"

    FINTO_REF_DATA_TYPES = [
        DATA_TYPE_FIELD_OF_SCIENCE,
        DATA_TYPE_LANGUAGE,
        DATA_TYPE_LOCATION,
        DATA_TYPE_KEYWORD,
    ]

    LOCAL_REF_DATA_TYPES = [
        DATA_TYPE_ACCESS_TYPE,
        DATA_TYPE_RESOURCE_TYPE,
        DATA_TYPE_IDENTIFIER_TYPE,
        DATA_TYPE_RESTRICTION_GROUNDS,
        DATA_TYPE_CONTRIBUTOR_ROLE,
        DATA_TYPE_CONTRIBUTOR_TYPE,
        DATA_TYPE_FUNDER_TYPE,
        DATA_TYPE_LICENSES,
        DATA_TYPE_FILE_TYPE,
        DATA_TYPE_USE_CATEGORY,
        IndexableData.DATA_TYPE_RELATION_TYPE,
        DATA_TYPE_LIFECYCLE_EVENT,
        DATA_TYPE_PRESERVATION_EVENT,
        DATA_TYPE_FILE_FORMAT_VERSION,
        DATA_TYPE_EVENT_OUTCOME,
        DATA_TYPE_RESEARCH_INFRA,
    ]

    def __init__(
        self,
        data_id,
        data_type,
        label,
        uri,
        parent_ids=[],
        child_ids=[],
        same_as=[],
        wkt="",
        input_file_format="",
        output_format_version="",
        scheme="",
        internal_code="",
    ):

        super(ReferenceData, self).__init__(
            data_id, data_type, label, uri, same_as, scheme
        )

        self.wkt = wkt
        self.input_file_format = input_file_format
        self.output_format_version = output_format_version

        self.parent_ids = []
        self.child_ids = []
        self.has_children = False

        self.internal_code = internal_code

        if len(parent_ids) > 0:
            self.parent_ids = [self._create_es_document_id(p_id) for p_id in parent_ids]
        if len(child_ids) > 0:
            self.child_ids = [self._create_es_document_id(c_id) for c_id in child_ids]

        if len(child_ids) > 0:
            self.has_children = True

    def __str__(self):
        return (
            "{" + '"id":"' + self.get_es_document_id() + '",'
            '"code":"' + self.code + '",'
            '"type":"' + self.doc_type + '",'
            '"uri":"' + self.uri + '",'
            '"wkt":"' + self.wkt + '",'
            '"input_file_format":"' + self.input_file_format + '",'
            '"output_format_version":"' + self.output_format_version + '",'
            '"label":' + json.dumps(self.label) + ","
            '"parent_ids":' + json.dumps(self.parent_ids) + ","
            '"child_ids":' + json.dumps(self.child_ids) + ","
            '"has_children":' + json.dumps(self.has_children) + ","
            '"same_as":' + json.dumps(self.same_as) + ","
            '"internal_code":"' + self.internal_code + '",'
            '"scheme":"' + self.scheme + '"'
            "}"
        )
