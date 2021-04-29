# SPDX-FileCopyrightText: Copyright (c) 2018-2019 Ministry of Education and Culture, Finland
#
# SPDX-License-Identifier: GPL-3.0-or-later

from metax_api.tasks.refdata.refdata_indexer.service.service_utils import set_default_label


class IndexableData:
    """
    Base class for any data that can be indexed into Metax Elasticsearch
    """

    DATA_TYPE_RELATION_TYPE = "relation_type"
    DATA_TYPE_ORGANIZATION = "organization"

    def __init__(self, doc_id, doc_type, label, uri, same_as, scheme):

        self.doc_type = doc_type
        self.doc_id = self._create_es_document_id(doc_id)
        self.label = (
            label  # { 'fi': 'value1', 'en': 'value2',..., 'und': 'default_value' }
        )
        self.same_as = same_as
        self.code = doc_id

        self.scheme = ""
        if scheme:
            self.scheme = scheme
        else:
            if self.doc_type != IndexableData.DATA_TYPE_RELATION_TYPE:
                self.scheme = "http://uri.suomi.fi/codelist/fairdata/" + self.doc_type

        # Replace quotes with corresponding html entity not to break outbound json
        if self.label:
            set_default_label(self.label)
        else:
            self.label = {"und": self.code}

        self.uri = uri if uri else ""

    def to_es_document(self):
        return str(self)

    def get_es_document_id(self):
        return self.doc_id

    def _create_es_document_id(self, doc_id):
        return self.doc_type + "_" + doc_id
