# SPDX-FileCopyrightText: Copyright (c) 2018-2019 Ministry of Education and Culture, Finland
#
# SPDX-License-Identifier: GPL-3.0-or-later
import json

from metax_api.tasks.refdata.refdata_indexer.domain.indexable_data import IndexableData

class OrganizationData(IndexableData):
    '''
    Model class for organization data that can be indexed into Metax Elasticsearch
    '''

    ORG_PURL_BASE_URL = 'http://uri.suomi.fi/codelist/fairdata/' + IndexableData.DATA_TYPE_ORGANIZATION + '/code/'

    def __init__(
        self,
        org_id,
        label,
        parent_id='',
        same_as=[],
        org_csc='',
        scheme=''
    ):

        super().__init__(
            org_id,
            IndexableData.DATA_TYPE_ORGANIZATION,
            label,
            OrganizationData.ORG_PURL_BASE_URL + org_id,
            same_as,
            scheme
        )
        self.parent_id = ''
        if parent_id:
            self.parent_id = self._create_es_document_id(parent_id)
        self.org_csc = org_csc

    def __str__(self):
        return (
            '{' +
            '"id":"' + self.get_es_document_id() + '",'
            '"code":"' + self.code + '",'
            '"type":"' + IndexableData.DATA_TYPE_ORGANIZATION + '",'
            '"uri":"' + self.uri + '",'
            '"org_csc":"' + self.org_csc + '",'
            '"parent_id":"' + self.parent_id + '",'
            '"label":' + json.dumps(self.label) + ','
            '"same_as":' + json.dumps(self.same_as) + ','
            '"scheme":"' + self.scheme + '"'
            '}'
        )
