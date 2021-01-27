# SPDX-FileCopyrightText: Copyright (c) 2018-2019 Ministry of Education and Culture, Finland
#
# SPDX-License-Identifier: GPL-3.0-or-later
import logging.config

from metax_api.tasks.refdata.refdata_indexer.domain.indexable_data import (
    IndexableData as IdxData,
)
from metax_api.tasks.refdata.refdata_indexer.domain.reference_data import (
    ReferenceData as RefData,
)
from metax_api.tasks.refdata.refdata_indexer.service.elasticsearch_service import (
    ElasticSearchService as ESS,
)
from metax_api.tasks.refdata.refdata_indexer.service.finto_data_service import (
    FintoDataService,
)
# from service.infra_data_service import InfraDataService
from metax_api.tasks.refdata.refdata_indexer.service.local_data_service import (
    LocalDataService,
)
from metax_api.tasks.refdata.refdata_indexer.service.mime_data_service import (
    MimeDataService,
)
from metax_api.tasks.refdata.refdata_indexer.service.organization_service import (
    OrganizationService,
)

_logger = logging.getLogger(__name__)


def index_data():
    """
    Runner file for indexing data to elasticsearch. Make sure requirementx.txt is installed via pip.
    """
    NO = "no"
    ALL = "all"

    instructions = """
        \nRun the program as metax-user with pyenv activated using
        'python es_index_data.py remove_and_recreate_index=INDEX types_to_reindex=TYPE',
        where either or both of the arguments should be provided with one of the following values
        per argument:\n\nINDEX:\n{indices}\n\nTYPE:\n{types}
    """
    instructions = instructions.format(
        indices=str([NO, ALL, ESS.REF_DATA_INDEX_NAME, ESS.ORG_DATA_INDEX_NAME]),
        types=str(
            [NO, ALL, ESS.REF_DATA_INDEX_NAME, IdxData.DATA_TYPE_ORGANIZATION]
            + RefData.FINTO_REF_DATA_TYPES
            + RefData.LOCAL_REF_DATA_TYPES
            + [RefData.DATA_TYPE_RESEARCH_INFRA, RefData.DATA_TYPE_MIME_TYPE]
        ),
    )

    es = ESS()
    finto_service = FintoDataService()
    local_service = LocalDataService()
    org_service = OrganizationService()
    mime_service = MimeDataService()

    es.delete_index(ESS.REF_DATA_INDEX_NAME)
    es.delete_index(ESS.ORG_DATA_INDEX_NAME)

    # Create reference data index with mappings

    es.create_index(ESS.REF_DATA_INDEX_NAME, ESS.REF_DATA_INDEX_FILENAME)
    es.create_index(ESS.ORG_DATA_INDEX_NAME, ESS.ORG_DATA_INDEX_FILENAME)

    # Reindexing for Finto data

    for data_type in RefData.FINTO_REF_DATA_TYPES:
        finto_es_data_models = finto_service.get_data(data_type)
        if len(finto_es_data_models) == 0:
            _logger.info(
                "No data models to reindex for finto data type {0}".format(data_type)
            )
            continue

        es.delete_and_update_indexable_data(
            ESS.REF_DATA_INDEX_NAME, data_type, finto_es_data_models
        )

    # Reindexing local data

    for data_type in RefData.LOCAL_REF_DATA_TYPES:
        es.delete_and_update_indexable_data(
            ESS.REF_DATA_INDEX_NAME, data_type, local_service.get_data(data_type)
        )

    # Reindexing organizations

    es.delete_and_update_indexable_data(
        ESS.ORG_DATA_INDEX_NAME, IdxData.DATA_TYPE_ORGANIZATION, org_service.get_data()
    )

    # Reindexing mime types

    mime_es_data_models = mime_service.get_data()
    if len(mime_es_data_models) > 0:
        es.delete_and_update_indexable_data(
            ESS.REF_DATA_INDEX_NAME, RefData.DATA_TYPE_MIME_TYPE, mime_es_data_models
        )
    else:
        _logger.info("no data models to reindex for mime type data type")

    _logger.info("Done")
