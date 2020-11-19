# SPDX-FileCopyrightText: Copyright (c) 2018-2019 Ministry of Education and Culture, Finland
#
# SPDX-License-Identifier: GPL-3.0-or-later
import json
import logging
import os

import metax_api.tasks.refdata.refdata_indexer.organization_csv_parser as org_parser
from metax_api.tasks.refdata.refdata_indexer.domain.organization_data import (
    OrganizationData,
)

logger = logging.getLogger(__name__)


class OrganizationService:
    """
    Service for getting organization data for elasticsearch index
    """

    INPUT_FILE = org_parser.OUTPUT_FILE

    def get_data(self):
        logger.info("parsing organizations")
        # Parse csv files containing organizational data
        org_parser.parse_csv()

        index_data_models = []
        with open(self.INPUT_FILE) as org_data_file:
            data = json.load(org_data_file)

        for org in data:
            parent_id = org.get("parent_id", "")
            same_as = org.get("same_as", [])
            org_csc = org.get("org_csc", "")
            index_data_models.append(
                OrganizationData(
                    org["org_id"], org["label"], parent_id, same_as, org_csc
                )
            )

        os.remove(self.INPUT_FILE)
        return index_data_models
