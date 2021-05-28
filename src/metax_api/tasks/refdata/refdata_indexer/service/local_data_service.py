# SPDX-FileCopyrightText: Copyright (c) 2018-2019 Ministry of Education and Culture, Finland
#
# SPDX-License-Identifier: GPL-3.0-or-later
import json

from django.conf import settings

from metax_api.tasks.refdata.refdata_indexer.domain.reference_data import ReferenceData


class LocalDataService:
    """
    Service for getting reference data for elasticsearch index. The data is local,
    i.e. data should exist on localhost.
    """

    LOCAL_REFDATA_FOLDER = settings.LOCAL_REF_DATA_FOLDER

    # move infra here beacuse the fetch API for it is broken and there is no estimate when it could be fixed.
    # to keep the reference data unchanged, use the old scheme until some fix for it has been invented and
    # validated.
    INFRA_SCHEME = (
        "https://avaa.tdata.fi/api/jsonws/tupa-portlet.Infrastructures/get-all-infrastructures"
    )

    def get_data(self, data_type):
        return self._parse_local_reference_data(data_type)

    def _parse_local_reference_data(self, data_type):
        index_data_models = []
        with open(self.LOCAL_REFDATA_FOLDER + data_type + ".json", "r") as f:
            data = json.load(f)
            for item in data:
                ref_item = ReferenceData(
                    item.get("id", ""),
                    data_type,
                    item.get("label", ""),
                    item.get("uri", ""),
                    same_as=item.get("same_as", []),
                    input_file_format=item.get("input_file_format", ""),
                    output_format_version=item.get("output_format_version", ""),
                    internal_code=item.get("internal_code", ""),
                    scheme=self.INFRA_SCHEME
                    if data_type == ReferenceData.DATA_TYPE_RESEARCH_INFRA
                    else "",
                )

                index_data_models.append(ref_item)

        return index_data_models
