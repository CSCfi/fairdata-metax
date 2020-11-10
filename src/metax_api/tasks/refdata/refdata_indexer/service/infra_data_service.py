# SPDX-FileCopyrightText: Copyright (c) 2018-2019 Ministry of Education and Culture, Finland
#
# SPDX-License-Identifier: GPL-3.0-or-later
import json
import logging
import os
import requests
from time import sleep

from metax_api.tasks.refdata.refdata_indexer.domain.reference_data import ReferenceData
from metax_api.tasks.refdata.refdata_indexer.service.service_utils import file_exists

_logger = logging.getLogger(__name__)


class InfraDataService:
    """
    Service for getting research infrastructure data for elasticsearch index. The data is in AVAA,
    so it is first fetched and parsed.
    """

    INFRA_REF_DATA_SOURCE_URL = 'https://avaa.tdata.fi/api/jsonws/tupa-portlet.Infrastructures/get-all-infrastructures'

    TEMP_FILENAME = '/tmp/data.json'

    def get_data(self):
        self._fetch_infra_data()

        if not file_exists(self.TEMP_FILENAME):
            return []

        index_data_models = self._parse_infra_data()
        os.remove(self.TEMP_FILENAME)

        return index_data_models

    def _parse_infra_data(self):
        index_data_models = []

        _logger.info("Extracting relevant data from the fetched data")
        with open(self.TEMP_FILENAME, 'r') as f:
            data = json.load(f)
            for item in data:
                if item.get('urn', ''):
                    data_id = self._get_data_id(item['urn'])
                    data_type = ReferenceData.DATA_TYPE_RESEARCH_INFRA
                    uri = 'http://urn.fi/' + item['urn']
                    same_as = []
                    label = {}
                    if item.get('name_FI'):
                        label['fi'] = item['name_FI']
                    if item.get('name_EN'):
                        label['en'] = item['name_EN']

                    ref_item = ReferenceData(
                        data_id,
                        data_type,
                        label,
                        uri,
                        same_as=same_as,
                        scheme=InfraDataService.INFRA_REF_DATA_SOURCE_URL
                    )
                    index_data_models.append(ref_item)

        return index_data_models

    def _fetch_infra_data(self):
        url = self.INFRA_REF_DATA_SOURCE_URL
        _logger.info("Fetching data from url " + url)

        sleep_time = 2
        num_retries = 7

        for x in range(0, num_retries):
            try:
                response = requests.get(url, stream=True)
                str_error = None
            except Exception as e:
                str_error = e

            if str_error:
                sleep(sleep_time)  # wait before trying to fetch the data again
                sleep_time *= 2  # exponential backoff
            else:
                break

        if not str_error and response:
            with open(self.TEMP_FILENAME, 'wb') as handle:
                for block in response.iter_content(1024):
                    handle.write(block)

    def _get_data_id(self, urn):
        return urn.replace(':', '-')
