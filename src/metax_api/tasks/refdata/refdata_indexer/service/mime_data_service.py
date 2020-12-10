# SPDX-FileCopyrightText: Copyright (c) 2018-2019 Ministry of Education and Culture, Finland
#
# SPDX-License-Identifier: GPL-3.0-or-later
import logging
import os
from time import sleep
from xml.etree import cElementTree as ET

import requests

from metax_api.tasks.refdata.refdata_indexer.domain.reference_data import ReferenceData
from metax_api.tasks.refdata.refdata_indexer.service.service_utils import file_exists

_logger = logging.getLogger(__name__)


class MimeDataService:
    """
    Service for getting mime type reference data for elasticsearch index. The data is in iana.org,
    so it is first fetched and parsed.
    """

    IANA_NS = "{http://www.iana.org/assignments}"
    MIME_TYPE_REF_DATA_SOURCE_URL = (
        "https://www.iana.org/assignments/media-types/media-types.xml"
    )
    MIME_TYPE_REGISTRY_IDS = [
        "application",
        "audio",
        "font",
        "image",
        "message",
        "model",
        "multipart",
        "text",
        "video",
    ]

    TEMP_XML_FILENAME = "/tmp/data.xml"

    def get_data(self):
        self._fetch_mime_data()

        if not file_exists(self.TEMP_XML_FILENAME):
            return []

        index_data_models = self._parse_mime_data()
        os.remove(self.TEMP_XML_FILENAME)

        return index_data_models

    def _parse_mime_data(self):
        data_type = ReferenceData.DATA_TYPE_MIME_TYPE
        index_data_models = []
        _logger.info("Extracting relevant data from the fetched data")

        is_parsing_model_elem = False
        found_valid_file_elem = False
        found_valid_name_elem = False
        for event, elem in ET.iterparse(
            self.TEMP_XML_FILENAME, events=("start", "end")
        ):
            if event == "start":
                if (
                    elem.tag == (self.IANA_NS + "registry")
                    and elem.get("id") in self.MIME_TYPE_REGISTRY_IDS
                ):
                    is_parsing_model_elem = True
                    registry_name = elem.get("id")
                if is_parsing_model_elem and elem.tag == (self.IANA_NS + "name"):
                    if elem.text:
                        found_valid_name_elem = True
                        uri = (
                            "https://www.iana.org/assignments/media-types/"
                            + registry_name
                            + "/"
                            + elem.text
                        )
                        data_id = registry_name + "/" + elem.text
                if (
                    is_parsing_model_elem
                    and elem.tag == (self.IANA_NS + "file")
                    and elem.get("type") == "template"
                ):
                    if elem.text:
                        found_valid_file_elem = True
                        uri = (
                            "https://www.iana.org/assignments/media-types/" + elem.text
                        )
                        data_id = elem.text
            elif event == "end":
                if elem.tag == self.IANA_NS + "registry":
                    is_parsing_model_elem = False
                if is_parsing_model_elem and elem.tag == (self.IANA_NS + "record"):
                    if found_valid_file_elem or found_valid_name_elem:
                        ref_item = ReferenceData(
                            data_id,
                            data_type,
                            {},
                            uri,
                            scheme=self.MIME_TYPE_REF_DATA_SOURCE_URL,
                        )
                        index_data_models.append(ref_item)
                    found_valid_file_elem = False
                    found_valid_name_elem = False

        return index_data_models

    def _fetch_mime_data(self):
        url = self.MIME_TYPE_REF_DATA_SOURCE_URL
        sleep_time = 2
        num_retries = 7

        _logger.info("Fetching data from url " + url)

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
            with open(self.TEMP_XML_FILENAME, "wb") as handle:
                for block in response.iter_content(1024):
                    handle.write(block)
