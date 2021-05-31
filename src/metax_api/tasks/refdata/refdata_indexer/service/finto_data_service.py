# SPDX-FileCopyrightText: Copyright (c) 2018-2019 Ministry of Education and Culture, Finland
#
# SPDX-License-Identifier: GPL-3.0-or-later
import json
import logging
from time import sleep

import requests
from django.conf import settings
from rdflib import RDF, Graph, URIRef
from rdflib.namespace import SKOS

from metax_api.tasks.refdata.refdata_indexer.domain.reference_data import ReferenceData

_logger = logging.getLogger(__name__)


class FintoDataService:
    """
    Service for getting reference data for elasticsearch index. The data is in Finto,
    so it is first fetched and parsed.
    """

    FINTO_REFERENCE_DATA_SOURCE_URLS = {
        ReferenceData.DATA_TYPE_FIELD_OF_SCIENCE: "http://finto.fi/rest/v1/okm-tieteenala/data",
        ReferenceData.DATA_TYPE_LANGUAGE: "http://finto.fi/rest/v1/lexvo/data",
        ReferenceData.DATA_TYPE_LOCATION: "http://finto.fi/rest/v1/yso-paikat/data",
        ReferenceData.DATA_TYPE_KEYWORD: "http://finto.fi/rest/v1/koko/data",
    }

    WKT_FILENAME = settings.WKT_FILENAME

    # Use this to decide whether to read location coordinates from a file
    # or whether to read coordinates from wikidata and paikkatiedot.fi and
    # at the same time writing the coordinates to a file
    READ_COORDINATES_FROM_FILE = True

    def get_data(self, data_type):
        graph = self._fetch_finto_data(data_type)

        if graph is None:
            return []

        index_data_models = self._parse_finto_data(graph, data_type)
        return index_data_models

    def _parse_finto_data(self, graph, data_type):
        index_data_models = []

        if data_type == ReferenceData.DATA_TYPE_LOCATION:
            if self.READ_COORDINATES_FROM_FILE:
                with open(self.WKT_FILENAME) as c:
                    coordinates = json.load(c)
            else:
                with open(self.WKT_FILENAME, "w") as outfile:
                    outfile.write("{\n")

        _logger.info("Extracting relevant data from the fetched data")

        in_scheme = ""
        for concept in graph.subjects(RDF.type, SKOS.Concept):
            for value in graph.objects(concept, SKOS.inScheme):
                in_scheme = str(value)
                break
            break

        for concept in graph.subjects(RDF.type, SKOS.Concept):
            uri = str(concept)
            # preferred labels
            label = dict(
                (
                    (literal.language, str(literal))
                    for literal in graph.objects(concept, SKOS.prefLabel)
                )
            )
            # parents (broader)
            parent_ids = [
                self._get_uri_end_part(parent) for parent in graph.objects(concept, SKOS.broader)
            ]
            # children (narrower)
            child_ids = [
                self._get_uri_end_part(child) for child in graph.objects(concept, SKOS.narrower)
            ]
            same_as = []
            wkt = ""
            if data_type == ReferenceData.DATA_TYPE_LOCATION:
                # find out the coordinates of matching PNR or Wikidata entities
                matches = sorted(graph.objects(concept, SKOS.closeMatch))
                for match in matches:
                    if self.READ_COORDINATES_FROM_FILE:
                        wkt = coordinates.get(uri, "")
                    else:
                        wkt = self._get_coordinates_for_location_from_url(match)
                        with open(self.WKT_FILENAME, "a") as outfile:
                            outfile.write('"' + uri + '":"' + wkt + '",\n')
                    if wkt != "":
                        # Stop after first success
                        break

            data_id = self._get_uri_end_part(concept)

            ref_item = ReferenceData(
                data_id,
                data_type,
                label,
                uri,
                parent_ids=parent_ids,
                child_ids=child_ids,
                same_as=same_as,
                wkt=wkt,
                scheme=in_scheme,
            )
            index_data_models.append(ref_item)

        if data_type == ReferenceData.DATA_TYPE_LOCATION:
            if not self.READ_COORDINATES_FROM_FILE:
                with open(self.WKT_FILENAME, "a") as outfile:
                    outfile.write("}")

        _logger.info("Done with all")
        return index_data_models

    def _fetch_finto_data(self, data_type):
        url = self.FINTO_REFERENCE_DATA_SOURCE_URLS[data_type]
        _logger.info("Fetching data from url " + url)
        sleep_time = 2
        num_retries = 7
        g = Graph()

        for x in range(0, num_retries):
            try:
                g.parse(url)
                str_error = None
            except Exception as e:
                str_error = e

            if str_error:
                sleep(sleep_time)  # wait before trying to fetch the data again
                sleep_time *= 2  # exponential backoff
            else:
                break

        if not str_error:
            return g
        else:
            _logger.error("Failed to read Finto data of type %s, skipping.." % data_type)
            return None

    def _get_uri_end_part(self, uri):
        return uri[uri.rindex("/") + 1 :].strip()

    def _get_coordinates_for_location_from_url(self, url):
        sleep_time = 2
        num_retries = 4

        if "wikidata" in url:
            g = Graph()
            for x in range(0, num_retries):
                try:
                    g.parse(url + ".rdf")
                    str_error = None
                except Exception as e:
                    str_error = e

                if str_error:
                    _logger.error("Unable to read wikidata, trying again..")
                    sleep(sleep_time)  # wait before trying to fetch the data again
                    sleep_time *= 2  # exponential backoff
                else:
                    break

            if not str_error:
                subject = URIRef(url)
                predicate = URIRef("http://www.wikidata.org/prop/direct/P625")
                for o in g.objects(subject, predicate):
                    return str(o).upper()
            else:
                _logger.error("Failed to read wikidata, skipping..")

        elif "paikkatiedot" in url:
            for x in range(0, num_retries):
                try:
                    response = requests.get(url + ".jsonld")
                    str_error = None
                except Exception as e:
                    str_error = e

                if str_error:
                    _logger.error("Unable to read paikkatiedot, trying again..")
                    sleep(sleep_time)  # wait before trying to fetch the data again
                    sleep_time *= 2  # exponential backoff
                else:
                    break

            if not str_error and response and response.status_code == requests.codes.ok:
                data_as_str = self._find_between(
                    response.text, '<script type="application/ld+json">', "</script>"
                )
                if data_as_str:
                    data = json.loads(data_as_str)
                    if (
                        data
                        and data.get("geo", False)
                        and data.get("geo").get("latitude", False)
                        and data.get("geo").get("longitude", False)
                    ):
                        return (
                            "POINT("
                            + str(data["geo"]["longitude"])
                            + " "
                            + str(data["geo"]["latitude"])
                            + ")"
                        )
            else:
                _logger.error("Failed to read pakkatiedot, skipping..")

        return ""

    def _find_between(self, s, first, last):
        try:
            if s:
                start = s.index(first) + len(first)
                end = s.index(last, start)
                return s[start:end]
        except Exception:
            pass
        return None
