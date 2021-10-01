# SPDX-FileCopyrightText: Copyright (c) 2018-2019 Ministry of Education and Culture, Finland
#
# SPDX-License-Identifier: GPL-3.0-or-later
"""
A script that parses csv files and returns a list of organization objects for elasticsearch indexing purposes.

The first row in a csv file defines the keys that are used to parse the
organizations. The following keys are accepted as row headers. Separate column values with commas (,):
org_name_fi,org_name_en,org_name_sv,org_code,unit_main_code,unit_sub_code,unit_name
unit_main_code can be left blank, other fields are required.

This file is a modified version of the original implementation by Peter Kronström.
"""

import csv
import json
import logging

from django.conf import settings

_logger = logging.getLogger(__name__)

INPUT_FILES = [settings.ORG_FILE_PATH]
OUTPUT_FILE = "/tmp/metax_organizations.json"


def parse_csv():
    root_orgs = {}
    output_orgs = []

    for csvfile in INPUT_FILES:
        _logger.info("Now parsing file {}".format(csvfile))
        try:
            with open(csvfile, "r") as csv_file:
                csv_reader = csv.DictReader(csv_file, delimiter=",", quotechar='"')
                for row in csv_reader:
                    # parse fields in a single row
                    org_name_fi = row.get("org_name_fi", "")
                    org_name_en = row.get("org_name_en", "")
                    org_name_sv = row.get("org_name_sv", "")
                    org_code = row.get("org_code", "")
                    # unit_main_code = row.get('unit_main_code', '')
                    unit_sub_code = row.get("unit_sub_code", "")
                    unit_name = row.get("unit_name", "").rstrip()
                    org_isni = row.get("org_isni", "")
                    org_csc = row.get("org_csc", "")

                    if govern(row):
                        # save parent ids to parent_organizations dict
                        # and create a root level organization
                        if org_code not in root_orgs:
                            root_org_dict = create_organization(
                                org_code,
                                org_name_fi,
                                org_name_en=org_name_en,
                                org_name_sv=org_name_sv,
                                org_isni=org_isni,
                                org_csc=org_csc,
                            )
                            root_orgs[org_code] = root_org_dict.get("org_id", None)
                            output_orgs.append(root_org_dict)

                        # otherwise create an org and append it to existing root's hierarchy
                        if unit_sub_code and unit_name:
                            organization_code = "-".join([org_code, unit_sub_code])  # Unique
                            parent_id = root_orgs.get(org_code, None)
                            output_orgs.append(
                                create_organization(
                                    organization_code, unit_name, parent_id=parent_id
                                )
                            )

        except IOError:
            _logger.error("File {} could not be found.".format(csvfile))

    with open(OUTPUT_FILE, "w+") as outfile:
        json.dump(output_orgs, outfile)


def govern(row):
    """
    returns false, if the row does not contain necessary fields.
    """

    # root-level organization only
    if all(row[i] for i in ["org_name_fi", "org_code"]):
        # check if sub-unit fields are present
        if not all(row[i] for i in ["unit_sub_code", "unit_name"]):
            _logger.error(
                f"Missing unit codes (unit_sub_code, unit_name). Creating root organization only: {row}"
            )
        return True
    else:
        _logger.error(
            f"Missing root organization fields (org_name_fi, org_code). Skipping row {row}"
        )
        return False


def create_organization(
    org_id_str,
    org_name_fi,
    org_name_en=None,
    org_name_sv=None,
    org_isni=None,
    org_csc=None,
    parent_id=None,
):
    """
    create organization data_dict that is suitable for ES indexing
    """
    _logger.info("creating organization")
    org_dict = {}
    org_dict["org_id"] = org_id_str

    org_dict["label"] = {"fi": org_name_fi, "und": org_name_fi}
    if org_name_en:
        org_dict["label"]["en"] = org_name_en

    if org_name_sv:
        org_dict["label"]["sv"] = org_name_sv

    if parent_id:
        org_dict["parent_id"] = parent_id

    if org_isni:
        org_dict["same_as"] = [org_isni]

    if org_csc:
        org_dict["org_csc"] = org_csc

    return org_dict
