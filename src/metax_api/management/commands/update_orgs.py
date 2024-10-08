import csv
import logging
from dataclasses import asdict, dataclass, field
from typing import List

import requests
from django.conf import settings
from django.core.management.base import BaseCommand

logger = logging.getLogger(__name__)

CSV_HEADERS = [
    "org_name_fi",
    "org_name_en",
    "org_name_sv",
    "org_code",
    "unit_main_code",
    "unit_sub_code",
    "unit_name",
    "org_isni",
    "org_csc",
]


@dataclass(order=True)
class Organization:
    org_name_fi: str = field(compare=False)
    org_name_en: str = field(compare=False)
    org_code: str = field(compare=True)
    org_name_sv: str = field(default="", compare=False)
    unit_sub_code: str = field(default="", compare=True)
    unit_name: str = field(default="", compare=False)
    org_isni: str = field(default="", compare=False)
    org_csc: str = field(default="", compare=False)
    unit_main_code: str = field(default="", compare=True)

    def compare_and_update(self, other):
        changes = 0
        match = self == other
        if match:
            if self.org_name_fi != other.org_name_fi:
                self.org_name_fi = other.org_name_fi
                changes += 1
            if self.org_name_en != other.org_name_en:
                self.org_name_en = other.org_name_en
                changes += 1
            if self.org_name_sv != other.org_name_sv:
                self.org_name_sv = other.org_name_sv
                changes += 1
            if self.unit_name != other.unit_name:
                self.unit_name = other.unit_name
                changes += 1
            if self.org_isni is None and other.org_isni is not None:
                self.org_isni = other.org_isni
                changes += 1
            elif self.org_isni is not None and other.org_isni is None:
                other.org_isni = self.org_isni
                changes += 1

            if self.org_csc is None and other.org_csc is not None:
                self.org_csc = other.org_csc
                changes += 1
            elif self.org_csc is not None and other.org_csc is None:
                other.org_csc = self.org_csc
                changes += 1
        if self.unit_name.endswith(";;"):
            self.unit_name = self.unit_name[:-2]

        return match, changes

    def __str__(self):
        return f"{self.org_code}-{self.unit_sub_code}-{self.unit_name}"

    def __post_init__(self):
        if self.unit_name.endswith(";;"):
            self.unit_name = self.unit_name[:-2]


def get_orgs_from_api() -> List[Organization]:
    res = requests.get(
        "https://researchfi-api-production.2.rahtiapp.fi/portalapi/organization/_search?size=100"
    )
    data = res.json()

    orgs_json = data["hits"]["hits"]
    orgs = []

    for org in orgs_json:
        org_source = org["_source"]

        name_fi = str(org_source["nameFi"]).strip()
        name_en = str(org_source["nameEn"]).strip()
        name_sv = str(org_source.get("nameSv")).strip()
        org_code = str(org_source["organizationId"]).strip()

        sub_units = org_source.get("subUnits")

        if isinstance(sub_units, list):
            for sub_unit in sub_units:
                o = Organization(name_fi, name_en, org_code)
                if name_sv:
                    o.org_name_sv = name_sv
                unit_sub_code = str(sub_unit["subUnitID"]).strip()
                unit_name = str(sub_unit["subUnitName"]).strip()
                o.unit_sub_code = unit_sub_code
                o.unit_name = unit_name
                orgs.append(o)

        o = Organization(name_fi, name_en, org_code)
        if name_sv:
            o.org_name_sv = name_sv
        orgs.append(o)
        
    logger.info(f"retrieved {len(orgs)} organizations from research.fi")
    return orgs


def get_local_orgs() -> List[Organization]:
    local_orgs = []
    with open(settings.ORG_FILE_PATH, "r") as f:
        reader = csv.DictReader(
            f,
            delimiter=",",
            fieldnames=CSV_HEADERS,
        )
        for row in reader:
            o = Organization(**row)
            local_orgs.append(o)
    logger.info(f"loaded {len(local_orgs)} organizations from local csv")
    return local_orgs


class Command(BaseCommand):
    def handle(self, *args, **options):
        api_orgs = get_orgs_from_api()
        loc_orgs = get_local_orgs()
        union = []
        # update local orgs from api ones
        for i in loc_orgs:
            for a in api_orgs:
                match, changes = i.compare_and_update(a)
                if match and changes > 0:
                    logger.info(
                        f"updated org {i.org_name_fi} with {changes} changes in unit: {a.unit_name}"
                    )
            union.append(i)
        # add missing orgs to local ones
        added = 0
        for i in api_orgs:
            match = False
            for a in loc_orgs:
                match = i == a
            if not match:
                # logger.info(f"adding missing org {i.org_name_fi}, {i.unit_name}")
                union.append(i)
                added += 1
        logger.info(f"Added {added} organisations from research.fi to local org list")

        # sort orgs alphabetically
        s = sorted(union, key=lambda i: (i.org_name_fi, i.unit_name))
        # write new orgs to local csv
        with open(settings.ORG_FILE_PATH, "w", newline='') as f:
            logger.info("writing updated csv")
            # remove duplicates
            no_duplicates = []
            for c in s:
                if c not in no_duplicates:
                    no_duplicates.append(c)

            csv_serialized = [asdict(v) for v in no_duplicates]
            writer = csv.DictWriter(
                f,
                fieldnames=CSV_HEADERS,
                lineterminator='\n',
            )
            writer.writeheader()
            for i in csv_serialized:
                # Don't write header as row
                if i["org_name_fi"] == "org_name_fi":
                    continue
                # Malformed values from TTV api
                if i["unit_name"] == "LÃ„Ã„KETIETEELLINEN TIEDEKUNTA":
                    continue
                if "Ã…" in i["unit_name"]:
                    i["unit_name"] = str(i["unit_name"]).replace("Ã…", "Å")
                if "Ã–" in i["unit_name"]:
                    i["unit_name"] = str(i["unit_name"]).replace("Ã–", "Ö")
                if "Ã„" in i["unit_name"]:
                    i["unit_name"] = str(i["unit_name"]).replace("Ã„", "Ä")
                writer.writerow(i)
            logger.info("successfully updated organization csv")
