from django.db import migrations
from metax_api.models import CatalogRecord as CRM

import json

import logging

from pprint import pprint

logger = logging.getLogger(__name__)

syke_org_strs = [
"Finnish Environment Institute",
"Finnish Environment Intitute (SYKE)",
"Finnish Environment Institute (SYKE)",
"Pohjavesiaineisto- SYKE, Tieaineisto- LiV",
"Suomen ympäristökeskus",
"Suomen ympäristökeskus / Kulutuksen ja tuotannon keskus/ Ympäristötehokkuusyksikkö",
"Suomen ympäristökeskus / Kulutuksen ja tuotannon keskus/Haitallisten aineiden yksikkö",
"Suomen ympäristökeskus / Luontoympäristökeskus / Biodiversiteettiyksikkö",
"Suomen ympäristökeskus / Tietokeskus",
"Suomen ympäristökeskus / Tietokeskus / Tietopalvelu ja kirjasto",
"Suomen ympäristökeskus / Vesikeskus / Malliyksikkö",
"Suomen ympäristökeskus / Vesikeskus /Malliyksikkö",
"Suomen ympäristökeskus / Vesikeskus / Sisävesiyksikkö",
"Suomen ympäristökeskus / Vesikeskus / Vesivarayksikkö",
"Suomen ympäristökeskus /Vesikeskus",
"Suomen ympäristökeskus/Vesikeskus",
"Suomen ympäristökeskus / Ympäristöpolitiikkakeskus",
"Suomen ympäristökeskus/Ympäristöpolitiikkakeskus",
"Suomen ympäristökeskus ja ELY-keskukset",
"SYKE"
]


def get_new_actor_data(actor_data):
	new_actor_data = actor_data
	new_actor_data["identifier"] = "http://uri.suomi.fi/codelist/fairdata/organization/code/7020017"
	return new_actor_data

def check_actor_data_needs_update(actor_data):
	if actor_data["@type"] != "Organization":
		return False
	if "identifier" in actor_data:
		return False
	if any(syke_str in actor_data["name"].values() for syke_str in syke_org_strs):
		return True
	return False


def update_actor_data(cr):
	actor_types = ["contributor", "curator", "creator", "rights_holder", "publisher"]
	updated = False
	for actor_type in actor_types:
		if actor_type in cr.research_dataset:
			if isinstance(cr.research_dataset[actor_type], list):
				for actor_data in cr.research_dataset[actor_type]:
					if check_actor_data_needs_update(actor_data):
						actor_data = get_new_actor_data(actor_data)
						updated = True
			else:
				actor_data = cr.research_dataset[actor_type]
				if check_actor_data_needs_update(actor_data):
					actor_data = get_new_actor_data(actor_data)
					updated = True
	return updated


def update_syke_crs(apps, schema_editor):
	logger.info("")
	CatalogRecord = apps.get_model("metax_api", "CatalogRecord")
	syke_crs = CatalogRecord.objects.filter(data_catalog__catalog_json__identifier='urn:nbn:fi:att:data-catalog-harvest-syke')
	logger.info(f"len(crs): {len(syke_crs)}")
	update_count = 0
	for cr in syke_crs:
		updated = update_actor_data(cr)
		if updated:
			logger.info(f"updated CR: {cr.identifier}")
			update_count += 1
		cr.save()
	logger.info(f"Updated {update_count} dataset(s)")



def revert(apps, schema_editor):
    """
    Revert does not anything
    """
    pass


class Migration(migrations.Migration):

    dependencies = [
        ("metax_api", "0056_file_pas_compatible"),
    ]

    operations = [
        migrations.RunPython(update_syke_crs, revert),
    ]
