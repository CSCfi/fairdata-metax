from django.db import migrations
from metax_api.models import CatalogRecord as CRM

import json

import logging

logger = logging.getLogger(__name__)


def replace_org(obj, old_org_id, new_org_id):

	def decode_dict(a_dict):
		old_org_value = f"http://uri.suomi.fi/codelist/fairdata/organization/code/{old_org_id}"
		new_org_value = f"http://uri.suomi.fi/codelist/fairdata/organization/code/{new_org_id}"
		for key, value in a_dict.items():
			try:
				a_dict[key] = value.replace(old_org_value, new_org_value)
			except AttributeError:
				pass
		return a_dict

	return json.loads(json.dumps(obj), object_hook=decode_dict)


def update_luke_datasets(apps, schema_editor):
	logger.info("")


	new_org_id = "4100010"
	old_org_id = "02446292"
	old_org_obj = f"\"identifier\": \"http://uri.suomi.fi/codelist/fairdata/organization/code/{old_org_id}\""

	CatalogRecord = apps.get_model('metax_api', 'CatalogRecord')
	crs = CatalogRecord.objects.filter(research_dataset__icontains=old_org_obj)
	
	logger.info(f"Changing organization id from: {old_org_id} to: {new_org_id} on {len(crs)} dataset(s)")
	for cr in crs:
		logger.info(f"Applying migration to catalog record: {cr}")
		new_rd = replace_org(cr.research_dataset, old_org_id, new_org_id)
		cr.research_dataset = new_rd
		cr.save()
	logger.info(f"Applied migration to {len(crs)} catalog record(s)")

def revert(apps, schema_editor):
    pass



class Migration(migrations.Migration):

    dependencies = [
        ('metax_api', '0050_remove_previous_dataset_version_from_deleted_datasets'),
    ]

    operations = [
        migrations.RunPython(update_luke_datasets, revert),
    ]
