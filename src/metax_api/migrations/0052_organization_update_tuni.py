from django.db import migrations
from metax_api.models import CatalogRecord as CRM

import json

import logging

logger = logging.getLogger(__name__)

def is_obj_sub_org(obj, old_sub_org_value):
	if isinstance(obj, list):
		return False
	if obj.get("@type", None) != "Organization":
		return False
	return old_sub_org_value in str(obj)


def replace_sub_org_objs(obj, sub_org_pattern, replacements):
	def decode_dict(a_dict):
		for key, value in a_dict.items():
			try:
				if is_obj_sub_org(value, sub_org_pattern):
					value_as_str = json.dumps(value)
					new_value_as_str = value_as_str
					for replacement in replacements:
						new_value_as_str = new_value_as_str.replace(replacement[0]+ '"', replacement[1] + '"')
					new_value = json.loads(new_value_as_str)
					a_dict[key] = new_value

			except AttributeError:
				pass
		return a_dict

	return json.loads(json.dumps(obj), object_hook=decode_dict)



def replace_org(obj, old_org_value, new_org_value, old_en_name, new_en_name):

	def decode_dict(a_dict):
		for key, value in a_dict.items():
			try:
				new_value = value
				# To prevent updating any fields accidentally, change the value
				# only if the old value matches the old name or the old org value
				if value == old_en_name:
					new_value = new_value.replace(old_en_name, new_en_name)
				elif value == old_org_value:
					new_value = new_value.replace(old_org_value, new_org_value)

				a_dict[key] = new_value
			except AttributeError:
				pass
		return a_dict

	return json.loads(json.dumps(obj), object_hook=decode_dict)


def update_tuni_datasets(apps, schema_editor):
	logger.info("")

	old_en_name = "University of Tampere"
	new_en_name = "Tampere University"
	new_old_en_name = "University of Tampere (-2018)"
	tmp_en_name = "Temporary organization name 123-cba-en"

	old_fi_name = "Tampereen yliopisto"
	new_fi_name = "Tampereen yliopisto"
	new_old_fi_name = "Tampereen yliopisto (-2018)"
	tmp_fi_name = "Temporary organization name 123-cba-fi"

	old_sv_name = "Tammerfors universitet"
	new_sv_name = "Tammerfors universitet"
	new_old_sv_name = "Tammerfors universitet (-2018)"
	tmp_sv_name = "Temporary organization name 123-cba-sv"

	old_org_id = "01905"
	new_org_id = "10122"
	tmp_org_prefix = "tmp-org-123-cba-"
	url_prefix = "http://uri.suomi.fi/codelist/fairdata/organization/code/"

	old_org_sql = f"\"identifier\": \"{url_prefix}{old_org_id}\""
	old_org_value = f"{url_prefix}{old_org_id}"
	sub_org_pattern = f"{url_prefix}{old_org_id}-"
	tmp_org_value = f"{url_prefix}{tmp_org_prefix}{old_org_id}"
	new_org_value = f"{url_prefix}{new_org_id}"

	tmp_replacements = [[old_org_value, tmp_org_value], [old_en_name, tmp_en_name], [old_fi_name, tmp_fi_name], [old_sv_name, tmp_sv_name]]
	new_replacements = [[tmp_org_value, old_org_value], [tmp_en_name, new_old_en_name], [tmp_fi_name, new_old_fi_name], [tmp_sv_name, new_old_sv_name]]

	CatalogRecord = apps.get_model('metax_api', 'CatalogRecord')
	crs = CatalogRecord.objects.filter(research_dataset__icontains=old_org_sql)
	
	logger.info(f"Changing organization id from: {old_org_id} to: {new_org_id} on {len(crs)} dataset(s)")
	for cr in crs:
		logger.info(f"Applying migration to catalog record: {cr}")

		# Replace parent organization id and names in sub organizations with temporary values
		tmp_rd = replace_sub_org_objs(cr.research_dataset, sub_org_pattern, tmp_replacements)

		# Replace the old organization id and names with the new values
		new_rd = replace_org(tmp_rd, old_org_value, new_org_value, old_en_name, new_en_name)

		# Replace the temporary parent organization id and names in sub orgs with the original id and new "old" name
		final_rd = replace_sub_org_objs(new_rd, sub_org_pattern, new_replacements)

		cr.research_dataset = final_rd
		cr.save()
	logger.info(f"Applied migration to {len(crs)} catalog record(s)")


def revert(apps, schema_editor):
	"""
	Revert does not anything
	"""
	pass



class Migration(migrations.Migration):

    dependencies = [
        ('metax_api', '0051_organization_update_luke'),
    ]

    operations = [
        migrations.RunPython(update_tuni_datasets, revert),
    ]
