from django.db import migrations
from metax_api.models import CatalogRecord as CRM

import json

import logging

logger = logging.getLogger(__name__)


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


def update_fmi_datasets(apps, schema_editor):
    logger.info("")

    url_prefix = "http://uri.suomi.fi/codelist/fairdata/organization/code/"

    new_org_id = "4940015"
    old_org_id = "02446647"
    old_org_obj = f'"identifier": "{url_prefix}{old_org_id}"'

    old_en_name = "Finnish Meteorological Insitute"
    new_en_name = "Finnish Meteorological Institute"

    old_org_value = f"{url_prefix}{old_org_id}"
    new_org_value = f"{url_prefix}{new_org_id}"

    CatalogRecord = apps.get_model("metax_api", "CatalogRecord")
    crs = CatalogRecord.objects.filter(research_dataset__icontains=old_org_obj)

    logger.info(
        f"Changing organization id from: {old_org_id} to: {new_org_id} on {len(crs)} dataset(s)"
    )
    for cr in crs:
        logger.info(f"Applying migration to catalog record: {cr}")
        new_rd = replace_org(
            cr.research_dataset, old_org_value, new_org_value, old_en_name, new_en_name
        )
        cr.research_dataset = new_rd
        cr.save()
    logger.info(f"Applied migration to {len(crs)} catalog record(s)")


def revert(apps, schema_editor):
    """
    Revert does not anything
    """
    pass


class Migration(migrations.Migration):

    dependencies = [
        ("metax_api", "0053_home_organization_update"),
    ]

    operations = [
        migrations.RunPython(update_fmi_datasets, revert),
    ]
