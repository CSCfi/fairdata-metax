from django.db import migrations

import logging

logger = logging.getLogger(__name__)


def change_metadata_provider_org(apps, schema_editor):
    new_fmi_org = "fmi.fi"
    old_fmi_org = "FMI.fi"

    CatalogRecord = apps.get_model('metax_api', 'CatalogRecord')

    fmi_crs = CatalogRecord.objects.filter(metadata_provider_org=old_fmi_org)
    logger.info(f"Found {len(fmi_crs)} catalog records to update")
    for cr in fmi_crs:
        try:
            logger.info(f"changing metadata_provider_org organization for cr {cr.identifier}")
            cr.metadata_provider_org = new_fmi_org
            cr.save()
            logger.info("cr save successful")
        except Exception as e:
            logger.error(e)


def revert(apps, schema_editor):
	"""
	No need to revert the changes
	"""
	pass

class Migration(migrations.Migration):

    dependencies = [
        ('metax_api', '0065_auto_20230908_1003'),
    ]

    operations = [
        migrations.RunPython(change_metadata_provider_org, revert),
    ]