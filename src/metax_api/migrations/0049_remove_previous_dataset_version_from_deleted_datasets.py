from django.db import migrations
from metax_api.models import CatalogRecord as CRM

from pprint import pprint

import logging

logger = logging.getLogger(__name__)


def remove_previous_dataset_version_from_deleted_datasets(apps, schema_editor):
	logger.info("")
	logger.info("Removing previous dataset versions information from deleted datasets")
	CatalogRecord = apps.get_model('metax_api', 'CatalogRecord')
	crs = CatalogRecord.objects.filter(state = CRM.STATE_PUBLISHED, previous_dataset_version__isnull=False, removed = True)
	for cr in crs:
		logger.info(f"Applying migration to catalog record: {cr}")
		cr.previous_dataset_version = None
		cr.save()
	logger.info(f"Applied migration to {len(crs)} catalog record(s)")

def revert(apps, schema_editor):
    pass



class Migration(migrations.Migration):

    dependencies = [
        ('metax_api', '0048_organizationstatistics_projectstatistics'),
    ]

    operations = [
        migrations.RunPython(remove_previous_dataset_version_from_deleted_datasets, revert),
    ]
