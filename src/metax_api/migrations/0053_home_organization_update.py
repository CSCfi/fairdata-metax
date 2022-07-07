from django.db import migrations

import logging

logger = logging.getLogger(__name__)

def replace_home_organization(cr, old_org, new_org):
    logger.info(f"replacing home organization {old_org} with new_org: {new_org}")
    changed = False
    if cr.metadata_provider_org:
        if cr.metadata_provider_org == old_org:
            cr.metadata_provider_org = new_org
            changed = True
            logger.info("metadata_provider_user changed")
    if cr.metadata_owner_org:
        if cr.metadata_owner_org == old_org:
            cr.metadata_owner_org = new_org
            changed = True
            logger.info("metadata_owner_user changed")
    if changed == False:
        logger.info("home organization not changed")


def change_metadata_provider_user(apps, schema_editor):
    new_fmi_org = "fmi.fi"
    old_fmi_org = "fmi.fi helsinki.fi"
    new_tuni_org = "tuni.fi"
    old_tuni_org = "tuni.fi tuni.fi"
    
    tuni_datasets = [
		"0ab24b68-4658-4259-9f1d-3150be898c63",
		"b2017eef-4b9c-4961-9ad6-5fe5ad9fc6ee",
		"14cff64b-a019-49cb-bcb9-7729ee7f55b4"
    ]
    fmi_datasets = [
		"7cf4b0d1-7789-4756-b7bc-3964d0646a4c",
		"823067a6-3774-4ce5-81ce-1852be663dcf"
    ]

    CatalogRecord = apps.get_model('metax_api', 'CatalogRecord')

    tuni_crs = CatalogRecord.objects.filter(identifier__in=tuni_datasets)
    logger.info(f"Found {len(tuni_crs)} catalog records to update")
    for cr in tuni_crs:
        try:
            logger.info(f"changing home organization for cr {cr.identifier}")
            replace_home_organization(cr, old_tuni_org, new_tuni_org)
            cr.save()
            logger.info("cr save successful")
        except Exception as e:
            logger.error(e)

    fmi_crs = CatalogRecord.objects.filter(identifier__in=fmi_datasets)
    logger.info(f"Found {len(fmi_crs)} catalog records to update")
    for cr in fmi_crs:
        try:
            logger.info(f"changing home organization for cr {cr.identifier}")
            replace_home_organization(cr, old_fmi_org, new_fmi_org)
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
        ('metax_api', '0052_organization_update_tuni'),
    ]

    operations = [
        migrations.RunPython(change_metadata_provider_user, revert),
    ]