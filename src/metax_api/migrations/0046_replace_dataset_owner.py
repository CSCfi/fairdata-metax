from django.db import migrations

import logging

logger = logging.getLogger(__name__)

def replace_metadata_provider_user(cr, old_user, new_user):
    logger.info(f"replacing metadata_provider_user: {old_user} with new_user: {new_user}")
    if cr.metadata_provider_user:
        if cr.metadata_provider_user == old_user:
            cr.metadata_provider_user = new_user
            logger.info("metadata_provider_user changed")

def change_metadata_provider_user(apps, schema_editor):
    new_user = "frickmar"
    old_user = "mfrick@oulu.fi"
    CatalogRecord = apps.get_model('metax_api', 'CatalogRecord')
    crs = CatalogRecord.objects.filter(metadata_provider_user=old_user)
    logger.info(f"Found {len(crs)} catalog records to update")
    for cr in crs:
        try:
            logger.info(f"changing metadata_provider_user for cr {cr.identifier}")
            replace_metadata_provider_user(cr, old_user, new_user)
            # cr.editor_permissions.user_id = "asdf"
            cr.save()
            cr.editor_permissions.users.update(user_id=new_user)
            logger.info("cr save successful")
        except Exception as e:
            logger.error(e)


def revert(apps, schema_editor):
    new_user = "frickmar"
    old_user = "mfrick@oulu.fi"
    CatalogRecord = apps.get_model('metax_api', 'CatalogRecord')
    crs = CatalogRecord.objects.filter(metadata_provider_user=new_user)
    logger.info(f"Found {len(crs)} catalog records to update")
    for cr in crs:
        try:
            logger.info(f"changing metadata_provider_user for cr {cr.identifier}")
            replace_metadata_provider_user(cr, new_user, old_user)
            # cr.editor_permissions.user_id = old_user
            cr.save()
            cr.editor_permissions.users.update(user_id=old_user)
            logger.info("cr save successful")
        except Exception as e:
            logger.error(e)
            logger.error(e)

class Migration(migrations.Migration):

    dependencies = [
        ('metax_api', '0045_add_publish_fields_to_catalogs'),
    ]

    operations = [
        migrations.RunPython(change_metadata_provider_user, revert),
    ]