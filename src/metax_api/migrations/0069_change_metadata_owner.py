from django.db import migrations

import logging

logger = logging.getLogger(__name__)


def change_metadata_provider_user(apps, old_name, new_name):
    CatalogRecord = apps.get_model("metax_api", "CatalogRecord")
    crs = CatalogRecord.objects.filter(metadata_provider_user=old_name)
    logger.info(f"Updating metadata_provider_user for {len(crs)} CatalogRecord objects")
    crs.update(metadata_provider_user=new_name)

    EditorUserPermission = apps.get_model("metax_api", "EditorUserPermission")
    user_perms = EditorUserPermission.objects.filter(user_id=old_name)
    logger.info(f"Updating user_id for {user_perms.count()} EditorUserPermissions objects")
    user_perms.update(user_id=new_name)


def update(apps, schema_editor):
    old_name = "pvaisane"
    new_name = "paulivai"

    change_metadata_provider_user(apps, old_name, new_name)


def revert(apps, schema_editor):
    """
    No need to revert the changes
    """
    pass


class Migration(migrations.Migration):
    dependencies = [
        ("metax_api", "0068_catalogrecord_pid_migrated"),
    ]

    operations = [
        migrations.RunPython(update, revert),
    ]
