from django.db import migrations

import logging

logger = logging.getLogger(__name__)


def add_metax_service_user_to_catalogs(apps, schema_editor):
    DataCatalog = apps.get_model('metax_api', 'DataCatalog')

    catalogs = DataCatalog.objects.all()
    logger.info(f"Giving metax_service user permissions to create, update, and read catalog records in all catalogs")
    logger.info(f"Updating all {len(catalogs)} catalogs")
    for catalog in catalogs:
        logger.info(f"Updating catalog: {catalog.catalog_json['identifier']}")
        catalog.catalog_record_services_create += ",metax_service"
        catalog.catalog_record_services_edit += ",metax_service"
        catalog.catalog_record_services_read += ",metax_service"
        catalog.save()


def revert(apps, schema_editor):
    DataCatalog = apps.get_model('metax_api', 'DataCatalog')

    catalogs = DataCatalog.objects.all()
    logger.info(f"Removing metax_service user the permission to create, update, and read catalog records in all catalogs")
    logger.info(f"Updating all {len(catalogs)} catalogs")
    for catalog in catalogs:
        logger.info(f"Updating catalog: {catalog.catalog_json['identifier']}")
        catalog.catalog_record_services_create.replace(",metax_service", "")
        catalog.catalog_record_services_edit.replace(",metax_service", "")
        catalog.catalog_record_services_read.replace(",metax_service", "")
        catalog.save()

class Migration(migrations.Migration):

    dependencies = [
        ('metax_api', '0066_metadata_org_update_fmi'),
    ]

    operations = [
        migrations.RunPython(add_metax_service_user_to_catalogs, revert),
    ]