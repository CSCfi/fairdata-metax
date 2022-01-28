from django.db import migrations, models

import logging

logger = logging.getLogger(__name__)


def add_publish_fields_to_catalogs(apps, schema_editor):
    logger.info("Add publish_to_etsin and publish_to_ttv fields to data catalogs")

    dont_publish_to_etsin = ["urn:nbn:fi:att:data-catalog-legacy", "urn:nbn:fi:att:data-catalog-reportronic", "urn:nbn:fi:att:data-catalog-repotronic"]
    dont_publish_to_ttv = ["urn:nbn:fi:att:data-catalog-legacy"]

    DataCatalog = apps.get_model('metax_api', 'DataCatalog')
    catalogs = DataCatalog.objects.all()

    for catalog in catalogs:
        if catalog.catalog_json["identifier"] in dont_publish_to_etsin:
            catalog.publish_to_etsin = False

        if catalog.catalog_json["identifier"] in dont_publish_to_ttv:
            catalog.publish_to_ttv = False
        
        catalog.save()


def revert(apps, schema_editor):
    pass



class Migration(migrations.Migration):

    dependencies = [
        ('metax_api', '0044_change_aalto_catalog_to_harvested'),
    ]

    operations = [
        migrations.AddField(
            model_name='datacatalog',
            name='publish_to_etsin',
            field=models.BooleanField(default=True),
        ),
        migrations.AddField(
            model_name='datacatalog',
            name='publish_to_ttv',
            field=models.BooleanField(default=True),
        ),
        migrations.RunPython(add_publish_fields_to_catalogs, revert),
    ]
