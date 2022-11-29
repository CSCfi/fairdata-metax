from django.core.exceptions import ObjectDoesNotExist
from django.db import migrations

import logging

logger = logging.getLogger(__name__)

def update_fmi_catalog_name(apps, schema_editor):
    logger.info(f"Update FMI catalog name")

    new_title_en = "METIS - FMI's EUDAT B2SHARE research data repository"
    new_title_fi = "METIS - Ilmatieteen laitoksen EUDAT B2SHARE tutkimusaineistojen data-arkisto"
    old_title_sv = "Meteorologiska Institutet EUDAT B2SHARE register"
    
    try:
        DataCatalog = apps.get_model('metax_api', 'DataCatalog')
        fmi_catalog = DataCatalog.objects.get(catalog_json__identifier = "urn:nbn:fi:att:data-catalog-fmi")

        fmi_catalog.catalog_json["title"]["en"] = new_title_en
        fmi_catalog.catalog_json["title"]["fi"] = new_title_fi
        fmi_catalog.catalog_json["title"].pop("sv", old_title_sv)

        fmi_catalog.save()
    except DataCatalog.DoesNotExist:
        logger.info("FMI catalog does not exist. Passing")
        pass

def revert(apps, schema_editor):
    """
    Revert does not anything
    """
    pass

class Migration(migrations.Migration):

    dependencies = [
        ('metax_api', '0057_fix_syke_actor_data'),
    ]

    operations = [
        migrations.RunPython(update_fmi_catalog_name, revert),
    ]
