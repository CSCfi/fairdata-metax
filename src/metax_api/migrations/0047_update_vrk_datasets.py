from django.db import migrations

import logging

logger = logging.getLogger(__name__)

def update_vrk_datasets(apps, schema_editor):

    logger.info("Updating organization info of catalog records by Väestörekisterikeskus")

    CatalogRecord = apps.get_model('metax_api', 'CatalogRecord')
    new_name_fi = "Digi- ja väestötietovirasto"
    new_name_en = "Digital and Population Data Services Agency"
    new_name_sv = "Myndigheten för digitalisering och befolkningsdata"
    description_suffix_fi = "\n\nAineiston luojaorganisaation aikaisempi nimi: Väestörekisterikeskus."
    description_suffix_en = "\n\nPrevious name of dataset creator organization: Population Register Center."

    # Catalog Records by Väestörekisterikeskus
    # Getting these from the database using Django filters would have
    # been too complicated, so instead they are hardcoded
    cr_ids = [
        "a3610de8-73fa-4e25-a89b-320549c71f0a",
        "b77c91cf-a437-4d01-b2ec-efb08605d559",
        "7787c312-3973-4e16-a032-7b89a0257739"
        ]
    
    crs = CatalogRecord.objects.filter(identifier__in = cr_ids)
    logger.info(f"Found {len(crs)} catalog records to update")
    for cr in crs:
        cr_json = cr.research_dataset
        logger.info(f"Updating catalog record: {cr}")
        cr_json["creator"][0]["name"]["en"] = new_name_en
        cr_json["creator"][0]["name"]["fi"] = new_name_fi
        cr_json["creator"][0]["name"]["sv"] = new_name_sv
        if description_suffix_en not in cr_json["description"]["en"]:
            cr_json["description"]["en"] += description_suffix_en
        if description_suffix_fi not in cr_json["description"]["fi"]:
            cr_json["description"]["fi"] += description_suffix_fi
        cr.save()


def revert(apps, schema_editor):
    pass



class Migration(migrations.Migration):

    dependencies = [
        ('metax_api', '0046_replace_dataset_owner'),
    ]

    operations = [
        migrations.RunPython(update_vrk_datasets, revert),
    ]
