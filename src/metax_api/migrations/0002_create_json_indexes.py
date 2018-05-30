# -*- coding: utf-8 -*-
from __future__ import unicode_literals

from django.db import migrations


class Migration(migrations.Migration):

    """
    Create unique indexes for fields inside the jsonfields in different models.
    """

    dependencies = [
        ('metax_api', '0001_initial'),
    ]

    operations = [
        # data catalogs
        migrations.RunSQL(
            "CREATE UNIQUE INDEX IF NOT EXISTS dat_cat_ident "
            "ON metax_api_datacatalog ((catalog_json->>'identifier'));"
        ),

        # contracts
        migrations.RunSQL(
            "CREATE UNIQUE INDEX IF NOT EXISTS contract_ident "
            "ON metax_api_contract ((contract_json->>'identifier'));"
        ),

        # file storages
        migrations.RunSQL(
            "CREATE UNIQUE INDEX IF NOT EXISTS file_storage_ident "
            "ON metax_api_filestorage ((file_storage_json->>'identifier'));"
        ),
    ]
