# This file is part of the Metax API service
#
# Copyright 2017-2018 Ministry of Education and Culture, Finland
#
# :author: CSC - IT Center for Science Ltd., Espoo Finland <servicedesk@csc.fi>
# :license: MIT

import datetime
import json
import logging

from django.core.management.base import BaseCommand
from django.db import IntegrityError

from metax_api.models import DataCatalog, FileStorage

logger = logging.getLogger(__name__)

class Command(BaseCommand):

    help = "Load initial data for Metax: Data catalogs, file storages."

    def handle(self, *args, **options):
        with open("metax_api/initialdata/datacatalogs.json", "r") as f:
            data_catalogs = json.load(f)
            for json_dc in data_catalogs:
                try:
                    dc = DataCatalog(catalog_json=json_dc["catalog_json"], date_created=datetime.datetime.now())
                    dc.catalog_record_services_create = json_dc["catalog_record_services_create"]
                    dc.catalog_record_services_edit = json_dc["catalog_record_services_edit"]
                    dc.catalog_record_services_read = json_dc["catalog_record_services_read"]
                    dc.save()
                except IntegrityError as e:
                    logger.error("datacatalog already exists in the database")
        logger.info("Successfully created missing datacatalogs")

        with open("metax_api/initialdata/filestorages.json", "r") as f:
            storages = json.load(f)
            for fs in storages:
                try:
                    fs = FileStorage(file_storage_json=fs["file_storage_json"], date_created=datetime.datetime.now())
                    fs.save()
                except IntegrityError as e:
                    logger.error("filestorage already exists in the database")
        logger.info("Successfully created missing filestorages")
