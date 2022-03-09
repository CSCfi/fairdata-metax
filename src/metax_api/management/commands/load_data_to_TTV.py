# This file is part of the Metax API service
#
# Copyright 2017-2018 Ministry of Education and Culture, Finland
#
# :author: CSC - IT Center for Science Ltd., Espoo Finland <servicedesk@csc.fi>
# :license: MIT

import logging

from django.core.management.base import BaseCommand

from metax_api.models import CatalogRecord
from metax_api.services import RabbitMQService

logger = logging.getLogger(__name__)

# serializer needs these in context so give them.
# definitely not the way to do it but still, here it is..
class User:
    def __init__(self):
        self.is_service = True


class Request:
    def __init__(self, user):
        self.user = user
        self.query_params = []
        self.method = "POST"


class Command(BaseCommand):

    help = "Upload all existing and removed catalog records to TTV's RabbitMQ queue"

    def handle(self, *args, **options):
        user = User()
        request = Request(user)
        context = {"request": request}

        aff_rows = 0
        catalog_records = CatalogRecord.objects.filter(state="published")
        for catalog_record in catalog_records:
            serializer = catalog_record.serializer_class
            cr_json = serializer(catalog_record, context=context).data
            cr_json["data_catalog"] = {"catalog_json": catalog_record.data_catalog.catalog_json}

            RabbitMQService.publish(cr_json, routing_key="create", exchange="TTV-datasets")
            aff_rows += 1
        logger.info(f"Published {aff_rows} records to exchange: TTV-datasets, routing_key: create")

        aff_rows = 0
        removed_catalog_records = CatalogRecord.objects_unfiltered.filter(removed=True)
        for catalog_record in removed_catalog_records:
            serializer = catalog_record.serializer_class
            cr_json = serializer(catalog_record, context=context).data
            cr_json["data_catalog"] = {"catalog_json": catalog_record.data_catalog.catalog_json}

            RabbitMQService.publish(cr_json, routing_key="delete", exchange="TTV-datasets")
            aff_rows += 1
        logger.info(f"Published {aff_rows} records to exchange: TTV-datasets, routing_key: delete")



        logger.info("All catalog records published to TTV exchange")
