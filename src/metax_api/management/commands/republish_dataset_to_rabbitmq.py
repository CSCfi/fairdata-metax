# This file is part of the Metax API service
#
# Copyright 2017-2018 Ministry of Education and Culture, Finland
#
# :author: CSC - IT Center for Science Ltd., Espoo Finland <servicedesk@csc.fi>
# :license: MIT

import logging
import uuid

from django.conf import settings as django_settings
from django.core.management.base import BaseCommand

from metax_api.models import CatalogRecord, DataCatalog
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
    help = """Republish a list of catalog records to RabbitMQ"""

    def add_arguments(self, parser):
        parser.add_argument(
            "routing_key",
            type=str,
            help="The routing_key of the RabbitMQ message."
            " Possible values: create, update, delete",
        )
        parser.add_argument(
            "exchanges",
            type=str,
            help="Comma separated list of exchanges where the RabbitMQ messages are sent."
            " Possible values: datasets, TTV-datasets",
        )
        parser.add_argument(
            "catalog_record_identifiers",
            type=str,
            help="Comma separated list of uuid identifiers of the catalog records which are republished to RabbitMQ",
        )

    def is_valid_uuid(self, val):
        try:
            uuid.UUID(str(val))
            return True
        except ValueError:
            return False

    def handle(self, *args, **options):
        user = User()
        request = Request(user)
        context = {"request": request}

        cr_ids = options["catalog_record_identifiers"].split(",")
        exchanges = options["exchanges"].split(",")
        routing_key = options["routing_key"]

        logger.info(f"cr_ids: {cr_ids}")
        logger.info(f"exchanges: {exchanges}")
        logger.info(f"routing_key: {routing_key}")

        assert routing_key in [
            "create",
            "update",
            "delete",
        ], f"invalid value {routing_key} for routing_key"

        for exchange in exchanges:
            assert exchange in [
                "datasets",
                "TTV-datasets",
            ], f"invalid value {exchange} in exchanges"

        for cr_id in cr_ids:
            assert self.is_valid_uuid(cr_id), f"invalid value {cr_id} in catalog_record_identifiers"

        crs = CatalogRecord.objects.filter(identifier__in=cr_ids)
        for cr in crs:
            logger.info("Republishing datasets found in database")
            for exchange in exchanges:
                if exchange == "TTV-datasets" and cr.catalog_publishes_to_ttv() == False:
                    logger.info(
                        f"Catalog record {cr.identifier} is in a catalog that doesn't publish to TTV. Skipping."
                    )
                    continue
                if (
                    exchange == "TTV-datasets"
                    and cr.catalog_is_pas()
                    and cr.preservation_state != cr.PRESERVATION_STATE_IN_PAS
                ):
                    logger.info(
                        f"Not publishing the catalog record {cr.identifier} to TTV."
                        " Catalog Record is in PAS catalog and preservation state is not"
                        f" {cr.PRESERVATION_STATE_IN_PAS}"
                    )
                    continue
                if exchange == "datasets" and cr.catalog_publishes_to_etsin() == False:
                    logger.info(
                        f"Catalog record {cr.identifier} is in a catalog that doesn't publish to Etsin. Skipping."
                    )
                    continue

                serializer = cr.serializer_class
                cr_json = serializer(cr, context=context).data
                cr_json["data_catalog"] = {"catalog_json": cr.data_catalog.catalog_json}

                logger.info(
                    f"Republishing {cr.identifier} to exchange: {exchange} with routing_key: {routing_key}"
                )
                RabbitMQService.publish(cr_json, routing_key=routing_key, exchange=exchange)

        # To republish hard-deleted datasets, 'delete' message can be sent also
        # for catalog records which are not in database
        if routing_key == "delete" and len(crs) != len(cr_ids):
            logger.info("Republishing datasets not found in database")
            for cr_id in cr_ids:
                if cr_id not in crs.values_list("identifier", flat=True):
                    cr_json = {"identifier": cr_id}  # Create dummy cr_json
                    for exchange in exchanges:
                        logger.info(
                            f"Republishing {cr_id} to exchange: {exchange} with routing_key: {routing_key}"
                        )
                        RabbitMQService.publish(cr_json, routing_key=routing_key, exchange=exchange)

        logger.info("Republished catalog records to RabbitMQ")
