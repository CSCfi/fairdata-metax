# This file is part of the Metax API service
#
# Copyright 2017-2018 Ministry of Education and Culture, Finland
#
# :author: CSC - IT Center for Science Ltd., Espoo Finland <servicedesk@csc.fi>
# :license: MIT

import logging
import json

from django.core.management.base import BaseCommand
from metax_api.models import CatalogRecord
from metax_api.services import RabbitMQService
from metax_api.api.rest.base.serializers import CatalogRecordSerializer

logger = logging.getLogger(__name__)


class Command(BaseCommand):

	help = "Upload all existing data to TTV's RabbitMQ queue"

	def handle(self, *args, **options):
		catalog_records = CatalogRecord.objects.all()
		logger.info(f"found {catalog_records.count()} catalog records")
		aff_rows = 0

		for catalog_record in catalog_records:
			cr = CatalogRecordSerializer(catalog_record).data
			logger.info(f"Publishing catalog record {aff_rows} from TOTAL {catalog_records.count()}")
			RabbitMQService.publish(cr, routing_key='', exchange="TTV-datasets")
			aff_rows += 1
			logger.info(f"Published catalog record id={cr['identifier']}")

		logger.info(f"All catalog records published")

