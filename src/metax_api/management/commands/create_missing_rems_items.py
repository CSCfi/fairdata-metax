import logging

from django.core.management.base import BaseCommand

from metax_api.services.redis_cache_service import RedisClient
from metax_api.utils import ReferenceDataLoader
from metax_api.models import CatalogRecordV2
from metax_api.services.rems_service import REMSCatalogItemNotFoundException, REMSService

_logger = logging.getLogger(__name__)

class Command(BaseCommand):
    def handle(self, *args, **options):
        rems_service = REMSService()
        found_entity_count = 0
        created_entity_count = 0
        missing_entity_count = 0
        try:
            rems_crs = CatalogRecordV2.objects.filter(rems_identifier__isnull=False)
            _logger.info(f"Found {len(rems_crs)} CatalogRecords with rems_identifiers")
            for cr in rems_crs:
                try:
                    rems_service.get_rems_entity(cr)
                    found_entity_count += 1
                except REMSCatalogItemNotFoundException as e:
                    missing_entity_count += 1
                    if not cr.access_granter:
                        _logger.info(
                            f"Missing access_granter for {cr.identifier}, not creating REMS entity"
                        )
                        continue
                    if len(cr.research_dataset.get("access_rights", {}).get("license") or []) == 0:
                        _logger.info(
                            f"Missing license for {cr.identifier}, not creating REMS entity"
                        )
                        continue

                    _logger.info(
                        f"REMS entity {cr.rems_identifier} for dataset {cr.identifier} not found, creating"
                    )
                    rems_service.create_rems_entity(cr, cr.access_granter)
                    created_entity_count += 1

        except Exception as e:
            _logger.error(e)
            raise e

        _logger.info(f"CatalogRecords with existing REMS entities: {found_entity_count}")
        _logger.info(f"Missing REMS entities: {missing_entity_count}")
        _logger.info(f"Created REMS entities: {created_entity_count}")
