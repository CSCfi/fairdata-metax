import logging

from django.core.management.base import BaseCommand
from django.db.models import Sum
from django.db.models.expressions import RawSQL

from metax_api.models import (
    File,
    CatalogRecordV2,
    OrganizationStatistics,
    ProjectStatistics,
    FileStorage,
)
from metax_api.api.rest.base.views import FileViewSet
from metax_api.services import FileService, StatisticService

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    def handle(self, *args, **options):

        logger.info("Creating statistic summary")

        OrganizationStatistics.objects.all().delete()
        ProjectStatistics.objects.all().delete()

        ida_file_storage = FileStorage.objects.get(
            file_storage_json__icontains="urn:nbn:fi:att:file-storage-ida"
        )
        pas_file_storage = FileStorage.objects.get(
            file_storage_json__icontains="urn:nbn:fi:att:file-storage-pas"
        )

        all_projects = File.objects.all().values("project_identifier").distinct()

        for project in all_projects:
            logger.info(f"all_project: {project}")
            project_id = project["project_identifier"]

            ida_files_stats = StatisticService.count_files(
                [project_id], removed="false", include_pids=True, file_storage=ida_file_storage
            )
            ida_count = ida_files_stats[0]["count"]
            ida_size = ida_files_stats[0]["byte_size"]
            ida_file_pids = ida_files_stats[1]
            ida_published_catalog_record_pids = self.get_published_cr_pids(ida_file_pids)

            pas_files_stats = StatisticService.count_files(
                [project_id], removed="false", include_pids=True, file_storage=pas_file_storage
            )
            pas_count = pas_files_stats[0]["count"]
            pas_size = pas_files_stats[0]["byte_size"]
            pas_file_pids = pas_files_stats[1]
            pas_published_catalog_record_pids = self.get_published_cr_pids(pas_file_pids)

            stat = ProjectStatistics(
                project_id,
                ida_count,
                ida_size,
                ida_published_catalog_record_pids,
                pas_count,
                pas_size,
                pas_published_catalog_record_pids,
            )
            stat.save()

        organizations = (
            CatalogRecordV2.objects.all().order_by().values("metadata_provider_org").distinct()
        )

        for org in organizations:
            org_id = org["metadata_provider_org"]
            ida_ret = StatisticService.count_datasets(metadata_provider_org=org_id, data_catalog="urn:nbn:fi:att:data-catalog-ida", removed=False, legacy=False)
            pas_ret = StatisticService.count_datasets(metadata_provider_org=org_id, data_catalog="urn:nbn:fi:att:data-catalog-pas", removed=False, legacy=False)
            att_ret = StatisticService.count_datasets(metadata_provider_org=org_id, data_catalog="urn:nbn:fi:att:data-catalog-att", removed=False, legacy=False)
            total_ret = StatisticService.count_datasets(metadata_provider_org=org_id, removed=False, legacy=False)

            total_count = total_ret["count"]
            ida_count = ida_ret["count"]
            pas_count = pas_ret["count"]
            att_count = att_ret["count"]
            other_count = total_count - ida_count - pas_count - att_count

            total_byte_size = total_ret["ida_byte_size"]
            ida_byte_size = ida_ret["ida_byte_size"]
            pas_byte_size = pas_ret["ida_byte_size"]

            stat = OrganizationStatistics(
                org_id,
                total_count,
                ida_count,
                pas_count,
                att_count,
                other_count,
                total_byte_size,
                ida_byte_size,
                pas_byte_size
            )
            stat.save()

        logger.info("Statistic summary created")

    def get_published_cr_pids(self, file_pids):

        if len(file_pids) == 0:
            return ""
        else:
            all_catalog_records = FileService.get_identifiers(file_pids, "noparams", True).data
            published_catalog_records = CatalogRecordV2.objects.filter(
                identifier__in=all_catalog_records, state="published"
            )
            return list(
                published_catalog_records.values_list(
                    "research_dataset__preferred_identifier", flat=True
                ).distinct()
            )
