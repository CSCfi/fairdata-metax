import logging

from django.core.management.base import BaseCommand
from django.db.models import Sum
from django.db.models.expressions import RawSQL

from metax_api.models import File, CatalogRecordV2, OrganizationStatistics, ProjectStatistics
from metax_api.api.rest.base.views import FileViewSet
from metax_api.services import FileService, StatisticService

logger = logging.getLogger(__name__)

class Command(BaseCommand):
	def handle(self, *args, **options):
		
		logger.info("Creating statistic summary")

		OrganizationStatistics.objects.all().delete()
		ProjectStatistics.objects.all().delete()


		ida_projects = File.objects.all().values("project_identifier").distinct()
		for project in ida_projects:
			project_id = project["project_identifier"]
			ret = StatisticService.count_files([project_id], include_pids=True)
			count = ret[0]["count"]
			size = ret[0]["byte_size"]
			file_pids = ret[1]

			if len(file_pids) == 0:
				catalog_records = ""
			else:
				catalog_records = FileService.get_identifiers(file_pids, "noparams", True, get_pids=True).data

			stat = ProjectStatistics(project_id, count, size, catalog_records)
			stat.save()


		organizations = CatalogRecordV2.objects.all().order_by().values("metadata_provider_org").distinct()

		for org in organizations:
			org_id = org["metadata_provider_org"]
			ret = StatisticService.count_datasets(metadata_provider_org=org_id)
			stat = OrganizationStatistics(org_id, ret["count"], ret["ida_byte_size"])
			stat.save()

		logger.info("Statistic summary created")