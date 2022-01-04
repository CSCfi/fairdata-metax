import logging

from metax_api.models import File
from metax_api.services import StatisticService

logger = logging.getLogger(__name__)

def create_statistic_summary():
	logger.info("create_statistic_summary")
	
	ida_projects = File.objects.all().values("project_identifier").distinct()
	for project in ida_projects:
		logger.info(f"project: {project}")
		ret = StatisticService.count_files(project)
		logger.info(f"ret: {ret}")