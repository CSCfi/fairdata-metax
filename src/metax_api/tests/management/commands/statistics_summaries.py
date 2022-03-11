import logging

from django.core.management import call_command
from django.test import TestCase

from metax_api.models import OrganizationStatistics, ProjectStatistics
from metax_api.tests.utils import test_data_file_path

_logger = logging.getLogger(__name__)

class StatisticsSummariesTest(TestCase):
	@classmethod
	def setUpClass(cls):
		"""
		Loaded only once for test cases inside this class.
		"""
		call_command("loaddata", test_data_file_path, verbosity=0)
		call_command("create_statistic_report")

		super(StatisticsSummariesTest, cls).setUpClass()


	def testProjectStatistics(self):
		project_x_stats = ProjectStatistics.objects.get(project_identifier="project_x")
		research_project_112_stats = ProjectStatistics.objects.get(project_identifier="research_project_112")

		self.assertEqual(project_x_stats.count, 20)
		self.assertEqual(project_x_stats.byte_size, 21000)
		self.assertEqual(len(project_x_stats.published_datasets.split(",")), 20)

		self.assertEqual(research_project_112_stats.count, 88)
		self.assertEqual(research_project_112_stats.byte_size, 625000)
		self.assertEqual(len(research_project_112_stats.published_datasets.split(",")), 88)

	def testOrganizationStatistics(self):
		org_stats = OrganizationStatistics.objects.get(organization="abc-org-123")

		self.assertEqual(org_stats.count, 28)
		self.assertEqual(org_stats.byte_size, 710800)