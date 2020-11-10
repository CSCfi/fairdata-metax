from django.core.management.base import BaseCommand, CommandError

from metax_api.tasks.refdata.refdata_indexer.es_index_data import index_data

class Command(BaseCommand):
    def handle(self, *args, **options):
        index_data()
