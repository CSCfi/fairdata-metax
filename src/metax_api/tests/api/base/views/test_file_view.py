from django.test import TestCase
from metax_api.api.base.views import FileViewSet

class FileViewSetTests(TestCase):

    def test_has_json_schema_set_on_init(self):
        fvs = FileViewSet()
        self.assertEqual(isinstance(fvs.json_schema, dict), True, 'JSON schema missing after object init')
