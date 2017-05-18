from json import load as json_load
from jsonschema import validate as json_validate
from os import path

from rest_framework.viewsets import ModelViewSet

def get_schema(view_file, model_name):
    """
    view_file is a __file___ variable
    """
    with open(path.dirname(view_file) + '/../schemas/json_schema_%s.json' % model_name) as f:
        return json_load(f)

class CommonViewSet(ModelViewSet):

    json_schema = False
    validate_json_schema = False

    def update(self, request, *args, **kwargs):
        if self.validate_json_schema:
            self._do_validate_json_schema(request)
        return super(CommonViewSet, self).update(request, *args, **kwargs)

    def partial_update(self, request, *args, **kwargs):
        if self.validate_json_schema:
            self._do_validate_json_schema(request)
        return super(CommonViewSet, self).partial_update(request, *args, **kwargs)

    def create(self, request, *args, **kwargs):
        if self.validate_json_schema:
            self._do_validate_json_schema(request)
        return super(CommonViewSet, self).create(request, *args, **kwargs)

    def use_json_schema_validation(self, view_file):
        """
        An inheriting class can call this method in its constructor if it wants to do
        json schema validation automatically for update and create methods.

        view_file is __file__ of the inheriting object. This way get_schema()
        always looks for the schema from a directory relative to the view's location,
        taking into account its version.
        """
        self.json_schema = get_schema(view_file, self.__class__.__name__.lower()[:-(len('viewset'))])
        self.validate_json_schema = True

    def _do_validate_json_schema(self, request):
        if not self.json_schema:
            raise Exception("schema missing")
        json_validate(request.data['json'], self.json_schema)
