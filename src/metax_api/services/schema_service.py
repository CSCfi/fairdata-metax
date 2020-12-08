# This file is part of the Metax API service
#
# Copyright 2017-2018 Ministry of Education and Culture, Finland
#
# :author: CSC - IT Center for Science Ltd., Espoo Finland <servicedesk@csc.fi>
# :license: MIT

import logging
from os import listdir
from os.path import isfile, join, abspath, dirname

from django.http import Http404
from rest_framework import status
from rest_framework.response import Response

from metax_api.services import CommonService as CS

_logger = logging.getLogger(__name__)
d = logging.getLogger(__name__).debug

class SchemaService():

    @classmethod
    def get_all_schemas(cls):
        schema_dir = cls._get_schema_dir()
        # Listing files that end with _schema.json.
        # The prefix is stripped from returned strings,
        # so that results can be used as is for retrieving specific
        # schema by reusing the get_json_schema function from CommonService.
        schema_files = [f[0:f.rfind('_schema.json')]
            for f in listdir(schema_dir)
            if isfile(join(schema_dir, f)) and f.endswith('_schema.json')]
        return Response(data={'count': len(schema_files), 'results': schema_files}, status=status.HTTP_200_OK)

    @classmethod
    def get_schema_content(cls, name):
        schema_dir = cls._get_schema_dir()
        file_path = schema_dir + "/" + name
        # Trying to make sure that the absolute path of the requested file
        # starts with the schema folder in order to prevent dangerous path
        # traversals.
        if abspath(file_path).startswith(schema_dir) and isfile('%s_schema.json' % file_path):
            return Response(CS.get_json_schema(schema_dir, name), status=status.HTTP_200_OK)
        else:
            raise Http404

    @staticmethod
    def _get_schema_dir():
        cur_dir = abspath(dirname(__file__))
        return abspath('%s/../api/rest/base/schemas' % cur_dir)
