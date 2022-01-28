# This file is part of the Metax API service
#
# Copyright 2017-2018 Ministry of Education and Culture, Finland
#
# :author: CSC - IT Center for Science Ltd., Espoo Finland <servicedesk@csc.fi>
# :license: MIT

import logging

from django.conf import settings
from django.db import connection
from rest_framework import status
from rest_framework.decorators import action
from rest_framework.response import Response

from metax_api.exceptions import Http400
from metax_api.services import FileService

from .common_rpc import CommonRPC

_logger = logging.getLogger(__name__)


class FileRPC(CommonRPC):
    @action(detail=False, methods=["post"], url_path="delete_project")
    def delete_project(self, request):
        """
        Marks files deleted, deprecates related datasets and removes all directories.
        """
        if "project_identifier" not in request.query_params:
            raise Http400({"detail": ["required query parameter project_identifier missing"]})

        return FileService.delete_project(request.query_params["project_identifier"])

    @action(detail=False, methods=["post"], url_path="flush_project")
    def flush_project(self, request):
        """
        Permanently delete an entire project's files and directories.

        WARNING! Does not check file association with datasets! Not meant for active production use!!
        """
        if settings.ENV == "production":
            raise Http400({"detail": ["API currently allowed only in test environments"]})

        if "project_identifier" not in request.query_params:
            raise Http400({"detail": ["project_identifier is a required query parameter"]})

        project = request.query_params["project_identifier"]

        sql_delete_cr_files = """
            delete from metax_api_catalogrecord_files
            where file_id in (
                select id from metax_api_file where project_identifier = %s
            )
        """

        sql_delete_files = """
            delete from metax_api_file where project_identifier = %s
        """

        sql_delete_directories = """
            delete from metax_api_directory where project_identifier = %s
        """

        _logger.info(
            "Flushing project %s on the request of user: %s" % (project, request.user.username)
        )

        with connection.cursor() as cr:
            cr.execute(sql_delete_cr_files, [project])
            cr.execute(sql_delete_files, [project])
            cr.execute(sql_delete_directories, [project])
            if cr.rowcount == 0:
                _logger.info("No files or directories found for project %s" % project)
                return Response(project, status=status.HTTP_404_NOT_FOUND)

        _logger.info("Permanently deleted all files and directories from project %s" % project)

        return Response(data=None, status=status.HTTP_204_NO_CONTENT)
