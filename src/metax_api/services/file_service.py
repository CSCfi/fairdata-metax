# This file is part of the Metax API service
#
# Copyright 2017-2018 Ministry of Education and Culture, Finland
#
# :author: CSC - IT Center for Science Ltd., Espoo Finland <servicedesk@csc.fi>
# :license: MIT
import logging
from collections import defaultdict
from os import getpid
from os.path import basename, dirname
from time import time
from uuid import NAMESPACE_DNS as UUID_NAMESPACE_DNS, uuid3

from django.conf import settings
from django.db import connection
from django.http import Http404
from rest_framework import status
from rest_framework.response import Response
from rest_framework.serializers import ValidationError

from metax_api.exceptions import Http400, Http403
from metax_api.models import CatalogRecord, Directory, File, FileStorage
from metax_api.services import AuthService
from metax_api.services.pagination import DirectoryPagination
from metax_api.utils.utils import DelayedLog, get_tz_aware_now_without_micros

from .callable_service import CallableService
from .common_service import CommonService
from .reference_data_mixin import ReferenceDataMixin

DEBUG = settings.DEBUG
_logger = logging.getLogger(__name__)


"""
busting circular import issues...
note that the imports in below functions are only executed once, and
the function signatures are replaced with the imported classes. importing
multiple times in itself isnt bad, but using this function-replacement
-mechanic, so that the imports are not littered in several methods in
FileService where they would otherwise be needed.
"""


def DirectorySerializer(*args, **kwargs):
    from metax_api.api.rest.base.serializers import DirectorySerializer as DS

    DirectorySerializer = DS
    return DirectorySerializer(*args, **kwargs)


def FileSerializer(*args, **kwargs):
    from metax_api.api.rest.base.serializers import FileSerializer as FS

    FileSerializer = FS
    return FileSerializer(*args, **kwargs)


class MaxRecursionDepthExceeded(Exception):
    pass


class FileService(CommonService, ReferenceDataMixin):

    dp = DirectoryPagination()

    @staticmethod
    def check_user_belongs_to_project(request, project_identifier):
        if project_identifier not in AuthService.get_user_projects(request):
            raise Http403({"detail": ["You do not have access to this project."]})

    @classmethod
    def get_queryset_search_params(cls, request):
        """
        Get and validate parameters from request.query_params that will be used for filtering
        in view.get_queryset()
        """
        if not request.query_params:
            return {}

        queryset_search_params = {}

        if request.query_params.get("project_identifier", False):
            project = request.query_params["project_identifier"]
            if not request.user.is_service:
                cls.check_user_belongs_to_project(request, project)
            queryset_search_params["project_identifier"] = project

        if request.query_params.get("file_path", False):
            if not request.query_params.get("project_identifier", False):
                raise Http400(
                    "query parameter project_identifier is required when using file_path filter"
                )
            queryset_search_params["file_path__contains"] = request.query_params["file_path"]

        return queryset_search_params

    @classmethod
    def restore_files(cls, request, file_identifier_list):
        """
        Restore deleted files. If a file already has removed=false, does nothing.

        Only restores the files; does not touch datasets that might have been previously deprecated
        when a particular file was marked as removed.
        """
        _logger.info("Restoring files")

        if not file_identifier_list:
            _logger.info("Received file identifier list is empty - doing nothing")
            return Response({"files_restored_count": 0}, status=status.HTTP_200_OK)

        for id in file_identifier_list:
            if not isinstance(id, str):
                raise Http400(
                    {
                        "detail": [
                            "identifier values must be strings. found value '%s', which is of type %s"
                            % (id, type(id))
                        ]
                    }
                )

        _logger.info("Retrieving file details...")

        file_details_list = File.objects_unfiltered.filter(
            active=True, removed=True, identifier__in=file_identifier_list
        ).values("id", "identifier", "file_path", "project_identifier")

        if not len(file_details_list):
            _logger.info("None of the requested files were found")
            raise Http404

        elif len(file_details_list) < len(file_identifier_list):
            _logger.info("Some of the requested files were not found. Aborting")

            found_pids = {f["identifier"] for f in file_details_list}
            missing_identifiers = [pid for pid in file_identifier_list if pid not in found_pids]

            raise Http400(
                {
                    "detail": [
                        "not all requested file identifiers could be found. list of missing identifiers: %s"
                        % "\n".join(missing_identifiers)
                    ]
                }
            )
        elif len(file_details_list) > len(file_identifier_list):
            raise Http400(
                {
                    "detail": [
                        """
                    Found more files than were requested to be restored! Looks like there are some duplicates
                    within the removed files (same identifier exists more than once).

                    This should be a reasonably rare case that could only happen if uploading and deleting
                    the same files multiple times without regenerating identifiers inbetween, and then
                    attempting to restore. At this point, it is unclear which particular file of the many
                    available ones should be restored. If feasible, in this situation it may be best to properly
                    re-freeze those files entirely (so that new identifiers are generated).
                    """
                    ]
                }
            )
        else:
            # good case
            pass

        _logger.info("Validating restore is targeting only one project...")

        projects = {f["project_identifier"] for f in file_details_list}

        if len(projects) > 1:
            raise Http400(
                {
                    "detail": [
                        "restore operation should target one project at a time. the following projects were found: %s"
                        % ", ".join([p for p in projects])
                    ]
                }
            )

        # note: sets do not support indexing. getting the first (only) item here
        project_identifier = next(iter(projects))

        _logger.info(
            "Restoring files in project %s. Files to restore: %d"
            % (project_identifier, len(file_identifier_list))
        )

        # when files were deleted, any empty directories were deleted as well. check
        # and re-create directories, and assign new parent_directory_id to files being
        # restored as necessary.
        common_info = cls.update_common_info(request, return_only=True)
        file_details_with_dirs = cls._create_directories_from_file_list(
            common_info, file_details_list
        )

        # note, the purpose of %%s: request.user.username is inserted into the query in cr.execute() as
        # a separate parameter, in order to properly escape it.
        update_statements = [
            "(%d, %%s, %d)" % (f["id"], f["parent_directory"]) for f in file_details_with_dirs
        ]

        sql_restore_files = """
            update metax_api_file as file set
                service_modified = results.service_modified,
                parent_directory_id = results.parent_directory_id,
                removed = false,
                file_deleted = NULL,
                date_modified = CURRENT_TIMESTAMP,
                user_modified = NULL,
                date_removed = NULL
            from (values
                %s
            ) as results(id, service_modified, parent_directory_id)
            where results.id = file.id;
            """ % ",".join(
            update_statements
        )

        with connection.cursor() as cr:
            cr.execute(
                sql_restore_files,
                [request.user.username for i in range(len(file_details_list))],
            )
            affected_rows = cr.rowcount

        _logger.info("Restored %d files in project %s" % (affected_rows, project_identifier))

        cls.calculate_project_directory_byte_sizes_and_file_counts(project_identifier)

        return Response({"restored_files_count": affected_rows}, status=status.HTTP_200_OK)

    @classmethod
    def get_identifiers(cls, identifiers, params, keysonly):
        """
        keys='files': Find out which (non-deprecated) datasets a list of files belongs to, and return
        their preferred_identifiers per file as a list in json format.

        keys='datasets': Find out which files belong to a list of datasets, and return
        their preferred_identifiers per dataset as a list in json format.

        keysonly= for dataset return dataset ids that have files, for files return file ids that belong
        to some dataset

        Parameter identifiers can be a list of pk's (integers), or file/dataset identifiers (strings).
        """
        _logger.info("Retrieving detailed list of %s" % params)

        ids = cls.identifiers_to_ids(identifiers, params)
        if not ids:
            return Response([], status=status.HTTP_200_OK)

        _logger.info(
            "Searching return for the following %s (printing first 10):\n%s"
            % (params, "\n".join(str(id) for id in ids[:10]))
        )

        noparams = """
            SELECT cr.identifier
            FROM metax_api_catalogrecord cr
            INNER JOIN metax_api_catalogrecord_files cr_f
                ON catalogrecord_id = cr.id
            WHERE cr_f.file_id IN %s
                AND cr.removed = false AND cr.active = true AND cr.deprecated = false
            GROUP BY cr.identifier
            """

        files = """
            SELECT f.identifier, json_agg(cr.identifier)
            FROM metax_api_file f
            JOIN metax_api_catalogrecord_files cr_f
                ON f.id=cr_f.file_id
            JOIN metax_api_catalogrecord cr
                ON cr.id=cr_f.catalogrecord_id
            WHERE f.id IN %s
                AND cr.removed = false AND cr.active = true AND cr.deprecated = false
            GROUP BY f.id
            ORDER BY f.id ASC;
            """

        files_keysonly = files.replace(
            ", json_agg(cr.research_dataset->>'preferred_identifier')", ""
        )

        datasets = """
            SELECT cr.identifier, json_agg(f.identifier)
            FROM metax_api_file f
            JOIN metax_api_catalogrecord_files cr_f
                ON f.id=cr_f.file_id
            JOIN metax_api_catalogrecord cr
                ON cr.id=cr_f.catalogrecord_id
            WHERE cr.id IN %s
                AND cr.removed = false AND cr.active = true AND cr.deprecated = false
            GROUP BY cr.id
            ORDER BY cr.id ASC;
            """

        datasets_keysonly = datasets.replace(", json_agg(f.identifier)", "")

        if keysonly:
            sql = {
                "files": files_keysonly,
                "datasets": datasets_keysonly,
                "noparams": noparams,
            }
        else:
            sql = {"files": files, "datasets": datasets, "noparams": noparams}

        with connection.cursor() as cr:
            cr.execute(sql[params], [tuple(ids)])
            if cr.rowcount == 0:
                preferred_identifiers = []
                _logger.info("No %s found for list of input identifiers" % params)
            else:
                preferred_identifiers = cr.fetchall()
                _logger.info("Found following %s:\n%s" % (params, preferred_identifiers))

        if keysonly:
            list_of_keys = (
                []
            )  # This has to be here, cr.fetchall() returns a list of tuples which dict
            for tuples in preferred_identifiers:  # can't parse like below when second item is empty
                list_of_keys.append(tuples[0])
            return Response(list_of_keys, status=status.HTTP_200_OK)
        else:
            return Response(dict(preferred_identifiers), status=status.HTTP_200_OK)

    @classmethod
    def destroy_single(cls, file):
        """
        Mark a single file as removed. Marks related datasets deprecated, and deletes any empty
        directories above the file.
        """
        _logger.info("Begin delete file")

        deleted_files_count, project_identifier = cls._mark_files_as_deleted([file.id])
        cls._delete_empy_dir_chain_above(file.parent_directory)
        cls.calculate_project_directory_byte_sizes_and_file_counts(file.project_identifier)
        cls._mark_datasets_as_deprecated([file.id])

        CallableService.add_post_request_callable(
            DelayedLog(
                event="files_deleted",
                files={
                    "project_identifier": file.project_identifier,
                    "file_storage": file.file_storage.file_storage_json["identifier"],
                    "file_count": deleted_files_count,
                },
            )
        )

        _logger.info(
            "Marked %d files as deleted from project %s" % (deleted_files_count, project_identifier)
        )
        return Response({"deleted_files_count": deleted_files_count}, status=status.HTTP_200_OK)

    @classmethod
    def destroy_bulk(cls, file_identifiers):
        """
        Mark files as deleted en masse. Parameter file_identifiers can be a list of pk's
        (integers), or file identifiers (strings).

        A bulk delete request can concern either an entire directory and its subdirectories,
        or just some files in a directory, or all files in a single directory. Effectively,
        whatever files the request contains, should be deleted, while ensuring that
        the directory hierarchy remains valid for any files that are left after deleting.

        If the result of a bulk delete request is that none of the directories left
        contain any files, the all directories, including the root, will end up deleted.

        The method returns a http response with the number of deleted files in the body.
        """
        _logger.info("Begin bulk delete files")

        file_ids = cls.identifiers_to_ids(file_identifiers, "noparams")
        if not file_ids:
            raise Http404

        deleted_files_count, project_identifier = cls._mark_files_as_deleted(file_ids)

        cls._find_and_delete_empty_directories(project_identifier)
        cls.calculate_project_directory_byte_sizes_and_file_counts(project_identifier)
        cls._mark_datasets_as_deprecated(file_ids)

        file = File.objects_unfiltered.get(pk=file_ids[0])

        CallableService.add_post_request_callable(
            DelayedLog(
                event="files_deleted",
                files={
                    "project_identifier": file.project_identifier,
                    "file_storage": file.file_storage.file_storage_json["identifier"],
                    "file_count": deleted_files_count,
                },
            )
        )

        _logger.info(
            "Marked %d files as deleted from project %s" % (deleted_files_count, project_identifier)
        )
        return Response({"deleted_files_count": deleted_files_count}, status=status.HTTP_200_OK)

    @classmethod
    def delete_project(cls, project_id):
        """
        Marks files deleted, deprecates related datasets and removes all directories.

        This method is called by FileRPC
        """
        _logger.info("Begin to delete project from database...")

        file_ids = [
            id
            for id in File.objects.filter(project_identifier=project_id).values_list(
                "id", flat=True
            )
        ]

        deleted_files_count = 0

        if file_ids:
            deleted_files_count = cls._mark_files_as_deleted(file_ids)[0]
            cls._find_and_delete_empty_directories(project_id)
            cls.calculate_project_directory_byte_sizes_and_file_counts(project_id)
            cls._mark_datasets_as_deprecated(file_ids)
        else:
            _logger.info("Project %s contained no files" % project_id)

        _logger.info(
            "Deleted project %s successfully. %d files deleted" % (project_id, deleted_files_count)
        )

        return Response({"deleted_files_count": deleted_files_count}, status=status.HTTP_200_OK)

    @staticmethod
    def _mark_files_as_deleted(file_ids):
        """
        Mark files designated by file_ids as deleted.
        """
        _logger.info("Marking files as removed...")

        sql_delete_files = """
            update metax_api_file set
                removed = true, 
                file_deleted = CURRENT_TIMESTAMP, 
                date_modified = CURRENT_TIMESTAMP,
                date_removed = CURRENT_TIMESTAMP
            where active = true and removed = false
            and id in %s"""

        sql_select_related_projects = (
            "select distinct(project_identifier) from metax_api_file where id in %s"
        )

        with connection.cursor() as cr:
            cr.execute(sql_select_related_projects, [tuple(file_ids)])
            if cr.rowcount == 0:
                raise Http400({"detail": ["no files found for given identifiers"]})
            elif cr.rowcount > 1:
                raise Http400(
                    {
                        "project_identifier": [
                            "deleting files from more than one project in a single request is not allowed"
                        ]
                    }
                )
            project_identifier = cr.fetchone()[0]

            cr.execute(sql_delete_files, [tuple(file_ids)])
            deleted_files_count = cr.rowcount

        return deleted_files_count, project_identifier

    @classmethod
    def _find_and_delete_empty_directories(cls, project_identifier):
        """
        Find and delete, if feasible, all empty directories in a project. The only dir
        found in the project without a parent dir, is considered to be the root.
        """
        _logger.info("Finding and deleting empty directory chains...")

        root_dir = Directory.objects.filter(
            project_identifier=project_identifier, parent_directory_id=None
        )

        if root_dir.count() > 1:  # pragma: no cover
            raise ValidationError(
                {
                    "detail": [
                        "found more than one root dir (directories without parents: %s. unable to proceed"
                        % ", ".join(dr.directory_path for dr in root_dir)
                    ]
                }
            )

        cls._delete_empy_directories(root_dir[0])

    @classmethod
    def _delete_empy_dir_chain_above(cls, directory):
        """
        When deleting a single file, find out if directories above are empty, and delete them.
        """
        if not directory:
            return
        elif directory.files.exists():
            return
        elif directory.child_directories.exists():
            return
        parent_directory = directory.parent_directory
        directory.delete()
        if parent_directory:
            cls._delete_empy_dir_chain_above(parent_directory)

    @classmethod
    def _delete_empy_directories(cls, dr):
        """
        Find and delete any empty sub directories, and the directory itself. If any of the
        sub directories contained any files, directory is not deleted. This method is
        intended to be called recursively.

        Returns True if the given directory, and its subdirs were deleted, and False,
        if the directory or any of its sub directories contained files, preventing
        deleting the directory.
        """
        sub_dirs_can_be_deleted = False
        sub_dirs_are_empty = []

        sub_dirs = dr.child_directories.all()

        if sub_dirs.exists():
            # start deleting directories from the bottom to up
            for sub_dr in sub_dirs:
                sub_dirs_are_empty.append(cls._delete_empy_directories(sub_dr))

            if all(sub_dirs_are_empty):
                # if even one sub dir needs to be preserved, the current directory must be preserved.
                sub_dirs_can_be_deleted = True
        else:
            sub_dirs_can_be_deleted = True

        if dr.files.exists():
            return False
        elif sub_dirs_can_be_deleted:
            dr.delete()
            return True
        else:
            return False

    @staticmethod
    def _mark_datasets_as_deprecated(file_ids):
        """
        Get all CatalogRecords which have files set to them from file_ids,
        and set their deprecated flag to True. Then, publish update-messages to rabbitmq.
        """
        _logger.info("Marking related datasets as deprecated...")
        deprecated_records = []

        current_time = get_tz_aware_now_without_micros()

        records = (
            CatalogRecord.objects.filter(files__in=file_ids, deprecated=False)
            .exclude(data_catalog__catalog_json__identifier=settings.PAS_DATA_CATALOG_IDENTIFIER)
            .distinct("id")
        )

        for cr in records:
            cr.deprecate(current_time)
            deprecated_records.append(cr)
        if not deprecated_records:
            _logger.info("Files were not associated with any datasets.")
            return

        from metax_api.models.catalog_record import RabbitMQPublishRecord

        for cr in deprecated_records:
            cr.add_post_request_callable(RabbitMQPublishRecord(cr, "update"))

    @classmethod
    def get_directory_contents(
        cls,
        identifier=None,
        path=None,
        project_identifier=None,
        recursive=False,
        max_depth=1,
        dirs_only=False,
        include_parent=False,
        cr_identifier=None,
        not_cr_identifier=None,
        file_name=None,
        directory_name=None,
        paginate=None,
        request=None,
    ):
        """
        Get files and directories contained by a directory.

        Parameters:

        identifier: may be a pk, or an uuid value. Search using approriate fields.

        cr_identifier: may be used to browse files in the context of the given CatalogRecord id.
        Only those files and directories are retrieved, which have been
        selected for that CatalogRecord.

        not_cr_identifier: may be used to browse files in the context of the given CatalogRecord id.
        Only those files and directories are retrieved, which have NOT been
        selected for that CatalogRecord.

        path and project_identifier: may be specified to search directly by
        a known project and path, instead of a directory identifier. If identifier is passed,
        it is used instead of path and project.

        recursive: may be used to either get a flat list of all files below the
        directory, or a hierarchial directory tree with depth of max_depth.

        max_depth: has to be specified to a higher number than 1 for 'recursive' to go
        deeper than one level. 0 Does not return anything.

        dirs_only: may be set to True to retrieve only directories. If combined with
        'recursive', the returned directories will be hierarchial, and not a flat list, unlike
        with files.

        include_parent: may be set to True to include the 'parent' directory of the content
        being retrieved in the results. Example: /directories/3/files?include_parent=true also
        includes the data about directory id 3 in the results. Normally its data would not be
        present, and instead would need to be retrieved by calling /directories/3.

        file_name: substring search from file names. Only matching files are returned.
        Can be used with directory_name.

        directory_name: substring search from directory names. Only matching directories are returned.
        Can be used with file_name.

        request: the web request object.

        """
        assert request is not None, "kw parameter request must be specified"
        from metax_api.api.rest.base.serializers import LightDirectorySerializer

        # get targeted directory

        if identifier:
            try:
                params = {"id": int(identifier)}
            except ValueError:
                params = {"identifier": identifier}
        else:
            if path and project_identifier:
                params = {
                    "directory_path": path,
                    "project_identifier": project_identifier,
                }
            else:  # pragma: no cover
                raise ValidationError({"detail": ["no parameters to query by"]})

        try:
            fields = LightDirectorySerializer.ls_field_list()
            directory = Directory.objects.values(*fields).get(**params)
        except Directory.DoesNotExist:
            raise Http404

        cr_id = not_cr_id = None

        if cr_identifier:
            cr_id, cr_directory_data = cls._get_cr_if_relevant(cr_identifier, directory, request)
        elif not_cr_identifier:
            not_cr_id, cr_directory_data = cls._get_cr_if_relevant(
                not_cr_identifier, directory, request
            )
        else:
            # generally browsing the directory - NOT in the context of a cr! check user permissions
            if not request.user.is_service:
                cls.check_user_belongs_to_project(request, directory["project_identifier"])

            cr_directory_data = {}

        # get list of field names to retrieve. note: by default all fields are retrieved
        directory_fields, file_fields = cls._get_requested_file_browsing_fields(request)

        if cr_id and recursive and max_depth == "*" and not dirs_only:
            # optimized for downloading full file list of an entire directory
            files = cls._get_directory_file_list_recursively_for_cr(directory, cr_id, file_fields)
            if paginate:
                dirs, files = cls.dp.paginate_directory_data(None, files, request)
                return cls.dp.get_paginated_response(files)
            return files

        exclude_id = False
        if (recursive or not_cr_id) and directory_fields and "id" not in directory_fields:
            exclude_id = True
            directory_fields.append("id")

        contents = cls._get_directory_contents(
            directory["id"],
            recursive=recursive,
            max_depth=max_depth,
            dirs_only=dirs_only,
            cr_id=cr_id,
            not_cr_id=not_cr_id,
            directory_fields=directory_fields,
            file_fields=file_fields,
            file_name=file_name,
            directory_name=directory_name,
            paginate=paginate,
            request=request,
        )

        def _remove_id(dirs):
            for dir in dirs["directories"]:
                if dir.get("directories"):
                    _remove_id(dir)
                dir.pop("id")

        if exclude_id:
            _remove_id(contents)

        if recursive:
            if paginate:
                if dirs_only:
                    (
                        contents["directories"],
                        contents["files"],
                    ) = cls.dp.paginate_directory_data(contents["directories"], None, request)
                    del contents["files"]
                else:
                    dirs, files = cls.dp.paginate_directory_data(None, contents["files"], request)
                    return cls.dp.get_paginated_response(files)
            if dirs_only:
                # taken care of the in the called methods. can return the result as is
                # as a directory tree
                pass
            else:
                return contents.get("files", [])

        if include_parent:
            contents.update(LightDirectorySerializer.serialize(directory))

        if cls._include_total_byte_sizes_and_file_counts(cr_id, not_cr_id, directory_fields):
            cls.retrieve_directory_byte_sizes_and_file_counts_for_cr(
                contents, not_cr_id, directory_fields, cr_directory_data
            )

        if paginate:
            contents = cls.dp.get_paginated_response(contents)

        return contents

    @classmethod
    def _get_cr_if_relevant(cls, cr_identifier, directory, request):
        # browsing in the context of a cr
        try:
            cr_params = {"id": int(cr_identifier)}
        except ValueError:
            cr_params = {"identifier": cr_identifier}

        try:
            cr = CatalogRecord.objects.only(
                "id", "_directory_data", "user_created", "research_dataset"
            ).get(**cr_params)
        except CatalogRecord.DoesNotExist:
            # raise 400 instead of 404, to distinguish from the error
            # 'directory not found', which raises a 404
            raise ValidationError(
                {"detail": ["CatalogRecord with identifier %s does not exist" % cr_identifier]}
            )

        if not cr.authorized_to_see_catalog_record_files(request):
            raise Http403(
                {
                    "detail": [
                        "You do not have permission to see this information because the dataset access type is "
                        "not open and you are not the owner of the catalog record."
                    ]
                }
            )

        cr_id = cr.id
        cr_directory_data = cr._directory_data or {}

        return (cr_id, cr_directory_data)

    @classmethod
    def _get_requested_file_browsing_fields(cls, request):
        """
        Find out if only specific fields were requested to be returned, and return those fields
        for directories and files respectively.
        """
        from metax_api.api.rest.base.serializers import (
            LightDirectorySerializer,
            LightFileSerializer,
        )

        directory_fields = []
        file_fields = []

        if request.query_params.get("directory_fields", False):
            directory_fields = request.query_params["directory_fields"].split(",")

        if request.query_params.get("file_fields", False):
            file_fields = request.query_params["file_fields"].split(",")

        directory_fields = LightDirectorySerializer.ls_field_list(directory_fields)
        file_fields = LightFileSerializer.ls_field_list(file_fields)

        return directory_fields, file_fields

    @staticmethod
    def _get_directory_file_list_recursively_for_cr(directory, cr_id, file_fields):
        """
        Optimized for downloading full file list of an entire directory in a cr.
        Not a recursive method + no Model objects or normal serializers.
        """
        from metax_api.api.rest.base.serializers import LightFileSerializer

        params = {"project_identifier": directory["project_identifier"]}

        if directory["directory_path"] == "/":
            # for root dir, simply omit file_path param to get all project files.
            pass
        else:
            params["file_path__startswith"] = "%s/" % directory["directory_path"]

        files = CatalogRecord.objects.get(pk=cr_id).files.values(*file_fields).filter(**params)
        return LightFileSerializer.serialize(files)

    @staticmethod
    def _include_total_byte_sizes_and_file_counts(cr_id, not_cr_id, directory_fields):
        if not any([cr_id, not_cr_id]):
            # totals are counted only when browsing files for a specific record.
            # for non-cr file browsing, those numbers have been counted when the
            # files were first created.
            return False
        if not directory_fields:
            # specific fields not specified -> all fields are returned
            return True
        if "byte_size" in directory_fields or "file_count" in directory_fields:
            return True
        return False

    @classmethod
    def _get_directory_contents(
        cls,
        directory_id,
        request=None,
        recursive=False,
        max_depth=1,
        depth=0,
        dirs_only=False,
        cr_id=None,
        not_cr_id=None,
        directory_fields=[],
        file_fields=[],
        file_name=None,
        directory_name=None,
        paginate=None,
    ):
        """
        Get files and directories contained by a directory.

        If recursively requested, collects all files and dirs below the directory.

        If cr_id is provided, only those files and directories are retrieved, which have been
        selected for that CatalogRecord.

        If not_cr_id is provided, only those files and directories are retrieved, which have not been
        selected for that CatalogRecord.

        If directory_fields and/or file_fields are specified, then only specified fields are retrieved
        for directories and files respectively.
        """

        if recursive and max_depth != "*":
            if depth > max_depth:
                raise MaxRecursionDepthExceeded("max depth is %d" % max_depth)
            depth += 1

        if cr_id or not_cr_id:
            try:
                dirs, files = cls._get_directory_contents_for_catalog_record(
                    directory_id,
                    cr_id,
                    not_cr_id,
                    file_name=file_name,
                    directory_name=directory_name,
                    recursive=recursive,
                    dirs_only=dirs_only,
                    directory_fields=directory_fields,
                    file_fields=file_fields,
                )

            except Http404:
                if recursive:
                    return {"directories": []}
                raise
        else:
            # browsing from ALL files, not cr specific
            dirs = (
                Directory.objects.filter(parent_directory_id=directory_id)
                .order_by("directory_path")
                .values(*directory_fields)
            )

            # icontains returns exception on None and with empty string does unnecessary db hits
            if directory_name:
                dirs = dirs.filter(directory_name__icontains=directory_name)

            if dirs_only:
                files = None

            else:
                files = (
                    File.objects.filter(parent_directory_id=directory_id)
                    .order_by("file_path")
                    .values(*file_fields)
                )
                if file_name:
                    files = files.filter(file_name__icontains=file_name)

        if paginate and not recursive:
            dirs, files = cls.dp.paginate_directory_data(dirs, files, request)

        from metax_api.api.rest.base.serializers import LightDirectorySerializer

        contents = {"directories": LightDirectorySerializer.serialize(dirs)}

        if files or not dirs_only:
            # for normal file browsing (not with 'dirs_only'), the files-key should be present,
            # even if empty.
            from metax_api.api.rest.base.serializers import LightFileSerializer

            contents["files"] = LightFileSerializer.serialize(files)

        if recursive:
            for directory in contents["directories"]:
                try:
                    sub_dir_contents = cls._get_directory_contents(
                        directory["id"],
                        recursive=recursive,
                        max_depth=max_depth,
                        depth=depth,
                        dirs_only=dirs_only,
                        cr_id=cr_id,
                        not_cr_id=not_cr_id,
                        directory_fields=directory_fields,
                        file_fields=file_fields,
                        file_name=file_name,
                        directory_name=directory_name,
                        paginate=paginate,
                        request=request,
                    )
                except MaxRecursionDepthExceeded:
                    continue

                directory["directories"] = sub_dir_contents["directories"]

                if "files" in sub_dir_contents:
                    contents["files"] += sub_dir_contents["files"]

        return contents

    @classmethod
    def _get_directory_contents_for_catalog_record(
        cls,
        directory_id,
        cr_id,
        not_cr_id,
        file_name,
        directory_name,
        recursive,
        dirs_only=False,
        directory_fields=[],
        file_fields=[],
    ):
        """
        Browsing files in the context of a specific CR id.
        """

        def _cr_belongin_to_directory(id):
            if recursive and not dirs_only:
                return Directory.objects.filter(parent_directory_id=directory_id).values("id")

            # select dirs which are contained by the directory,
            # AND which contain files belonging to the cr <-> files m2m relation table,
            # AND there exists files for CR which begin with the same path as the dir path,
            # to successfully also include files which did not DIRECTLY contain any files, but do
            # contain files further down the tree.
            # dirs which otherwise contained files, but not any files that were selected for
            # the cr, are not returned.

            # directory_fields are validated in LightDirectorySerializer against allowed fields
            # which is set(DirectorySerializer.Meta.fields). Should be safe to use in raw SQL,
            # but considered to be more safe to sanitize here once more.

            from metax_api.api.rest.base.serializers import DirectorySerializer

            allowed_fields = set(DirectorySerializer.Meta.fields)

            directory_fields_sql = []

            for field in directory_fields:
                if field in allowed_fields:
                    directory_fields_sql.append("d." + field)
                elif (
                    "parent_directory__" in field
                    and field.split("parent_directory__")[1] in allowed_fields
                ):
                    directory_fields_sql.append(field.replace("parent_directory__", "parent_d."))
            directory_fields_string_sql = ", ".join(directory_fields_sql)

            dir_name_sql = (
                ""
                if not directory_name or not_cr_id
                else "AND d.directory_name LIKE ('%%' || %s || '%%')"
            )

            sql_select_dirs_for_cr = """
                SELECT {}
                FROM metax_api_directory d
                JOIN metax_api_directory parent_d
                    ON d.parent_directory_id = parent_d.id
                WHERE d.parent_directory_id = %s
                    {}
                AND EXISTS(
                    SELECT 1
                    FROM metax_api_file f
                    INNER JOIN metax_api_catalogrecord_files cr_f ON cr_f.file_id = f.id
                    WHERE f.file_path LIKE (d.directory_path || '/%%')
                    AND cr_f.catalogrecord_id = %s
                    AND f.removed = false
                    AND f.active = true
                )
                ORDER BY d.directory_path
                """
            with connection.cursor() as cr:
                sql_select_dirs_for_cr = sql_select_dirs_for_cr.format(
                    directory_fields_string_sql, dir_name_sql
                )
                sql_params = (
                    [directory_id, directory_name, id]
                    if directory_name and not not_cr_id
                    else [directory_id, id]
                )
                cr.execute(sql_select_dirs_for_cr, sql_params)

                dirs = [dict(zip(directory_fields, row)) for row in cr.fetchall()]

            return dirs

        if cr_id:
            dirs = _cr_belongin_to_directory(cr_id)

            files = (
                None
                if dirs_only
                else File.objects.filter(record__pk=cr_id, parent_directory=directory_id)
                .order_by("file_path")
                .values(*file_fields)
            )

        elif not_cr_id:
            dirs = _cr_belongin_to_directory(not_cr_id)

            if dirs_only or not recursive:
                dirs = (
                    Directory.objects.filter(parent_directory=directory_id)
                    .exclude(id__in=[dir["id"] for dir in dirs])
                    .values(*directory_fields)
                )
                if directory_name:
                    dirs = dirs.filter(directory_name__icontains=directory_name)

                if directory_name:
                    dirs = dirs.filter(directory_name__icontains=directory_name)

            files = (
                None
                if dirs_only
                else File.objects.exclude(record__pk=not_cr_id)
                .filter(parent_directory=directory_id)
                .order_by("file_path")
                .values(*file_fields)
            )

        if not dirs and not files:
            # for this specific version of the record, the requested directory either
            # didnt exist, or it was not selected
            raise Http404

        # icontains returns exception on None and with empty string does unnecessary db hits
        if files and file_name:
            files = files.filter(file_name__icontains=file_name)
        return dirs, files

    @classmethod
    def retrieve_directory_byte_sizes_and_file_counts_for_cr(
        cls, directory, not_cr_id=None, directory_fields=[], cr_directory_data={}
    ):
        """
        Retrieve total byte size and file counts of a directory, sub-directories included,
        in the context of a specific catalog record.
        Note: Called recursively.
        """
        if not directory_fields:
            BYTE_SIZE = FILE_COUNT = True
        else:
            BYTE_SIZE = "byte_size" in directory_fields
            FILE_COUNT = "file_count" in directory_fields

        if len(directory.get("directories", [])):
            for sub_dir in directory.get("directories", []):
                cls.retrieve_directory_byte_sizes_and_file_counts_for_cr(
                    sub_dir, not_cr_id, directory_fields, cr_directory_data
                )

        if "id" in directory:
            # bottom dir - retrieve total byte_size and file_count for this cr
            current_dir = cr_directory_data.get(str(directory["id"]), [0, 0])

            if not_cr_id:
                dr_total = Directory.objects.values("id", "byte_size", "file_count").get(
                    id=directory["id"]
                )

            if BYTE_SIZE:
                if not_cr_id:
                    directory["byte_size"] = dr_total["byte_size"] - current_dir[0]
                else:
                    directory["byte_size"] = current_dir[0]

            if FILE_COUNT:
                if not_cr_id:
                    directory["file_count"] = dr_total["file_count"] - current_dir[1]
                else:
                    directory["file_count"] = current_dir[1]

    @classmethod
    def get_project_root_directory(cls, project_identifier):
        """
        Return root directory for a project, with its child directories and files.
        """
        from metax_api.api.rest.base.serializers import LightDirectorySerializer

        directory_fields = LightDirectorySerializer.ls_field_list()
        try:
            root_dir = Directory.objects.values(*directory_fields).get(
                project_identifier=project_identifier, parent_directory=None
            )
        except Directory.DoesNotExist:
            raise Http404
        except Directory.MultipleObjectsReturned:  # pragma: no cover
            raise Exception(
                "Directory.MultipleObjectsReturned when looking for root directory. This should never happen"
            )
        root_dir_json = LightDirectorySerializer.serialize(root_dir)
        root_dir_json.update(
            cls._get_directory_contents(root_dir["id"], directory_fields=directory_fields)
        )
        return root_dir_json

    @classmethod
    def _create_single(cls, common_info, initial_data, serializer_class, **kwargs):
        """
        Override the original _create_single from CommonService to also create directories,
        and setting them as parent_directory to approriate dirs and the file, before creating.
        """
        _logger.info("Begin create single file")

        cls._check_errors_before_creating_dirs([initial_data])

        # the same initial data as received, except it has parent_directory set also
        initial_data_with_dirs = cls._create_directories_from_file_list(
            common_info, [initial_data], **kwargs
        )

        res = super(FileService, cls)._create_single(
            common_info, initial_data_with_dirs[0], serializer_class, **kwargs
        )

        cls.calculate_project_directory_byte_sizes_and_file_counts(
            initial_data["project_identifier"]
        )

        CallableService.add_post_request_callable(
            DelayedLog(
                event="files_created",
                user_id=initial_data.get("user_created", res[0]["service_created"]),
                files={
                    "project_identifier": initial_data["project_identifier"],
                    "file_storage": str(initial_data["file_storage"]),
                    "file_count": 1,
                },
            )
        )

        _logger.info("Created 1 new files")

        return res

    @classmethod
    def check_allowed_projects(cls, request):
        allowed_projects = CommonService.get_list_query_param(request, "allowed_projects")

        if allowed_projects is not None:
            if not isinstance(request.data, list):
                raise Http400({"detail": ["request message body must be a single json object"]})

            try:
                file_ids = [f["identifier"] for f in request.data]
            except KeyError:
                raise Http400({"detail": ["File identifier is missing"]})

            project_ids = [
                pid
                for pid in File.objects.filter(identifier__in=file_ids)
                .values_list("project_identifier", flat=True)
                .distinct("project_identifier")
            ]

            if not all(pid in allowed_projects for pid in project_ids):
                raise Http403({"detail": ["You do not have permission to update this file"]})

    @classmethod
    def _create_bulk(cls, common_info, initial_data_list, results, serializer_class, **kwargs):
        """
        Override the original _create_bulk from CommonService to also create directories,
        and setting them as parent_directory to approriate files, before creating the files.
        """
        _logger.info("Begin bulk create files")

        cls._check_errors_before_creating_dirs(initial_data_list)
        file_list_with_dirs = cls._create_directories_from_file_list(
            common_info, initial_data_list, **kwargs
        )

        _logger.info("Creating files...")

        cls._create_files(common_info, file_list_with_dirs, results, serializer_class, **kwargs)

        cls.calculate_project_directory_byte_sizes_and_file_counts(
            initial_data_list[0]["project_identifier"]
        )

        CallableService.add_post_request_callable(
            DelayedLog(
                event="files_created",
                user_id=initial_data_list[0].get("user_created", common_info["service_created"]),
                files={
                    "project_identifier": initial_data_list[0]["project_identifier"],
                    "file_storage": str(initial_data_list[0]["file_storage"]),
                    "file_count": len(results.get("success", [])),
                },
            )
        )

        _logger.info("Created %d new files" % len(results.get("success", [])))

    @classmethod
    def _create_files(cls, common_info, initial_data_list, results, serializer_class, **kwargs):
        """
        The actual part where the list is iterated and objects validated, and created.
        """
        project_identifier = initial_data_list[0]["project_identifier"]

        # pre-fetch all file_paths in the project, to spare an individual db fetch for each file
        # in serializer.save(), where they otherwise would check for path presence.
        project_file_paths = File.objects.filter(project_identifier=project_identifier).values_list(
            "file_path", flat=True
        )
        project_file_paths = set(project_file_paths)

        project_dir_paths = set(dirname(f["file_path"]) for f in initial_data_list)

        project_dir_data_list = Directory.objects.filter(
            project_identifier=project_identifier, directory_path__in=project_dir_paths
        ).values_list("id", "identifier")

        project_dir_data = {dr[0]: dr[1] for dr in project_dir_data_list}

        file_storage_id = None
        file_storage_identifier = None
        entries = []

        def to_model_format(entry, common_info):
            """
            Format that is inserted into db.
            """
            del entry["checksum"]
            entry["file_storage_id"] = entry["file_storage"]
            del entry["file_storage"]
            entry["parent_directory_id"] = entry["parent_directory"]
            del entry["parent_directory"]
            entry.update(**common_info)  # add date_created, service_created etc fields

        def to_repr(entry, common_info, project_dir_data, file_storage_identifier):
            """
            Format that is returned in the response.
            """
            entry["file_storage"] = {
                "id": entry["file_storage_id"],
                "identifier": file_storage_identifier,
            }
            entry["parent_directory"] = {
                "id": entry["parent_directory_id"],
                "identifier": project_dir_data[entry["parent_directory_id"]],
            }

            del entry["file_storage_id"]
            del entry["parent_directory_id"]

            for field in common_info.keys():
                # cast datetime objects into strings
                entry[field] = str(entry[field])

            entry["checksum"] = serializer_class.form_checksum(entry)

        if DEBUG:
            start = time()

        for i, row in enumerate(initial_data_list):

            if file_storage_id:
                # saves a fetch to db in serializer.is_valid(), once file_storage_id has been retrieved
                # for one of the files.
                row["file_storage"] = file_storage_id

            serializer = serializer_class(data=row, **kwargs)

            if row["file_path"] not in project_file_paths:
                # saves a fetch to db in serializer.is_valid()
                serializer.file_path_checked = True
            else:
                # looks like path already exists in the project scope... let the serializer
                # confirm it on its own, and raise an error along the standard validations.
                # if the identifier also already exists, the error may be ignored if parameter
                # ignore_already_exists_errors is used.
                pass

            try:
                serializer.is_valid(raise_exception=True)
            except Exception as e:
                if CommonService.get_boolean_query_param(
                    kwargs["context"]["request"], "ignore_already_exists_errors"
                ):
                    if cls._error_is_already_exists(e):
                        # add only a minuscule response informing of the situation...
                        results["success"].append(
                            {
                                "object": {
                                    "identifier": row["identifier"],
                                    "detail": ["already exists"],
                                }
                            }
                        )
                        continue
                cls._append_error(results, serializer, e)
            else:
                entry = serializer.initial_data
                to_model_format(entry, common_info)
                entries.append(File(**entry))
                file_storage_id = entry["file_storage_id"]  # re-used for following loops

                if file_storage_identifier is None:
                    # re-used for following loops
                    file_storage_identifier = FileStorage.objects.get(
                        pk=file_storage_id
                    ).file_storage_json["identifier"]

                to_repr(entry, common_info, project_dir_data, file_storage_identifier)
                results["success"].append({"object": entry})

                if i % 1000 == 0:
                    # pros and cons of Model.objects.bulk_create():
                    # pros:
                    # - less db calls... MUCH faster
                    # cons:
                    # - Model.save() is not called. luckily File does not implement anything custom there
                    # - uses a bit more memory since entries[] is accumulated. control by adjusting batch size
                    # - the values returned to the requestor do not look identical to serializer.data. currently
                    #   low impact though, as no service is inspecting it anyway.
                    File.objects.bulk_create(entries)
                    entries = []

                    # for large amounts of data, the process slows down considerably as the process continues...
                    # closing and re-opening the connection helps the speed stay constant. downside: unable
                    # to create files inside a transaction. for rare cases where retries are made
                    # with the same data where the requestor never got the previousy attempt's result back,
                    # the requestor can use the parameter ignore_already_exists_errors to be spared from
                    # uninteresting errors.
                    if not connection.in_atomic_block:
                        # do not close when in atomic block! should only happen when query param
                        # ?dryrun=true is specified, or when inside test cases
                        connection.close()
                        connection.connect()

                    if DEBUG:
                        end = time()
                        _logger.debug(
                            "processed %d files... (%.3f seconds per batch)" % (i, end - start)
                        )
                        start = time()

        if entries:
            _logger.debug("a final dose of %d records still left to bulk_create..." % len(entries))
            File.objects.bulk_create(entries)
            _logger.debug("done!")

        if DEBUG:
            end = time()
            _logger.debug(
                "total time for inserting %d files: %d seconds"
                % (len(initial_data_list), (end - start))
            )

    @staticmethod
    def _error_is_already_exists(e):
        """
        Check if the error 'identifier already exists' was raised. There may have been other errors
        included, but they may be a symptom of the record already existing, so we don't care about them.
        """
        if hasattr(e, "detail"):
            for field_name, errors in e.detail.items():
                if field_name == "identifier" and "already exists" in errors[0]:
                    return True
        return False

    @staticmethod
    def _check_errors_before_creating_dirs(initial_data_list):
        """
        Check for errors concerning these fields, since creating directories counts on them.
        FileSerializer is_valid() is called a lot later for more thorough error checking.

        Errors here would realistically be an extremely rare case of error from the requestor's side,
        but doing the minimum necessary here anyway since we can't count on FileSerializer.is_valid()
        """
        for row in initial_data_list:
            # ic(row)
            if "file_path" not in row:
                raise Http400(
                    {
                        "file_path": [
                            "file_path is a required parameter (file id: %s)" % row["identifier"]
                        ]
                    }
                )
            else:
                if row["file_path"][0] != "/":
                    raise Http400(
                        {
                            "file_path": [
                                "file path should start with '/' to point to the root. Now '%s'"
                                % row["file_path"]
                            ]
                        }
                    )

            if "project_identifier" not in row:
                raise Http400(
                    {
                        "project_identifier": [
                            "project_identifier is a required parameter (file id: %s)"
                            % row["identifier"]
                        ]
                    }
                )

        if len(set(f["project_identifier"] for f in initial_data_list)) > 1:
            raise Http400(
                {
                    "project_identifier": [
                        "creating files for multiple projects in one request is not permitted."
                    ]
                }
            )

    @classmethod
    def _create_directories_from_file_list(cls, common_info, initial_data_list, **kwargs):
        """
        IDA does not give metax information about directories associated with the files
        as separate entities in the request, so they have to be created based on the
        paths in the list of files.
        """
        _logger.info("Checking and creating file hierarchy...")

        project_identifier = initial_data_list[0]["project_identifier"]
        sorted_data = sorted(initial_data_list, key=lambda row: row["file_path"])
        received_dir_paths = sorted(set(dirname(f["file_path"]) for f in sorted_data))

        new_dir_paths, existing_dirs = cls._get_new_and_existing_dirs(
            received_dir_paths, project_identifier
        )

        if new_dir_paths:
            cls._create_directories(
                common_info, existing_dirs, new_dir_paths, project_identifier, **kwargs
            )

        cls._assign_parents_to_files(existing_dirs, sorted_data)

        _logger.info("Directory hierarchy in place")
        return sorted_data

    @classmethod
    def _get_new_and_existing_dirs(cls, received_dir_paths, project_identifier):
        """
        From the received file paths list, find out which directories already exist in the db,
        and which need to be created.
        """
        received_dir_paths = cls._get_unique_dir_paths(received_dir_paths)

        # get existing dirs as a dict of { 'directory_path': 'id' }
        existing_dirs = dict(
            (dr.directory_path, dr.id)
            for dr in Directory.objects.filter(
                directory_path__in=received_dir_paths,
                project_identifier=project_identifier,
            ).order_by("directory_path")
        )

        new_dir_paths = [path for path in received_dir_paths if path not in existing_dirs]
        _logger.info(
            "Found %d existing, %d new directory paths" % (len(existing_dirs), len(new_dir_paths))
        )
        return new_dir_paths, existing_dirs

    def _get_unique_dir_paths(received_dir_paths):
        """
        Those dirs that are only used to organize other dirs, i.e. does not contain files actually,
        need to be weeded out from the list of received dir paths, to get all required directories
        that need to be created.
        """
        all_paths = {"/": True}

        for path in received_dir_paths:
            parent_path = dirname(path)
            while parent_path != "/":
                if parent_path not in all_paths:
                    all_paths[parent_path] = True
                parent_path = dirname(parent_path)
            all_paths[path] = True

        return sorted(all_paths.keys())

    @classmethod
    def _create_directories(
        cls, common_info, existing_dirs, new_dir_paths, project_identifier, **kwargs
    ):
        """
        Create to db directory hierarchy from directories extracted from the received file list.

        Save created paths/id's to existing_dirs dict, so the results can be efficiently re-used
        by other dirs being created, and later by the files that are created.

        It is possible, that no new directories are created at all, when appending files to an
        existing dir.
        """
        _logger.info("Creating directories...")
        python_process_pid = str(getpid())

        for i, path in enumerate(new_dir_paths):

            directory = {
                "directory_path": path,
                "directory_name": basename(path) if path != "/" else "/",
                # identifier: uuid3 as hex, using as salt time in ms, idx of loop, and python process id
                "identifier": uuid3(
                    UUID_NAMESPACE_DNS,
                    "%d%d%s" % (int(round(time() * 1000)), i, python_process_pid),
                ).hex,
                "project_identifier": project_identifier,
            }

            directory.update(common_info)

            if path != "/":
                cls._find_parent_dir_from_previously_created_dirs(directory, existing_dirs)

            serializer = DirectorySerializer(data=directory, **kwargs)
            serializer.is_valid(raise_exception=True)
            serializer.save()
            existing_dirs[serializer.data["directory_path"]] = serializer.data["id"]

    @classmethod
    def _assign_parents_to_files(cls, existing_dirs, sorted_data):
        """
        Using the previously created dirs, assign parent_directory to
        each file in the received file list.

        Assigning parent_directory is not allowed for requestors, so all existing
        parent_directories in the received files are purged.
        """
        _logger.info("Assigning parent directories to files...")

        for row in sorted_data:
            row.pop("parent_directory", None)

        for row in sorted_data:
            cls._find_parent_dir_from_previously_created_dirs(row, existing_dirs)

        return sorted_data

    @staticmethod
    def _find_parent_dir_from_previously_created_dirs(node, existing_dirs):
        """
        Parameter existing_dirs contains key-values as (directory_path, id).

        Find the approriate directory id to use as parent_directory, using the node's
        dirname(file_path) or dirname(directory_path) as key.
        """
        node_path = node.get("file_path", node.get("directory_path", None))

        if node_path == "/":
            return

        expected_parent_dir_path = dirname(node_path)

        try:
            node["parent_directory"] = existing_dirs[expected_parent_dir_path]
        except KeyError:  # pragma: no cover
            raise Exception(
                "No parent found for path %s, even though existing_dirs had stuff "
                "in it. This should never happen"
                % node.get("file_path", node.get("directory_path", None))
            )

    @staticmethod
    def calculate_project_directory_byte_sizes_and_file_counts(project_identifier):
        """
        (Re-)calculate directory byte sizes and file counts in this project.
        """
        try:
            project_root_dir = Directory.objects.get(
                project_identifier=project_identifier, parent_directory_id=None
            )
        except Directory.DoesNotExist:
            # root directory does not exist - all project files have been deleted
            pass
        else:
            project_root_dir.calculate_byte_size_and_file_count()

    @classmethod
    def validate_file_characteristics_reference_data(cls, file_characteristics, cache):
        reference_data = cls.get_reference_data(cache).get("reference_data", None)
        errors = defaultdict(list)

        if "file_format" in file_characteristics:
            ff = file_characteristics["file_format"]
            fv = file_characteristics.get("format_version", "")
            versions = cls._validate_file_format_and_get_versions_from_reference_data(
                reference_data["file_format_version"], ff, errors
            )

            # If the given file_format is a valid value, proceed to checking the given format_version value
            if not errors:
                # If the given file_format had several output_format_version values in refdata, but the given
                # format_version is not one of them, it's an error
                if versions and fv not in versions:
                    errors["file_characteristics.format_version"].append(
                        "Value '{0}' for format_version does not match the allowed values for the given "
                        "file_format value '{1}' in reference data".format(fv, ff)
                    )
                # If the given file_format did not have any output_format_version values in refdata, but the given
                # format_version is non-empty, it's an error
                elif not versions and fv:
                    errors["file_characteristics.format_version"].append(
                        "Any non-empty value for format_version not allowed for the given file_format value "
                        "'{0}' in reference data".format(ff)
                    )
        # If format_version was given but no file_format was given, it's an error
        elif "format_version" in file_characteristics:
            errors["file_characteristics.file_format"].append("Value missing")

        if errors:
            raise ValidationError(errors)

    @staticmethod
    def _validate_file_format_and_get_versions_from_reference_data(
        file_format_version_refdata, input_file_format, errors={}
    ):
        """
        Check if the input_file_format value exists in file_format_version reference data, and if it does,
        return a list of all possible output_format_version values for the particular input_file_format.

        The input_file_format should be found from the reference data, otherwise it is an error.
        """
        versions = []
        iff_found = False
        for entry in file_format_version_refdata:
            if input_file_format == entry["input_file_format"]:
                iff_found = True
                if entry.get("output_format_version", False):
                    versions.append(entry["output_format_version"])

        if not iff_found:
            errors["file_characteristics.file_format"].append(
                "Value for file_format '%s' not found in reference data" % input_file_format
            )

        return versions
