# This file is part of the Metax API service
#
# Copyright 2017-2018 Ministry of Education and Culture, Finland
#
# :author: CSC - IT Center for Science Ltd., Espoo Finland <servicedesk@csc.fi>
# :license: MIT

import logging

from django.db import connection, models
from django.db.models import Count, Prefetch, Sum, Q

from .common import Common
from .file import File

_logger = logging.getLogger(__name__)


class Directory(Common):

    # MODEL FIELD DEFINITIONS #

    byte_size = models.BigIntegerField(default=0)
    directory_deleted = models.DateTimeField(null=True)
    directory_modified = models.DateTimeField(auto_now=True)
    directory_name = models.TextField()
    directory_path = models.TextField()
    identifier = models.CharField(max_length=200, unique=True)
    file_count = models.BigIntegerField(default=0)
    parent_directory = models.ForeignKey(
        "self", on_delete=models.SET_NULL, null=True, related_name="child_directories"
    )
    project_identifier = models.CharField(max_length=200)

    # END OF MODEL FIELD DEFINITIONS #

    class Meta:
        indexes = [
            models.Index(fields=["directory_path"]),
            models.Index(fields=["identifier"]),
            models.Index(fields=["parent_directory"]),
            models.Index(fields=["project_identifier"]),
        ]

    def delete(self):
        # Actual delete
        super().delete()

    def user_has_access(self, request):
        if request.user.is_service:
            return True
        from metax_api.services import AuthService

        return self.project_identifier in AuthService.get_user_projects(request)

    def calculate_byte_size_and_file_count(self):
        """
        Traverse the entire directory tree and update total byte size and file count
        for each directory. Intended to be called for the root directory of a project, since it
        doesnt make sense to update those numbers up to some middlepoint only.
        """
        if self.parent_directory_id:
            raise Exception(
                "while this is a recursive method, it is intended to be initially called by "
                "project root directories only."
            )

        _logger.info(
            "Calculating directory byte sizes and file counts for project %s..."
            % self.project_identifier
        )

        update_statements = []
        annotated_root_directory = self._get_project_directory_tree(with_own_sizes=True)
        annotated_root_directory._calculate_byte_size_and_file_count(update_statements)

        if len(update_statements) > 0:
            sql_update_all_directories = """
                update metax_api_directory as d set
                    byte_size = results.byte_size,
                    file_count = results.file_count
                from (values
                    %s
                ) as results(byte_size, file_count, id)
                where results.id = d.id;
                """ % ",".join(
                update_statements
            )

            with connection.cursor() as cursor:
                cursor.execute(sql_update_all_directories)

        _logger.info(
            "Project %s directory tree calculations complete. Total byte_size: "
            "%d bytes (%.3f GB), total file_count: %d files"
            % (
                self.project_identifier,
                self.byte_size,
                self.byte_size / 1024 / 1024 / 1024,
                self.file_count,
            )
        )

    def _get_project_directory_tree(self, with_own_sizes=False):
        """
        Get all project directories from DB in single query. Returns current directory with
        subdirectories in directory.sub_dirs.

        Optionally, annotate directories with total size and
        count of files they contain as own_byte_size and own_file_count.
        """
        project_directories = Directory.objects.filter(
            project_identifier=self.project_identifier
        ).only("file_count", "byte_size", "parent_directory_id")

        if with_own_sizes:
            project_directories = project_directories.annotate(
                own_byte_size=Sum("files__byte_size", filter=Q(files__removed=False)),
                own_file_count=Count("files", filter=Q(files__removed=False)),
            )

        # build directory tree from annotated directories
        directories_by_id = {d.id: d for d in project_directories}
        for d in project_directories:
            d.sub_dirs = []
        for d in project_directories:
            if d.parent_directory_id is not None:
                directories_by_id[d.parent_directory_id].sub_dirs.append(d)
        annotated_root_directory = directories_by_id.get(self.id)
        return annotated_root_directory

    def _calculate_byte_size_and_file_count(self, update_statements):
        """
        Recursively traverse the entire directory tree and update total byte size and file count
        for each directory. Accumulates a list of triplets for a big sql-update statement.
        """
        old_byte_size = self.byte_size
        old_file_count = self.file_count
        self.byte_size = 0
        self.file_count = 0

        if self.sub_dirs:
            for sub_dir in self.sub_dirs:
                sub_dir._calculate_byte_size_and_file_count(update_statements)

            # sub dir numbers
            self.byte_size = sum(d.byte_size for d in self.sub_dirs)
            self.file_count = sum(d.file_count for d in self.sub_dirs)

        # note: never actually saved using .save()
        self.byte_size += self.own_byte_size or 0
        self.file_count += self.own_file_count or 0

        # add updated values if changed
        if self.byte_size != old_byte_size or self.file_count != old_file_count:
            update_statements.append("(%d, %d, %d)" % (self.byte_size, self.file_count, self.id))

    def calculate_byte_size_and_file_count_for_cr(self, cr_id, directory_data):
        """
        Calculate total byte sizes and file counts for a directory tree in the context of
        selected directories and a specific catalog record, and store total byte size and
        file count for each directory into parameter directory_data. Intended to be called
        for the top-level directories of a project in a dataset.
        """
        _logger.debug(
            "Calculating directory byte sizes and file counts for project %s, directory %s..."
            % (self.project_identifier, self.directory_path)
        )

        stats = (
            File.objects.filter(record__pk=cr_id)
            .values_list("parent_directory_id")
            .annotate(Sum("byte_size"), Count("id"))
        )

        grouped_by_dir = {
            parent_id: (byte_size, file_count) for parent_id, byte_size, file_count in stats
        }

        directory_tree = self._get_project_directory_tree()
        directory_tree._calculate_byte_size_and_file_count_for_cr(
            grouped_by_dir, directory_data
        )

    def _calculate_byte_size_and_file_count_for_cr(self, grouped_by_dir, directory_data):
        """
        Recursively traverse the sub dirs, and accumulate values from grouped_by_dir, and
        store into the CatalogRecord _directory_data dict. Once finished, directory_data will look like:
        {
            'id1': [byte_size, file_count],
            'id2': [byte_size, file_count],
            ...
        }
        When browsing files for a given cr, total byte size and file count for a directory
        are retrieved from this lookup table.
        """
        self.byte_size = 0
        self.file_count = 0

        if self.sub_dirs:
            for sub_dir in self.sub_dirs:
                sub_dir._calculate_byte_size_and_file_count_for_cr(grouped_by_dir, directory_data)

            # sub dir numbers
            self.byte_size = sum(d.byte_size for d in self.sub_dirs)
            self.file_count = sum(d.file_count for d in self.sub_dirs)

        current_dir = grouped_by_dir.get(self.id, [0, 0])

        # note: never actually saved using .save()
        self.byte_size += current_dir[0]
        self.file_count += current_dir[1]

        # the accumulated numbers that exist in the directory for given cr
        directory_data[self.id] = [self.byte_size, self.file_count]

    def __repr__(self):
        return (
            "<%s: %d, removed: %s, project_identifier: %s, identifier: %s, directory_path: %s >"
            % (
                "Directory",
                self.id,
                str(self.removed),
                self.project_identifier,
                self.identifier,
                self.directory_path,
            )
        )
