# This file is part of the Metax API service
#
# Copyright 2017-2018 Ministry of Education and Culture, Finland
#
# :author: CSC - IT Center for Science Ltd., Espoo Finland <servicedesk@csc.fi>
# :license: MIT

import logging

from django.db import connection, models
from django.db.models import Count, Prefetch, Sum

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

        self._calculate_byte_size_and_file_count(update_statements)

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

    def _calculate_byte_size_and_file_count(self, update_statements):
        """
        Recursively traverse the entire directory tree and update total byte size and file count
        for each directory. Accumulates a list of triplets for a big sql-update statement.
        """
        self.byte_size = 0
        self.file_count = 0

        # fields id, parent_directory_id must be specified for joining for Prefetch-object to work properly
        sub_dirs = (
            self.child_directories.all()
            .only("byte_size", "parent_directory_id")
        )

        if sub_dirs:
            for sub_dir in sub_dirs:
                sub_dir._calculate_byte_size_and_file_count(update_statements)

            # sub dir numbers
            self.byte_size = sum(d.byte_size for d in sub_dirs)
            self.file_count = sum(d.file_count for d in sub_dirs)

        # note: never actually saved using .save()
        file_aggregates = self.files.aggregate(byte_size=Sum("byte_size"), count=Count("*"))
        self.byte_size += file_aggregates['byte_size'] or 0
        self.file_count += file_aggregates['count'] or 0

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

        self._calculate_byte_size_and_file_count_for_cr(grouped_by_dir, directory_data)

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

        sub_dirs = self.child_directories.all().only("id")

        if sub_dirs:
            for sub_dir in sub_dirs:
                sub_dir._calculate_byte_size_and_file_count_for_cr(grouped_by_dir, directory_data)

            # sub dir numbers
            self.byte_size = sum(d.byte_size for d in sub_dirs)
            self.file_count = sum(d.file_count for d in sub_dirs)

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
