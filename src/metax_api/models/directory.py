# This file is part of the Metax API service
#
# Copyright 2017-2018 Ministry of Education and Culture, Finland
#
# :author: CSC - IT Center for Science Ltd., Espoo Finland <servicedesk@csc.fi>
# :license: MIT

import logging

from django.db import connection, models
from django.db.models import Prefetch

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
    parent_directory = models.ForeignKey('self', on_delete=models.SET_NULL, null=True, related_name='child_directories')
    project_identifier = models.CharField(max_length=200)

    # END OF MODEL FIELD DEFINITIONS #

    class Meta:
        indexes = [
            models.Index(fields=['directory_path']),
            models.Index(fields=['identifier']),
            models.Index(fields=['parent_directory']),
            models.Index(fields=['project_identifier']),
        ]

    def delete(self):
        # actual delete
        super(Common, self).delete()

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
                'while this is a recursive method, it is intended to be initially called by '
                'project root directories only.'
            )

        _logger.info('Calculating directory byte sizes and file counts for project %s...' % self.project_identifier)

        update_statements = []

        self._calculate_byte_size_and_file_count(update_statements)

        sql_update_all_directories = '''
            update metax_api_directory as d set
                byte_size = results.byte_size,
                file_count = results.file_count
            from (values
                %s
            ) as results(byte_size, file_count, id)
            where results.id = d.id;
            ''' % ','.join(update_statements)

        with connection.cursor() as cursor:
            cursor.execute(sql_update_all_directories)

        _logger.info(
            'Project %s directory tree calculations complete. Total byte_size: '
            '%d bytes (%.3f GB), total file_count: %d files'
            % (
                self.project_identifier,
                self.byte_size,
                self.byte_size / 1024 / 1024 / 1024,
                self.file_count
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
        sub_dirs = self.child_directories.all() \
            .only('byte_size', 'parent_directory_id') \
            .prefetch_related(
                Prefetch('files', queryset=File.objects.only('id', 'byte_size', 'parent_directory_id')))

        if sub_dirs:
            for sub_dir in sub_dirs:
                sub_dir._calculate_byte_size_and_file_count(update_statements)

            # sub dir numbers
            self.byte_size = sum(d.byte_size for d in sub_dirs)
            self.file_count = sum(d.file_count for d in sub_dirs)

        # note: never actually saved using .save()
        self.byte_size += sum(f.byte_size for f in self.files.all()) or 0
        self.file_count += len(self.files.all()) or 0

        update_statements.append(
            '(%d, %d, %d)'
            % (self.byte_size, self.file_count, self.id)
        )

    def calculate_byte_size_and_file_count_for_cr(self, cr_id, directory_data):
        """
        Calculate total byte sizes and file counts for a directory tree in the context of
        selected directories and a specific catalog record, and store total byte size and
        file count for each directory into parameter directory_data. Intended to be called
        for the top-level directories of a project in a dataset.

        NOTE: Does not handle numbers for "leading prefix directories", or explained more verbally,
        directories in the following case:

        Example:

        The following directories are added to a dataset

        /root/experiments/phase_1
        /root/experiments/phase_1/2020
        /root/experiments/phase_1/2021
        /root/experiments/phase_2
        /root/experiments/phase_1/2022
        /root/experiments/phase_1/2023

        Here, these two directories are "top level directories", which basically already include
        all the files:
        /root/experiments/phase_1
        /root/experiments/phase_2
        These do have the numbers calculated. These are the directories that this method is executed
        on initially.

        However:
        /root
        /root/experiments
        These directories do not have the numbers calculated, since they were not specifically added
        to the dataset. They just happen to be part of the filepath, but they are not intended to
        be browsed.

        This is OK, since the way files are normally browsed, is through an UI, and
        they would begin browsing from the selected directories only. The only situation where it may be
        confusing that numbers are missing, is when manually browsing the API, and seeing all the other
        data in place, except the numbers. That is "as intended".
        """
        _logger.debug('Calculating directory byte sizes and file counts for project %s, directory %s...' %
            (self.project_identifier, self.directory_path))

        sql = '''
            select parent_directory_id, sum(f.byte_size) as byte_size, count(f.id) as file_count
            from metax_api_file f
            inner join metax_api_catalogrecord_files cr_f on (cr_f.file_id = f.id and cr_f.catalogrecord_id = %s)
            where f.removed = false and f.active = true
            group by parent_directory_id
        '''

        with connection.cursor() as cursor:
            cursor.execute(sql, [cr_id])
            grouped_by_dir = {}
            for row in cursor.fetchall():
                grouped_by_dir[row[0]] = [ int(row[1]), row[2] ]

        self._calculate_byte_size_and_file_count_for_cr(grouped_by_dir, directory_data)

    def _calculate_byte_size_and_file_count_for_cr(self, grouped_by_dir, directory_data):
        """
        Recursively traverse the sub dirs, and accumulate values from grouped_by_dir, and
        store into the dict directory_data. Once finished, directory_data will look like:
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

        sub_dirs = self.child_directories.all().only('id')

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
        directory_data[self.id] = [ self.byte_size, self.file_count ]

    def __repr__(self):
        return '<%s: %d, removed: %s, project_identifier: %s, identifier: %s, directory_path: %s >' % (
            'Directory',
            self.id,
            str(self.removed),
            self.project_identifier,
            self.identifier,
            self.directory_path
        )
