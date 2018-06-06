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
    directory_name = models.CharField(max_length=200)
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

    def __repr__(self):
        return '<%s: %d, removed: %s, project_identifier: %s, identifier: %s, directory_path: %s >' % (
            'Directory',
            self.id,
            str(self.removed),
            self.project_identifier,
            self.identifier,
            self.directory_path
        )
