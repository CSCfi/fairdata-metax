import logging

from django.db import connection, models

from .common import Common


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

    def calculate_byte_size_and_file_count(self, cursor=None):
        """
        Recursively traverse the entire directory tree and update total byte size and file count
        for each directory. Intended to be called for the root directory of a project, since it
        doesnt make sense to update those numbers up to some middlepoint only.
        """
        if cursor is None:
            if self.parent_directory_id:
                raise Exception(
                    'while this is a recursive method, it is intended to be initially called by '
                    'project root directories only.'
                )

            _logger.info('Calculating directory byte sizes and file counts for project %s...' % self.project_identifier)

            with connection.cursor() as cursor:
                self.calculate_byte_size_and_file_count(cursor)

        else:

            self.byte_size = 0
            self.file_count = 0

            sub_dirs = self.child_directories.all()
            if sub_dirs:
                for sub_dir in sub_dirs:
                    sub_dir.calculate_byte_size_and_file_count(cursor)

                # sub dir numbers
                self.byte_size = sum(d.byte_size for d in sub_dirs)
                self.file_count = sum(d.file_count for d in sub_dirs)

            sql_calculate_byte_size_and_file_count = """
                select sum(f.byte_size) as byte_size, count(f.id) as file_count
                from metax_api_file f
                where f.parent_directory_id = %s
                and f.removed = false
                and f.active = true
                """

            cursor.execute(sql_calculate_byte_size_and_file_count, [self.id])
            bs, fc = cursor.fetchall()[0]
            self.byte_size += bs or 0
            self.file_count += fc or 0
            self.save()

    def __repr__(self):
        return '<%s: %d, removed: %s, project_identifier: %s, identifier: %s, directory_path: %s >' % (
            'Directory',
            self.id,
            str(self.removed),
            self.project_identifier,
            self.identifier,
            self.directory_path
        )
