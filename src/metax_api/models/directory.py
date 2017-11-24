from django.db import connection, models

from .common import Common, CommonManager


class DirectoryManager(CommonManager):

    def delete_directory(self, directory_path):
        """
        For every File whose file_path.startswith(directory_path),
            set removed=True,
            and for every dataset this file belongs to,
                set flag removed_in_ida=True in file in dataset list
        """

        # copy-paste leftover from old FileManager, to be revisited...

        affected_files = 0

        sql = 'update metax_api_file set removed = true ' \
              'where removed = false ' \
              'and active = true ' \
              'and (file_path = %s or file_path like %s)'

        with connection.cursor() as cr:
            cr.execute(sql, [directory_path, '%s/%%' % directory_path])
            affected_files = cr.rowcount

        return affected_files


class Directory(Common):

    # MODEL FIELD DEFINITIONS #

    byte_size = models.PositiveIntegerField(default=0)
    directory_deleted = models.DateTimeField(null=True)
    directory_modified = models.DateTimeField(auto_now=True)
    directory_name = models.CharField(max_length=200)
    directory_path = models.TextField()
    identifier = models.CharField(max_length=200, unique=True)
    file_count = models.PositiveIntegerField(default=0, null=True)
    parent_directory = models.ForeignKey('self', on_delete=models.SET_NULL, null=True, related_name='child_directories')
    project_identifier = models.CharField(max_length=200)

    # END OF MODEL FIELD DEFINITIONS #

    indexes = [
        models.Index(fields=['identifier']),
    ]

    objects = DirectoryManager()

    def delete(self):
        # actual delete
        super(Common, self).delete()
