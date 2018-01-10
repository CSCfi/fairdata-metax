from django.db import models

from .common import Common


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

    def delete(self):
        # actual delete
        super(Common, self).delete()

    def __repr__(self):
        return '<%s: %d, removed: %s, project_identifier: %s, identifier: %s, directory_path: %s >' % (
            'Directory',
            self.id,
            str(self.removed),
            self.project_identifier,
            self.identifier,
            self.directory_path
        )
