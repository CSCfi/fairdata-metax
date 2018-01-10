from django.contrib.postgres.fields import JSONField

from .common import Common


class FileStorage(Common):

    # MODEL FIELD DEFINITIONS #

    file_storage_json = JSONField(blank=True, null=True)

    # END OF MODEL FIELD DEFINITIONS #