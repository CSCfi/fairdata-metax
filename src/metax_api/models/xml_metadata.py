# This file is part of the Metax API service
#
# Copyright 2017-2018 Ministry of Education and Culture, Finland
#
# :author: CSC - IT Center for Science Ltd., Espoo Finland <servicedesk@csc.fi>
# :license: MIT

from django.db import models

from .common import Common
from .file import File


class XmlMetadata(Common):

    # MODEL FIELD DEFINITIONS #

    namespace = models.CharField(max_length=200)
    xml = models.TextField()
    file = models.ForeignKey(File, on_delete=models.DO_NOTHING)

    # END OF MODEL FIELD DEFINITIONS #

    class Meta:
        unique_together = ('namespace', 'file')

    indexes = [
        models.Index(fields=['namespace']),
    ]

    def delete(self):
        """
        Deletes permanently - does not only mark as removed
        """
        super(Common, self).delete()
