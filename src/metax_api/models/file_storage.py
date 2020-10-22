# This file is part of the Metax API service
#
# Copyright 2017-2018 Ministry of Education and Culture, Finland
#
# :author: CSC - IT Center for Science Ltd., Espoo Finland <servicedesk@csc.fi>
# :license: MIT

from django.db.models import JSONField
from .common import Common


class FileStorage(Common):

    # MODEL FIELD DEFINITIONS #

    file_storage_json = JSONField(blank=True, null=True)

    # END OF MODEL FIELD DEFINITIONS #

    def __init__(self, *args, **kwargs):
        super(FileStorage, self).__init__(*args, **kwargs)
        self.track_fields('file_storage_json.identifier')

    def save(self, *args, **kwargs):
        if self._operation_is_update():
            if self.field_changed('file_storage_json.identifier'):
                # read-only after creating
                self.file_storage_json['identifier'] = self._initial_data['file_storage_json']['identifier']

        super(FileStorage, self).save(*args, **kwargs)

    def __repr__(self):
        return '<%s: %d, removed: %s, identifier: %s >' % (
            'FileStorage',
            self.id,
            str(self.removed),
            self.file_storage_json['identifier'],
        )

    def delete(self):
        super(FileStorage, self).remove()