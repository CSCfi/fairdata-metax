# This file is part of the Metax API service
#
# Copyright 2017-2018 Ministry of Education and Culture, Finland
#
# :author: CSC - IT Center for Science Ltd., Espoo Finland <servicedesk@csc.fi>
# :license: MIT

from django.db.models import JSONField
from django.db import connection

from .common import Common


class Contract(Common):

    # MODEL FIELD DEFINITIONS #

    contract_json = JSONField(blank=True, null=True)

    # END OF MODEL FIELD DEFINITIONS #

    def delete(self):
        """
        Read django docs about deleting objects in bulk using a queryset to see
        why direct sql is easier here (individual model level delete() methods are not executed).
        https://docs.djangoproject.com/en/1.11/topics/db/models/#overriding-model-methods
        """
        super(Contract, self).remove()
        sql = 'update metax_api_catalogrecord set removed = true ' \
              'where active = true and removed = false ' \
              'and contract_id = %s'
        with connection.cursor() as cr:
            cr.execute(sql, [self.id])

    def __repr__(self):
        return '<%s: %d, removed: %s, identifier: %d, record_count: %d >' \
            % (
                'Contract',
                self.id,
                str(self.removed),
                self.contract_json['identifier'],
                self.records.count(),
            )
