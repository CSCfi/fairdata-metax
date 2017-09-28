from django.contrib.postgres.fields import JSONField
from django.db import connection

from .common import Common

class Contract(Common):

    contract_json = JSONField(blank=True, null=True)

    def delete(self):
        """
        Read django docs about deleting objects in bulk using a queryset to see
        why direct sql is easier here (individual model level delete() methods are not executed).
        https://docs.djangoproject.com/en/1.11/topics/db/models/#overriding-model-methods
        """
        super(Contract, self).delete()
        sql = 'update metax_api_catalogrecord set removed = true ' \
              'where active = true and removed = false ' \
              'and contract_id = %s'
        with connection.cursor() as cr:
            cr.execute(sql, [self.id])
