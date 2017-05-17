from uuid import uuid4
from django.contrib.postgres.fields import JSONField
from django.db import models

class File(models.Model):

    id = models.UUIDField(primary_key=True, default=uuid4, editable=False)
    file_name = models.CharField(max_length=64, blank=True, null=True)
    json = JSONField(blank=True, null=True)

    def __str__(self):
        return str(self.id)
