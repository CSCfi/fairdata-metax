from uuid import uuid4
from django.db import models
from django.contrib.auth.models import User


import logging
_logger = logging.getLogger(__name__)
d = logging.getLogger(__name__).debug

class Common(models.Model):

    id = models.UUIDField(primary_key=True, default=uuid4, editable=False)
    identifier_sha256 = models.DecimalField(max_digits=79, decimal_places=0, blank=False, unique=True)
    active = models.BooleanField(default=True)
    removed = models.BooleanField(default=False)
    modified_by_api = models.DateTimeField(null=True)
    modified_by_user_id = models.ForeignKey(User, related_name='%(class)s_modified_by_user',
                                            null=True, db_column='modified_by_user_id')
    created_by_api = models.DateTimeField()
    created_by_user_id = models.ForeignKey(User, related_name='%(class)s_created_by_user',
                                           null=True, db_column='created_by_user_id')

    class Meta:
        abstract = True
        indexes = [
            models.Index(fields=['identifier_sha256']),
        ]

    def delete(self):
        """
        Mark record as removed, never delete from db.
        """
        self.removed = True
        self.save()

    def __str__(self):
        return str(self.id)
