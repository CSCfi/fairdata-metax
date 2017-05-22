from uuid import uuid4
from django.db import models
from django.contrib.auth.models import User

class Common(models.Model):

    id = models.UUIDField(primary_key=True, default=uuid4, editable=False)
    active = models.BooleanField(default=True)
    modified_by_api = models.DateTimeField(auto_now=True)
    modified_by_user_id = models.ForeignKey(User, related_name='modified_by_user', null=True)
    created_by_api = models.DateTimeField(auto_now_add=True)
    created_by_user_id = models.ForeignKey(User, related_name='creataed_by_user', null=True)

    class Meta:
        abstract = True

    def __str__(self):
        return str(self.id)
