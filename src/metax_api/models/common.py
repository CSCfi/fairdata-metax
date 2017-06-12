from uuid import uuid4
from django.db import models
from django.contrib.auth.models import User

class Common(models.Model):

    # todo decide on UUID. random, sequential, place of generation (metax, client?)
    id = models.UUIDField(primary_key=True, default=uuid4, editable=False)
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

    def get(self, **kwargs):
        kwargs.update({ 'active': True, 'removed': False })
        return super(Common, self).get(**kwargs)

    def delete(self):
        """
        Mark record as removed, never delete from db.
        """
        self.removed = True
        self.save()

    def __str__(self):
        return str(self.id)
