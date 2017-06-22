from django.db import models
from django.contrib.auth.models import User

class CommonManager(models.Manager):

    def get_queryset(self):
        return super(CommonManager, self).get_queryset().filter(active=True, removed=False)


class Common(models.Model):

    id = models.BigAutoField(primary_key=True, editable=False)
    active = models.BooleanField(default=True)
    removed = models.BooleanField(default=False)
    modified_by_api = models.DateTimeField(null=True)
    modified_by_user_id = models.ForeignKey(User, related_name='%(class)s_modified_by_user',
                                            null=True, db_column='modified_by_user_id')
    created_by_api = models.DateTimeField()
    created_by_user_id = models.ForeignKey(User, related_name='%(class)s_created_by_user',
                                           null=True, db_column='created_by_user_id')

    # all queries made using the default 'objects' table are filtered with active=True and removed=False
    objects = CommonManager()

    # to access removed or inactive records, use this manager instead
    objects_unfiltered = models.Manager()

    class Meta:
        abstract = True

    def delete(self):
        """
        Mark record as removed, never delete from db.
        """
        self.removed = True
        self.save()

    def __str__(self):
        return str(self.id)
