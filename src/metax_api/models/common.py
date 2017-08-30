from django.db import models
from django.core.exceptions import FieldError

class CommonManager(models.Manager):

    def get_queryset(self):
        return super(CommonManager, self).get_queryset().filter(active=True, removed=False)


class Common(models.Model):

    id = models.BigAutoField(primary_key=True, editable=False)
    active = models.BooleanField(default=True)
    removed = models.BooleanField(default=False)
    modified_by_api = models.DateTimeField(null=True)
    modified_by_user_id = models.CharField(max_length=200, null=True)
    created_by_api = models.DateTimeField()
    created_by_user_id = models.CharField(max_length=200, null=True)

    # all queries made using the default 'objects' table are filtered with active=True and removed=False
    objects = CommonManager()

    # to access removed or inactive records, use this manager instead
    objects_unfiltered = models.Manager()

    class Meta:
        abstract = True

    def __init__(self, *args, **kwargs):
        super(Common, self).__init__(*args, **kwargs)
        self._initial_data = {}

    def save(self, *args, **kwargs):
        super(Common, self).save(*args, **kwargs)
        self._update_tracked_field_values()

    def delete(self):
        """
        Mark record as removed, never delete from db.
        """
        self.removed = True
        self.save()

    def track_fields(self, *args):
        """
        Save initial values from object fields when object is created (= retrieved from db),
        so that they can be checked at a later time if the value is being changed or not.
        """
        for field_name in args:
            self._initial_data[field_name] = getattr(self, field_name)

    def field_changed(self, field_name):
        """
        Check if a tracked field has changed since last saved to db.
        """
        try:
            initial_value = self._initial_data[field_name]
        except Exception:
            raise FieldError('Field %s is not being tracked for changes' % field_name)
        return getattr(self, field_name) != initial_value

    def _update_tracked_field_values(self):
        """
        After saving record to db, tracked field values need to be updated so that
        field_changed() keeps working as expected
        """
        for field_name in self._initial_data.keys():
            self._initial_data[field_name] = getattr(self, field_name)

    def __str__(self):
        return str(self.id)
