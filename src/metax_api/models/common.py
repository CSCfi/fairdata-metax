from dateutil import parser
from time import time

from django.core.exceptions import FieldError
from django.db import models


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

    def modified_since(self, timestamp):
        """
        Return True if object has been modified since the given timestamp.

        parameters:
        timestamp: a timezone-aware datetime object, or a timestamp string with timezone information,
            or None, which implies 'the resource has never been modified before'
        """
        if not self.modified_by_api:
            # server version has never been modified
            return False
        elif not timestamp:
            # server version has been modified at some point, but the version being compared with
            # was never modified.
            return True

        if isinstance(timestamp, str):
            timestamp = parser.parse(timestamp)

        return timestamp < self.modified_by_api

    def track_fields(self, *args):
        """
        Save initial values from object fields when object is created (= retrieved from db),
        so that they can be checked at a later time if the value is being changed or not.

        If field_name contains a dot, i.e. research_data.urn_identifier, it is assumed that
        field_name is a dict (a JSON field). For now only one level of nesting is supported.
        If a need arises, can be made mega generic.
        """
        for field_name in args:
            if '.' in field_name:
                self._track_json_field(field_name)
            else:
                self._initial_data[field_name] = getattr(self, field_name)

    def field_changed(self, field_name):
        """
        Check if a tracked field has changed since last saved to db.
        """
        if '.' in field_name:
            return self._json_field_changed(field_name)
        try:
            initial_value = self._initial_data[field_name]
        except Exception:
            raise FieldError('Field %s is not being tracked for changes' % field_name)
        return getattr(self, field_name) != initial_value

    def _generate_identifier(self, salt):
        return 'pid:urn:%s:%d-%d' % (str(salt), self.id, int(round(time() * 1000)))

    def _json_field_changed(self, field_name):
        field_name, json_field_name = field_name.split('.')
        try:
            json_field_value = self._initial_data[field_name][json_field_name]
        except:
            raise FieldError('Field %s.%s is not being tracked for changes' % (field_name, json_field_name))
        return getattr(self, field_name).get(json_field_name) != json_field_value

    def _operation_is_create(self):
        return self.id is None

    def _track_json_field(self, field_name):
        field_name, json_field_name = field_name.split('.')
        json_field_value = getattr(self, field_name).get(json_field_name, None)
        if not self._initial_data.get(field_name, None):
            self._initial_data[field_name] = {}
        self._initial_data[field_name][json_field_name] = json_field_value

    def _update_tracked_field_values(self):
        """
        After saving record to db, tracked field values need to be updated so that
        field_changed() keeps working as expected
        """
        for field_name in self._initial_data.keys():
            if '.' in field_name:
                field_name, json_field_name = field_name.split('.')
                # by now should have crashed to checks in previous steps, so no need to check here
                self._initial_data[field_name][json_field_name] = getattr(self, field_name).get(json_field_name, None)
            else:
                self._initial_data[field_name] = getattr(self, field_name)

    def __str__(self):
        return str(self.id)
