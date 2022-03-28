# This file is part of the Metax API service
#
# Copyright 2017-2018 Ministry of Education and Culture, Finland
#
# :author: CSC - IT Center for Science Ltd., Espoo Finland <servicedesk@csc.fi>
# :license: MIT

import pickle

from dateutil import parser
from django.core.exceptions import FieldError
from django.db import models

from metax_api.utils.utils import executing_test_case, get_tz_aware_now_without_micros


class CommonManager(models.Manager):
    def get_queryset(self):
        return super(CommonManager, self).get_queryset().filter(active=True, removed=False)


class Common(models.Model):

    # MODEL FIELD DEFINITIONS #

    id = models.BigAutoField(primary_key=True, editable=False)
    active = models.BooleanField(default=True)
    removed = models.BooleanField(default=False)
    date_modified = models.DateTimeField(null=True)
    user_modified = models.CharField(max_length=200, null=True)
    date_created = models.DateTimeField()
    user_created = models.CharField(max_length=200, null=True)
    service_modified = models.CharField(
        max_length=200,
        null=True,
        help_text="Name of the service who last modified the record",
    )
    service_created = models.CharField(
        max_length=200,
        null=True,
        help_text="Name of the service who created the record",
    )
    date_removed = models.DateTimeField(null=True)

    # END OF MODEL FIELD DEFINITIONS #

    # may contain a http request object, used for permission checking etc when relevant.
    request = None

    # all queries made using the default 'objects' table are filtered with active=True and removed=False
    objects = CommonManager()

    # to access removed or inactive records, use this manager instead
    objects_unfiltered = models.Manager()

    class Meta:
        indexes = [
            models.Index(fields=["active"]),
            models.Index(fields=["removed"]),
        ]
        abstract = True

    def __init__(self, *args, **kwargs):
        if "__request" in kwargs:
            self.request = kwargs.pop("__request")

        super(Common, self).__init__(*args, **kwargs)

        self._initial_data = {}
        self._tracked_fields = []

        self.track_fields(
            "date_created",
            "user_created",
            "service_created",
        )

    def save(self, *args, **kwargs):
        if self._operation_is_update():
            self._check_read_only_after_create_fields()
            self._unset_removed()
        super(Common, self).save(*args, **kwargs)
        self._update_tracked_field_values()

    def force_save(self, *args, **kwargs):
        """
        Can be used to directly save to db while bypassing all tracked_fields
        checks. Should be used only in testing to set up data for a test case.
        """
        if not executing_test_case():
            raise Exception("this method should only be used inside a test case")
        super(Common, self).save(*args, **kwargs)
        self._update_tracked_field_values()

    def remove(self):
        """
        Mark record as removed, never delete from db.
        """
        self._set_removed()
        super().save(update_fields=["removed", "date_removed", "date_modified"])
        self._update_tracked_field_values()

    def user_has_access(self, request):
        """
        Inheriting objects should override to check permission by checking project affiliation,
        group membership, etc...
        """
        if request.user.is_service:
            return True
        return False

    def modified_since(self, timestamp):
        """
        Return True if object has been modified since the given timestamp. Currently this method is used for validating
        date_modified string representation or http header timestamp originated datetime object. In the former case,
        the format should be well-known since it is created by Metax API.

        parameters:
        timestamp: a timezone-aware datetime object, or a timestamp string with timezone information,
            or None, which implies 'the resource has never been modified before'
        """
        if not self.date_modified:
            # server version has never been modified
            return False
        elif not timestamp:
            # server version has been modified at some point, but the version being compared with
            # was never modified.
            return True

        if isinstance(timestamp, str):
            timestamp = parser.parse(timestamp)

        return timestamp < self.date_modified

    def _deepcopy_field(self, field_value):
        """
        Deep copy field value.

        Pickle can be an order of magnitude faster than copy.deepcopy for
        deeply nested fields like CatalogRecord.research_dataset.
        """
        return pickle.loads(pickle.dumps(field_value))


    def track_fields(self, *fields):
        """
        Save initial values from object fields when object is created (= retrieved from db),
        so that they can be checked at a later time if the value is being changed or not.

        If field_name contains a dot, i.e. research_data.metadata_version_identifier, it is assumed that
        field_name is a dict (a JSON field). For now only one level of nesting is supported.
        If a need arises, can be made mega generic.
        """
        self._tracked_fields.extend(fields)

        for field_name in fields:
            if "." in field_name:
                self._track_json_field(field_name)
            else:
                if self._field_is_loaded(field_name):
                    requested_field = getattr(self, field_name)
                    if isinstance(requested_field, dict):
                        self._initial_data[field_name] = self._deepcopy_field(requested_field)
                    else:
                        self._initial_data[field_name] = requested_field

    def _set_removed(self):
        self.removed = True
        self.date_removed = self.date_modified = get_tz_aware_now_without_micros()

    def _unset_removed(self):
        self.removed = False
        self.date_removed = None
        self.date_modified = get_tz_aware_now_without_micros()

    def _track_json_field(self, field_name):
        field_name, json_field_name = field_name.split(".")
        if self._field_is_loaded(field_name) and json_field_name in getattr(self, field_name):
            json_field_value = getattr(self, field_name)[json_field_name]

            if not self._initial_data.get(field_name, None):
                self._initial_data[field_name] = {}

            if isinstance(json_field_value, dict):
                self._initial_data[field_name][json_field_name] = self._deepcopy_field(json_field_value)
            else:
                self._initial_data[field_name][json_field_name] = json_field_value

    def _field_is_loaded(self, field_name):
        """
        Field has been loaded from the db
        """
        return field_name in self.__dict__

    def _field_is_tracked(self, field_name):
        """
        Field is requested to be tracked. Not necessarily loaded yet.
        """
        return field_name in self._tracked_fields

    def _field_initial_value_loaded(self, field_name):
        """
        Field was loaded to _initial_data during __init__ for tracking
        """
        return field_name in self._initial_data

    def field_changed(self, field_name):
        """
        Check if a tracked field has changed since last saved to db.
        """
        if "." in field_name:
            return self._json_field_changed(field_name)

        if not self._field_is_loaded(field_name):
            return False

        if self._field_is_tracked(field_name):  # pragma: no cover
            if not self._field_initial_value_loaded(field_name):
                self._raise_field_not_tracked_error(field_name)
        else:  # pragma: no cover
            raise FieldError("Field %s is not being tracked for changes" % (field_name))

        return getattr(self, field_name) != self._initial_data[field_name]

    def _json_field_changed(self, field_name_full):
        field_name, json_field_name = field_name_full.split(".")

        if not self._field_is_loaded(field_name):
            return False

        if self._field_is_tracked(field_name_full):  # pragma: no cover
            if not self._field_initial_value_loaded(field_name):
                self._raise_field_not_tracked_error(field_name_full)
        else:  # pragma: no cover
            raise FieldError("Field %s is not being tracked for changes" % (field_name_full))

        json_field_value = self._initial_data[field_name].get(json_field_name, None)
        return getattr(self, field_name).get(json_field_name, None) != json_field_value

    def _raise_field_not_tracked_error(self, field_name):
        """
        If a field is not retrieved from the db durin record initialization (__init__()),
        then the field will not have its initial value loaded for tracking. That is OK,
        for the cases when that field is not otherwise being modified, to not do needless work.

        However, if the field does end up being modified, then the caller should make sure the
        field is included when the object is being created.

        We COULD simply retrieve the value from the db when it was not included in the original
        query, but that is always an extra query to the db per field that was not loaded. So
        instead, we raise an error, so that the caller gets notified that they should optimise
        their initial query to include all the data they were going to need anyway.
        """
        raise FieldError(
            "Tried to check changes in field %(field_name)s, but the field was not loaded for "
            "tracking changes during __init__. Call .only(%(field_name)s) in you ORM query to "
            "load the field during __init__, so that it will be tracked." % locals()
        )

    def _check_read_only_after_create_fields(self):
        if self.field_changed("date_created"):
            self.date_created = self._initial_data["date_created"]
        if self.field_changed("user_created"):
            self.user_created = self._initial_data["user_created"]
        if self.field_changed("service_created"):
            self.service_created = self._initial_data["service_created"]

    def _operation_is_create(self):
        return self.id is None

    def _operation_is_update(self):
        return self.id is not None

    def _update_tracked_field_values(self):
        """
        After saving record to db, tracked field values need to be updated so that
        field_changed() keeps working as expected
        """
        for field_name in self._initial_data.keys():
            if "." in field_name:
                field_name, json_field_name = field_name.split(".")
                # by now should have crashed to checks in previous steps, so no need to check here
                self._initial_data[field_name][json_field_name] = getattr(self, field_name).get(
                    json_field_name, None
                )
            else:
                self._initial_data[field_name] = getattr(self, field_name)

    def __str__(self):
        return self.__repr__()
