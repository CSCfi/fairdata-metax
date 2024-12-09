# This file is part of the Metax API service
#
# Copyright 2017-2018 Ministry of Education and Culture, Finland
#
# :author: CSC - IT Center for Science Ltd., Espoo Finland <servicedesk@csc.fi>
# :license: MIT

import logging
import uuid
from collections import defaultdict
from copy import deepcopy
from datetime import datetime, timedelta

from django.conf import settings
from django.contrib.auth.models import AnonymousUser
from django.contrib.postgres.fields import ArrayField
from django.db import connection, models, transaction
from django.db.models import JSONField, Q, Sum
from django.http import Http404
from django.http.request import HttpRequest
from django.utils.crypto import get_random_string
from rest_framework.request import Request
from rest_framework.serializers import ValidationError

from metax_api.exceptions import Http400, Http403, Http503
from metax_api.tasks.refdata.refdata_indexer import service
from metax_api.utils import (
    DelayedLog,
    IdentifierType,
    catalog_allows_datacite_update,
    datetime_to_str,
    executing_test_case,
    extract_doi_from_doi_identifier,
    generate_doi_identifier,
    generate_uuid_identifier,
    get_identifier_type,
    get_tz_aware_now_without_micros,
    is_metax_generated_doi_identifier,
    parse_timestamp_string_to_tz_aware_datetime,
)

from .common import Common, CommonManager
from .contract import Contract
from .data_catalog import DataCatalog
from .directory import Directory
from .file import File

READ_METHODS = ("GET", "HEAD", "OPTIONS")
DEBUG = settings.DEBUG
_logger = logging.getLogger(__name__)


ACCESS_TYPES = {
    "open": "http://uri.suomi.fi/codelist/fairdata/access_type/code/open",
    "login": "http://uri.suomi.fi/codelist/fairdata/access_type/code/login",
    "permit": "http://uri.suomi.fi/codelist/fairdata/access_type/code/permit",
    "embargo": "http://uri.suomi.fi/codelist/fairdata/access_type/code/embargo",
    "restricted": "http://uri.suomi.fi/codelist/fairdata/access_type/code/restricted",
}


LEGACY_CATALOGS = settings.LEGACY_CATALOGS
IDA_CATALOG = settings.IDA_DATA_CATALOG_IDENTIFIER
ATT_CATALOG = settings.ATT_DATA_CATALOG_IDENTIFIER
PAS_CATALOG = settings.PAS_DATA_CATALOG_IDENTIFIER
DFT_CATALOG = settings.DFT_DATA_CATALOG_IDENTIFIER


class EditorPermissions(models.Model):
    """
    Shared permissions between linked copies of same dataset.

    Attaches a set of EditorUserPermission objects to a set of CatalogRecords.
    """

    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)

    @property
    def all_users(self):
        return self.users(manager="objects_unfiltered").all()


class PermissionRole(models.TextChoices):
    """Permission role for EditorPermission."""

    CREATOR = "creator"
    EDITOR = "editor"


class EditorUserPermission(Common):
    """Table for attaching user roles to an EditorPermissions object."""

    # Override inherited integer based id with uuid
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)

    # MODEL FIELD DEFINITIONS #
    editor_permissions = models.ForeignKey(
        EditorPermissions, related_name="users", on_delete=models.CASCADE
    )
    user_id = models.CharField(max_length=200)
    role = models.CharField(max_length=16, choices=PermissionRole.choices)

    class Meta:
        indexes = [
            models.Index(
                fields=[
                    "user_id",
                ]
            ),
        ]
        constraints = [
            models.UniqueConstraint(
                fields=["editor_permissions", "user_id"], name="unique_dataset_user_permission"
            ),
            models.CheckConstraint(check=~models.Q(user_id=""), name="require_user_id"),
            models.CheckConstraint(
                check=models.Q(role__in=PermissionRole.values), name="require_role"
            ),
        ]

    def __repr__(self):
        return f"<UserPermission user:{self.user_id} role:{self.role} editor_permissions:{self.editor_permissions_id} >"

    def delete(self, *args, **kwargs):
        super().remove(*args, **kwargs)


class DiscardRecord(Exception):
    pass


class AlternateRecordSet(models.Model):

    """
    A table which contains records that share a common preferred_identifier,
    but belong to different data catalogs.

    Note! Does not inherit from model Common, so does not have timestmap fields,
    and a delete is an actual delete.
    """

    id = models.BigAutoField(primary_key=True, editable=False)

    def print_records(self):  # pragma: no cover
        for r in self.records.all():
            print(r.__repr__())


class DatasetVersionSet(models.Model):

    """
    A table which contains records, that are different dataset versions of each other.

    Note! Does not inherit from model Common, so does not have timestamp fields,
    and a delete is an actual delete.
    """

    id = models.BigAutoField(primary_key=True, editable=False)

    def get_listing(self, only_published=True):
        """
        Return a list of record preferred_identifiers that belong in the same dataset version chain.
        Latest first.
        If only_published is True, return only versions that are in published state.
        """
        records = (
            self.records(manager="objects_unfiltered")
            .order_by("-date_created")
            .only(
                "id",
                "identifier",
                "research_dataset",
                "dataset_version_set_id",
                "date_created",
                "date_removed",
                "removed",
            )
        )
        if only_published:
            records = records.filter(state=CatalogRecord.STATE_PUBLISHED)
        return [r.version_dict for r in records]

    def print_records(self):  # pragma: no cover
        for r in self.records.all():
            print(r.__repr__())


class ResearchDatasetVersion(models.Model):

    date_created = models.DateTimeField()
    stored_to_pas = models.DateTimeField(null=True)
    metadata_version_identifier = models.CharField(max_length=200, unique=True)
    preferred_identifier = models.CharField(max_length=200)
    research_dataset = JSONField()
    catalog_record = models.ForeignKey(
        "CatalogRecord",
        on_delete=models.DO_NOTHING,
        related_name="research_dataset_versions",
    )

    class Meta:
        indexes = [
            models.Index(fields=["metadata_version_identifier"]),
        ]

    def __str__(self):
        return self.__repr__()

    def __repr__(self):
        return "<%s: %d, cr: %d, metadata_version_identifier: %s, stored_to_pas: %s>" % (
            "ResearchDatasetVersion",
            self.id,
            self.catalog_record_id,
            self.metadata_version_identifier,
            str(self.stored_to_pas),
        )


class CatalogRecordManager(CommonManager):
    def get(self, *args, **kwargs):
        if kwargs.get("using_dict", None):
            # for a simple "just get me the instance that equals this dict i have" search.
            # preferred_identifier is not a valid search key, since it wouldnt necessarily
            # work during an update (if preferred_identifier is being updated).

            # this is useful if during a request the url does not contain the identifier (bulk update),
            # and in generic operations where the type of object being handled is not known (also bulk operations).
            row = kwargs.pop("using_dict")
            if row.get("id", None):
                kwargs["id"] = row["id"]
            elif row.get("identifier", None):
                kwargs["identifier"] = row["identifier"]
            elif row.get("research_dataset", None) and row["research_dataset"].get(
                "metadata_version_identifier", None
            ):
                # todo probably remove at some point
                kwargs["research_dataset__contains"] = {
                    "metadata_version_identifier": row["research_dataset"][
                        "metadata_version_identifier"
                    ]
                }
            else:
                raise ValidationError(
                    "this operation requires an identifying key to be present: id, or identifier"
                )
        return super(CatalogRecordManager, self).get(*args, **kwargs)

    def get_id(self, metadata_version_identifier=None):  # pragma: no cover
        """
        Takes metadata_version_identifier, and returns the plain pk of the record. Useful for debugging
        """
        if not metadata_version_identifier:
            raise ValidationError("metadata_version_identifier is a required keyword argument")
        cr = (
            super(CatalogRecordManager, self)
            .filter(
                **{
                    "research_dataset__contains": {
                        "metadata_version_identifier": metadata_version_identifier
                    }
                }
            )
            .values("id")
            .first()
        )
        if not cr:
            raise Http404
        return cr["id"]


class CatalogRecord(Common):

    PRESERVATION_STATE_INITIALIZED = 0
    PRESERVATION_STATE_GENERATING_TECHNICAL_METADATA = 10
    PRESERVATION_STATE_TECHNICAL_METADATA_GENERATED = 20
    PRESERVATION_STATE_TECHNICAL_METADATA_GENERATED_FAILED = 30
    PRESERVATION_STATE_INVALID_METADATA = 40
    PRESERVATION_STATE_METADATA_VALIDATION_FAILED = 50
    PRESERVATION_STATE_VALIDATED_METADATA_UPDATED = 60
    PRESERVATION_STATE_VALIDATING_METADATA = 65
    PRESERVATION_STATE_REJECTED_BY_USER = 70
    PRESERVATION_STATE_METADATA_CONFIRMED = 75
    PRESERVATION_STATE_ACCEPTED_TO_PAS = 80
    PRESERVATION_STATE_IN_PACKAGING_SERVICE = 90
    PRESERVATION_STATE_PACKAGING_FAILED = 100
    PRESERVATION_STATE_SIP_IN_INGESTION = 110
    PRESERVATION_STATE_IN_PAS = 120
    PRESERVATION_STATE_REJECTED_FROM_PAS = 130
    PRESERVATION_STATE_IN_DISSEMINATION = 140

    PRESERVATION_STATE_CHOICES = (
        (PRESERVATION_STATE_INITIALIZED, "Initialized"),
        (PRESERVATION_STATE_GENERATING_TECHNICAL_METADATA, "Generating technical metadata"),
        (
            PRESERVATION_STATE_TECHNICAL_METADATA_GENERATED,
            "Technical metadata generated",
        ),
        (
            PRESERVATION_STATE_TECHNICAL_METADATA_GENERATED_FAILED,
            "Technical metadata generation failed",
        ),
        (PRESERVATION_STATE_INVALID_METADATA, "Invalid metadata"),
        (PRESERVATION_STATE_METADATA_VALIDATION_FAILED, "Metadata validation failed"),
        (PRESERVATION_STATE_VALIDATED_METADATA_UPDATED, "Validated metadata updated"),
        (PRESERVATION_STATE_VALIDATING_METADATA, "Validating metadata"),
        (PRESERVATION_STATE_REJECTED_BY_USER, "Rejected by user"),
        (PRESERVATION_STATE_METADATA_CONFIRMED, "Metadata confirmed"),
        (PRESERVATION_STATE_ACCEPTED_TO_PAS, "Accepted to digital preservation"),
        (PRESERVATION_STATE_IN_PACKAGING_SERVICE, "in packaging service"),
        (PRESERVATION_STATE_PACKAGING_FAILED, "Packaging failed"),
        (
            PRESERVATION_STATE_SIP_IN_INGESTION,
            "SIP sent to ingestion in digital preservation service",
        ),
        (PRESERVATION_STATE_IN_PAS, "in digital preservation"),
        (
            PRESERVATION_STATE_REJECTED_FROM_PAS,
            "Rejected in digital preservation service",
        ),
        (PRESERVATION_STATE_IN_DISSEMINATION, "in dissemination"),
    )

    CUMULATIVE_STATE_NO = 0
    CUMULATIVE_STATE_YES = 1
    CUMULATIVE_STATE_CLOSED = 2

    CUMULATIVE_STATE_CHOICES = (
        (CUMULATIVE_STATE_NO, "no"),
        (CUMULATIVE_STATE_YES, "yes"),
        (CUMULATIVE_STATE_CLOSED, "closed"),
    )

    STATE_PUBLISHED = "published"
    STATE_DRAFT = "draft"

    STATE_CHOICES = ((STATE_PUBLISHED, "published"), (STATE_DRAFT, "draft"))

    # MODEL FIELD DEFINITIONS #

    alternate_record_set = models.ForeignKey(
        AlternateRecordSet,
        on_delete=models.SET_NULL,
        null=True,
        related_name="records",
        help_text="Records which are duplicates of this record, but in another catalog.",
    )

    contract = models.ForeignKey(
        Contract, null=True, on_delete=models.DO_NOTHING, related_name="records"
    )

    data_catalog = models.ForeignKey(
        DataCatalog, on_delete=models.DO_NOTHING, related_name="records"
    )

    state = models.CharField(
        choices=STATE_CHOICES,
        default=STATE_DRAFT,
        max_length=200,
        help_text="Publishing state (published / draft) of the dataset.",
    )

    dataset_group_edit = models.CharField(
        max_length=200,
        blank=True,
        null=True,
        help_text="Group which is allowed to edit the dataset in this catalog record.",
    )

    deprecated = models.BooleanField(
        default=False,
        help_text="Is True when files attached to a dataset have been deleted in IDA.",
    )

    use_doi_for_published = models.BooleanField(
        default=None,
        blank=True,
        null=True,
        help_text='Is True when "Use_DOI" field is checked in Qvain Light for draft.',
    )

    date_deprecated = models.DateTimeField(null=True)

    _directory_data = JSONField(
        null=True,
        help_text="Stores directory data related to browsing files and directories",
    )

    files = models.ManyToManyField(File, related_query_name="record")

    identifier = models.CharField(max_length=200, unique=True, null=False)

    mets_object_identifier = ArrayField(models.CharField(max_length=200), null=True)

    metadata_owner_org = models.CharField(
        max_length=200,
        null=True,
        help_text="Actually non-nullable, but is derived from field metadata_provider_org if omitted.",
    )

    metadata_provider_org = models.CharField(
        max_length=200, null=False, help_text="Non-modifiable after creation"
    )

    metadata_provider_user = models.CharField(
        max_length=200, null=False, help_text="Non-modifiable after creation"
    )

    preservation_dataset_version = models.OneToOneField(
        "self",
        on_delete=models.DO_NOTHING,
        null=True,
        related_name="preservation_dataset_origin_version",
        help_text="Link between a PAS-stored dataset and the originating dataset.",
    )

    preservation_description = models.CharField(
        max_length=200,
        blank=True,
        null=True,
        help_text="Reason for accepting or rejecting PAS proposal.",
    )

    preservation_reason_description = models.CharField(
        max_length=200,
        blank=True,
        null=True,
        help_text="Reason for PAS proposal from the user.",
    )

    preservation_state = models.IntegerField(
        choices=PRESERVATION_STATE_CHOICES,
        default=PRESERVATION_STATE_INITIALIZED,
        help_text="Record state in PAS.",
    )

    preservation_state_modified = models.DateTimeField(
        null=True, help_text="Date of last preservation state change."
    )

    preservation_identifier = models.CharField(max_length=200, unique=True, null=True)

    research_dataset = JSONField()

    next_draft = models.OneToOneField(
        "self",
        on_delete=models.SET_NULL,
        null=True,
        related_name="draft_of",
        help_text="A draft of the next changes to be published on this dataset, in order to be able "
        "to save progress, and continue later. Is created from a published dataset. "
        "When the draft is published, changes are saved on top of the original dataset, "
        "and the draft record is destroyed.",
    )

    next_dataset_version = models.OneToOneField(
        "self", on_delete=models.DO_NOTHING, null=True, related_name="+"
    )

    previous_dataset_version = models.OneToOneField(
        "self", on_delete=models.DO_NOTHING, null=True, related_name="+"
    )

    dataset_version_set = models.ForeignKey(
        DatasetVersionSet,
        on_delete=models.DO_NOTHING,
        null=True,
        related_name="records",
        help_text="Records which are different dataset versions of each other.",
    )

    cumulative_state = models.IntegerField(
        choices=CUMULATIVE_STATE_CHOICES, default=CUMULATIVE_STATE_NO
    )

    date_cumulation_started = models.DateTimeField(
        null=True, help_text="Date when cumulative_state was set to YES."
    )

    date_cumulation_ended = models.DateTimeField(
        null=True, help_text="Date when cumulative_state was set to CLOSED."
    )

    date_last_cumulative_addition = models.DateTimeField(
        null=True,
        default=None,
        help_text="Date of last file addition while actively cumulative.",
    )

    access_granter = JSONField(
        null=True,
        default=None,
        help_text="Stores data of REMS user who is currently granting access to this dataset",
    )

    rems_identifier = models.CharField(
        max_length=200,
        null=True,
        default=None,
        help_text="Defines corresponding catalog item in REMS service",
    )

    api_meta = JSONField(
        null=True,
        default=dict,
        help_text="Saves api related info about the dataset. E.g. api version",
    )

    editor_permissions = models.ForeignKey(
        EditorPermissions, related_name="catalog_records", null=False, on_delete=models.PROTECT
    )

    pid_migrated = models.DateTimeField(
        null=True,
        default=None,
        help_text="DateTimeField that saves the timestamp when the PID has been migrated to PID MS"
    )

    # END OF MODEL FIELD DEFINITIONS #

    """
    Can be set on an instance when updates are requested without creating new versions,
    when a new version would otherwise normally be created.
    Note: Has to be explicitly set on the record before calling instance.save()!
    was orinially created for development/testing purposes. Time will tell if real world
    uses will surface (such as for critical fixes of some sort...)
    """
    preserve_version = False

    """
    Signals to the serializer the need to populate field 'new_version_created'. Includes information of
    the new version as value when a new version is being created.
    """
    new_dataset_version_created = None

    """
    Serializer class to use withing this class, where needed. Allows inheriting classes
    to define their own preference without hardcoding it everywhere.
    """
    serializer_class = None

    """
    Version is used to separate different api versions from each other so that they cannot be cross-edited.
    Inheriting classes should define this in their init.
    """
    api_version = 0

    objects = CatalogRecordManager()

    class Meta:
        indexes = [
            models.Index(fields=["data_catalog"]),
            models.Index(fields=["identifier"]),
        ]
        ordering = ["id"]

    def __init__(self, *args, **kwargs):
        super(CatalogRecord, self).__init__(*args, **kwargs)
        self.track_fields(
            "api_meta",
            "access_granter",
            "cumulative_state",
            "data_catalog_id",
            "date_deprecated",
            "deprecated",
            "identifier",
            "metadata_owner_org",
            "metadata_provider_org",
            "metadata_provider_user",
            "preservation_state",
            "preservation_identifier",
            "research_dataset",
            "research_dataset.files",
            "research_dataset.directories",
            "research_dataset.total_files_byte_size",
            "research_dataset.total_remote_resources_byte_size",
            "research_dataset.metadata_version_identifier",
            "research_dataset.preferred_identifier",
        )
        from metax_api.api.rest.base.serializers import CatalogRecordSerializer

        self.serializer_class = CatalogRecordSerializer
        self.api_version = 1

    def print_files(self):  # pragma: no cover
        for f in self.files.all():
            print(f)

    def user_has_access(self, request):
        """
        In the future, will probably be more involved checking...
        """
        if request.user.is_service:
            if request.method == "GET":
                return True
            if not self._check_catalog_permissions(
                self.data_catalog.catalog_record_group_edit,
                self.data_catalog.catalog_record_services_edit,
                request,
            ):
                return False
            return True

        elif request.method in READ_METHODS:
            if request.user.username == "":  # unauthenticated user
                if self.state == self.STATE_PUBLISHED:
                    return True
                else:
                    raise Http404
            else:  # enduser
                if self.state == self.STATE_PUBLISHED:
                    return True
                elif (
                    self.state == self.STATE_DRAFT
                    and self.metadata_provider_user == request.user.username
                ):
                    return True
                else:
                    raise Http404

        # write operation
        return self.user_is_privileged(request)

    def user_is_owner(self, request):
        if self.state == self.STATE_DRAFT and self.metadata_provider_user != request.user.username:
            _logger.debug(
                "404 due to state == draft and metadata_provider_user != request.user.username"
            )
            _logger.debug("metadata_provider_user = %s", self.metadata_provider_user)
            _logger.debug("request.user.username = %s", request.user.username)
            raise Http404

        if self.metadata_provider_user:
            return request.user.username == self.metadata_provider_user

        # note: once access control plans evolve, user_created may not be a legit field ever
        # to check access from. but until then ...
        return request.user.username == self.user_created

    def user_is_privileged(self, request):
        """
        Perhaps move this to Common model if/when other models need this information and have appropriate methods?

        :param instance:
        :return:
        """
        if request == None:
            return False
        if request.user.is_service:
            if request.method == "GET":
                if not self._check_catalog_permissions(
                    self.data_catalog.catalog_record_group_read,
                    self.data_catalog.catalog_record_services_read,
                    request,
                ):
                    return False
                else:
                    return True
            else:
                return True

        users = self.editor_permissions.users
        ids = users.all().values_list("user_id", flat=True)
        if request.user.username in ids:
            return True
        elif self.user_is_owner(request):
            return True
        else:
            return False

    def _check_catalog_permissions(self, catalog_groups, catalog_services, request=None):
        """
        Some data catalogs can only allow writing datasets from a specific group of users.
        Check if user has group/project which permits creating or editing datasets in
        dataset's selected data catalog.

        Note that there is also parameter END_USER_ALLOWED_DATA_CATALOGS in
        settings.py which dictates which catalogs are open for end users.
        """
        # populates self.request if not existing; happens with DELETE-request when self.request object is empty
        if request:
            self.request = request

        if not self.request:  # pragma: no cover
            # should only only happen when setting up test cases
            assert executing_test_case(), "only permitted when setting up testing conditions"
            return True

        if self.request.user.is_service:
            if catalog_services:
                allowed_services = [i.lower() for i in catalog_services.split(",")]
                from metax_api.services import AuthService

                return AuthService.check_services_against_allowed_services(
                    self.request, allowed_services
                )
            return False

        elif not self.request.user.is_service:
            if catalog_groups:
                allowed_groups = catalog_groups.split(",")

                from metax_api.services import AuthService

                return AuthService.check_user_groups_against_groups(self.request, allowed_groups)
            return True

        _logger.info(
            "Catalog {} is not belonging to any service or group ".format(
                self.data_catalog.catalog_json["identifier"]
            )
        )
        return False

    def _access_type_is_open(self):
        from metax_api.services import CatalogRecordService as CRS

        return CRS.get_research_dataset_access_type(self.research_dataset) == ACCESS_TYPES["open"]

    def _access_type_is_login(self):
        from metax_api.services import CatalogRecordService as CRS

        return CRS.get_research_dataset_access_type(self.research_dataset) == ACCESS_TYPES["login"]

    def _access_type_is_embargo(self):
        from metax_api.services import CatalogRecordService as CRS

        return (
            CRS.get_research_dataset_access_type(self.research_dataset) == ACCESS_TYPES["embargo"]
        )

    def _access_type_is_permit(self):
        from metax_api.services import CatalogRecordService as CRS

        return CRS.get_research_dataset_access_type(self.research_dataset) == ACCESS_TYPES["permit"]

    def _access_type_was_permit(self):
        from metax_api.services import CatalogRecordService as CRS

        return (
            CRS.get_research_dataset_access_type(self._initial_data["research_dataset"])
            == ACCESS_TYPES["permit"]
        )

    def _embargo_is_available(self):
        if not self.research_dataset.get("access_rights", {}).get("available", False):
            return False
        try:
            return get_tz_aware_now_without_micros() >= parse_timestamp_string_to_tz_aware_datetime(
                self.research_dataset.get("access_rights", {}).get("available", {})
            )
        except Exception as e:
            _logger.error(e)
            return False

    def authorized_to_see_catalog_record_files(self, request):
        return (
            self.user_is_privileged(request)
            or self._access_type_is_open()
            or self._access_type_is_login()
            or (self._access_type_is_embargo() and self._embargo_is_available())
        )

    def save(self, *args, **kwargs):
        """
        Note: keys are popped from kwargs, because super().save() will complain if it receives
        unknown keyword arguments.
        """
        if self._operation_is_create():
            self._pre_create_operations(pid_type=kwargs.pop("pid_type", None))
            super(CatalogRecord, self).save(*args, **kwargs)
            self._post_create_operations()
        else:
            self._pre_update_operations()
            super(CatalogRecord, self).save(*args, **kwargs)
            self._post_update_operations()

    def _process_file_changes(self, file_changes, new_record_id, old_record_id):
        """
        Process any changes in files between versions.

        Copy files from the previous version to the new version. Only copy files and directories according
        to files_to_keep and dirs_to_keep_by_project, and separately add any new files according to
        files_to_add and dirs_to_add_by_project.

        Returns a boolean actual_files_changed
        """
        actual_files_changed = False

        files_to_add = file_changes["files_to_add"]
        files_to_remove = file_changes["files_to_remove"]
        files_to_keep = file_changes["files_to_keep"]
        dirs_to_add_by_project = file_changes["dirs_to_add_by_project"]
        dirs_to_remove_by_project = file_changes["dirs_to_remove_by_project"]
        dirs_to_keep_by_project = file_changes["dirs_to_keep_by_project"]

        if (
            files_to_add
            or files_to_remove
            or files_to_keep
            or dirs_to_add_by_project
            or dirs_to_remove_by_project
            or dirs_to_keep_by_project
        ):

            # note: files_to_keep and dirs_to_keep_by_project are also included because we
            # want to create new version on some cumulative_state changes.

            if DEBUG:
                _logger.debug("Detected the following file changes:")

            if files_to_keep:
                # sql to copy single files from the previous version to the new version. only copy those
                # files which have been listed in research_dataset.files
                sql_copy_files_from_prev_version = """
                    insert into metax_api_catalogrecord_files (catalogrecord_id, file_id)
                    select %s as catalogrecord_id, file_id
                    from metax_api_catalogrecord_files as cr_f
                    inner join metax_api_file as f on f.id = cr_f.file_id
                    where catalogrecord_id = %s
                    and file_id in %s
                """
                sql_params_copy_files = [
                    new_record_id,
                    old_record_id,
                    tuple(files_to_keep),
                ]

                if DEBUG:
                    _logger.debug("File ids to keep: %s" % files_to_keep)

            if dirs_to_keep_by_project:
                # sql top copy files from entire directories. only copy files from the upper level dirs found
                # by processing research_dataset.directories.
                sql_copy_dirs_from_prev_version = """
                    insert into metax_api_catalogrecord_files (catalogrecord_id, file_id)
                    select %s as catalogrecord_id, file_id
                    from metax_api_catalogrecord_files as cr_f
                    inner join metax_api_file as f on f.id = cr_f.file_id
                    where catalogrecord_id = %s
                    and (
                        COMPARE_PROJECT_AND_FILE_PATHS
                    )
                    and f.id not in (
                        select file_id from
                        metax_api_catalogrecord_files cr_f
                        where catalogrecord_id = %s
                    )
                """
                sql_params_copy_dirs = [new_record_id, old_record_id]

                copy_dirs_sql = []

                for project, dir_paths in dirs_to_keep_by_project.items():
                    for dir_path in dir_paths:
                        copy_dirs_sql.append(
                            "(f.project_identifier = %s and f.file_path like (%s || '/%%'))"
                        )
                        sql_params_copy_dirs.extend([project, dir_path])

                sql_params_copy_dirs.extend([new_record_id])

                sql_copy_dirs_from_prev_version = sql_copy_dirs_from_prev_version.replace(
                    "COMPARE_PROJECT_AND_FILE_PATHS", " or ".join(copy_dirs_sql)
                )
                # ^ generates:
                # and (
                #   (f.project_identifier = %s and f.file_path like %s/)
                #   or
                #   (f.project_identifier = %s and f.file_path like %s/)
                #   or
                #   (f.project_identifier = %s and f.file_path like %s/)
                # )

                if DEBUG:
                    _logger.debug("Directory paths to keep, by project:")
                    for project, dir_paths in dirs_to_keep_by_project.items():
                        _logger.debug("\tProject: %s" % project)
                        for dir_path in dir_paths:
                            _logger.debug("\t\t%s" % dir_path)

            if dirs_to_add_by_project:
                # sql to add new files by directory path that were not previously included.
                # also takes care of "path is already included by another dir, but i want to check if there
                # are new files to add in there"
                sql_select_and_insert_files_by_dir_path = """
                    insert into metax_api_catalogrecord_files (catalogrecord_id, file_id)
                    select %s as catalogrecord_id, f.id
                    from metax_api_file as f
                    where f.active = true and f.removed = false
                    and (
                        COMPARE_PROJECT_AND_FILE_PATHS
                    )
                    and f.id not in (
                        select file_id from
                        metax_api_catalogrecord_files cr_f
                        where catalogrecord_id = %s
                    )
                """
                sql_params_insert_dirs = [new_record_id]

                add_dirs_sql = []

                for project, dir_paths in dirs_to_add_by_project.items():
                    for dir_path in dir_paths:
                        add_dirs_sql.append(
                            "(f.project_identifier = %s and f.file_path like (%s || '/%%'))"
                        )
                        sql_params_insert_dirs.extend([project, dir_path])

                sql_select_and_insert_files_by_dir_path = (
                    sql_select_and_insert_files_by_dir_path.replace(
                        "COMPARE_PROJECT_AND_FILE_PATHS", " or ".join(add_dirs_sql)
                    )
                )

                sql_params_insert_dirs.extend([new_record_id])

                if DEBUG:
                    _logger.debug("Directory paths to add, by project:")
                    for project, dir_paths in dirs_to_add_by_project.items():
                        _logger.debug("\tProject: %s" % project)
                        for dir_path in dir_paths:
                            _logger.debug("\t\t%s" % dir_path)

            if files_to_add:
                # sql to add any new singular files which were not covered by any directory path
                # being added. also takes care of "path is already included by another dir,
                # but this file did not necessarily exist yet at that time, so add it in case
                # its a new file"
                sql_insert_single_files = """
                    insert into metax_api_catalogrecord_files (catalogrecord_id, file_id)
                    select %s as catalogrecord_id, f.id
                    from metax_api_file as f
                    where f.active = true and f.removed = false
                    and f.id in %s
                    and f.id not in (
                        select file_id from
                        metax_api_catalogrecord_files cr_f
                        where catalogrecord_id = %s
                    )
                """
                sql_params_insert_single = [
                    new_record_id,
                    tuple(files_to_add),
                    new_record_id,
                ]

                if DEBUG:
                    _logger.debug("File ids to add: %s" % files_to_add)

            sql_detect_files_changed = """
                select exists(
                    select a.file_id from metax_api_catalogrecord_files a where a.catalogrecord_id = %s
                    except
                    select b.file_id from metax_api_catalogrecord_files b where b.catalogrecord_id = %s
                ) as compare_new_to_old
                union
                select exists(
                    select a.file_id from metax_api_catalogrecord_files a where a.catalogrecord_id = %s
                    except
                    select b.file_id from metax_api_catalogrecord_files b where b.catalogrecord_id = %s
                ) as compare_old_to_new
            """
            sql_params_files_changed = [
                new_record_id,
                old_record_id,
                old_record_id,
                new_record_id,
            ]

            with connection.cursor() as cr:
                if files_to_keep:
                    cr.execute(sql_copy_files_from_prev_version, sql_params_copy_files)

                if dirs_to_keep_by_project:
                    cr.execute(sql_copy_dirs_from_prev_version, sql_params_copy_dirs)

                if dirs_to_add_by_project:
                    cr.execute(sql_select_and_insert_files_by_dir_path, sql_params_insert_dirs)

                if files_to_add:
                    cr.execute(sql_insert_single_files, sql_params_insert_single)

                cr.execute(sql_detect_files_changed, sql_params_files_changed)

                # fetchall() returns i.e. [(False, ), (True, )] or just [(False, )]
                actual_files_changed = any(v[0] for v in cr.fetchall())

            if DEBUG:
                _logger.debug(
                    "Actual files changed during version change: %s" % str(actual_files_changed)
                )
        else:
            # no files to specifically add or remove - do nothing. shouldnt even be here. the new record
            # created will be discarded
            return False

        return actual_files_changed

    def _find_file_changes(self):
        """
        Find new additions and removed entries in research_metadata.files and directories, and return
        - lists of files to add and remove
        - lists of dirs to add and remove, grouped by project
        - list of files and dirs to keep between versions
        """
        file_description_changes = self._get_metadata_file_changes()

        # not allowing file/dir updates via rest api for deprecated datasets will (hopefully) make
        # the versioning more robust
        if self.deprecated and any(
            file_description_changes["files"]["removed"]
            or file_description_changes["files"]["added"]
            or file_description_changes["directories"]["removed"]
            or file_description_changes["directories"]["added"]
        ):

            raise Http400(
                "Cannot add or remove files/directories from deprecated dataset. "
                "Please use API /rpc/fix_deprecated to fix deprecation and to allow file modifications."
            )

        # after copying files from the previous version to the new version, these arrays hold any new
        # individual files and new directories that should be separately added as well.
        #
        # new file and directory description entries will always have to be included even if they
        # are already included by other directory paths, to cover the case of "user wants to add new files
        # which were frozen later". the sql used for that makes sure that already included files are not
        # copied over twice.
        files_to_add = []
        dirs_to_add_by_project = defaultdict(list)

        # when copying files from the previous version to the new version, files and paths specified
        # in these arrays are not copied over. it may be that those paths would be included in the other
        # directories included in the update, but for simplicity those files will then be added later
        # separately.
        files_to_remove = []
        dirs_to_remove_by_project = defaultdict(list)

        # lists of files and dir entries, which did not change between versions
        files_to_keep = []
        dirs_to_keep_by_project = defaultdict(list)

        # list of projects involved in current changes. handy later when checking user
        # permissions for changed files: no need to retrieve related project identifiers again
        changed_projects = defaultdict(set)

        self._find_new_dirs_to_add(
            file_description_changes,
            dirs_to_add_by_project,
            dirs_to_remove_by_project,
            dirs_to_keep_by_project,
        )

        self._find_new_files_to_add(
            file_description_changes,
            files_to_add,
            files_to_remove,
            files_to_keep,
            changed_projects,
        )

        # involved projects from single files (research_dataset.files) were accumulated
        # in the previous method, but for dirs, its just handy to get the keys from below
        # variables...
        for project_identifier in dirs_to_add_by_project.keys():
            changed_projects["files_added"].add(project_identifier)

        for project_identifier in dirs_to_remove_by_project.keys():
            changed_projects["files_removed"].add(project_identifier)

        return {
            "files_to_add": files_to_add,
            "files_to_remove": files_to_remove,
            "files_to_keep": files_to_keep,
            "dirs_to_add_by_project": dirs_to_add_by_project,
            "dirs_to_remove_by_project": dirs_to_remove_by_project,
            "dirs_to_keep_by_project": dirs_to_keep_by_project,
            "changed_projects": changed_projects,
        }

    def _find_new_dirs_to_add(
        self,
        file_description_changes,
        dirs_to_add_by_project,
        dirs_to_remove_by_project,
        dirs_to_keep_by_project,
    ):
        """
        Based on changes in research_metadata.directories (parameter file_description_changes), find out which
        directories should be kept when copying files from the previous version to the new version,
        and which new directories should be added.
        """
        assert "directories" in file_description_changes

        dir_identifiers = list(file_description_changes["directories"]["added"]) + list(
            file_description_changes["directories"]["removed"]
        )

        dir_details = Directory.objects.filter(identifier__in=dir_identifiers).values(
            "project_identifier", "identifier", "directory_path"
        )

        # skip deprecated datasets, since there might be deleted directories
        if len(dir_identifiers) != len(dir_details) and not self.deprecated:
            existig_dirs = set(d["identifier"] for d in dir_details)
            missing_identifiers = [d for d in dir_identifiers if d not in existig_dirs]
            raise ValidationError(
                {
                    "detail": [
                        "the following directory identifiers were not found:\n%s"
                        % "\n".join(missing_identifiers)
                    ]
                }
            )

        for dr in dir_details:
            if dr["identifier"] in file_description_changes["directories"]["added"]:
                # include all new dirs found, to add files from an entirely new directory,
                # or to search an already existing directory for new files (and sub dirs).
                # as a result the actual set of files may or may not change.
                dirs_to_add_by_project[dr["project_identifier"]].append(dr["directory_path"])

            elif dr["identifier"] in file_description_changes["directories"]["removed"]:
                if not self._path_included_in_previous_metadata_version(
                    dr["project_identifier"], dr["directory_path"]
                ):
                    # only remove dirs that are not included by other directory paths.
                    # as a result the actual set of files may change, if the path is not included
                    # in the new directory additions
                    dirs_to_remove_by_project[dr["project_identifier"]].append(dr["directory_path"])

        # when keeping directories when copying, only the top level dirs are required
        top_dirs_by_project = self._get_top_level_parent_dirs_by_project(
            file_description_changes["directories"]["keep"]
        )

        for project, dirs in top_dirs_by_project.items():
            dirs_to_keep_by_project[project] = dirs

    def _find_new_files_to_add(
        self,
        file_description_changes,
        files_to_add,
        files_to_remove,
        files_to_keep,
        changed_projects,
    ):
        """
        Based on changes in research_metadata.files (parameter file_description_changes), find out which
        files should be kept when copying files from the previous version to the new version,
        and which new files should be added.
        """
        assert "files" in file_description_changes

        add_and_keep_ids = list(file_description_changes["files"]["added"]) + list(
            file_description_changes["files"]["keep"]
        )

        add_and_keep = File.objects.filter(identifier__in=add_and_keep_ids).values(
            "id", "project_identifier", "identifier", "file_path"
        )

        removed_ids = list(file_description_changes["files"]["removed"])

        removed = File.objects_unfiltered.filter(identifier__in=removed_ids).values(
            "id", "project_identifier", "identifier", "file_path"
        )

        file_details = add_and_keep | removed

        if len(add_and_keep_ids) + len(removed_ids) != len(file_details) and not self.deprecated:
            existig_files = set(f["identifier"] for f in file_details)
            missing_identifiers = [f for f in add_and_keep_ids if f not in existig_files]
            missing_identifiers += [f for f in removed_ids if f not in existig_files]
            raise ValidationError(
                {
                    "detail": [
                        "the following file identifiers were not found:\n%s"
                        % "\n".join(missing_identifiers)
                    ]
                }
            )

        for f in file_details:
            if f["identifier"] in file_description_changes["files"]["added"]:
                # include all new files even if it already included by another directory's path,
                # to check later that it is not a file that was created later.
                # as a result the actual set of files may or may not change.
                files_to_add.append(f["id"])
                changed_projects["files_added"].add(f["project_identifier"])
            elif f["identifier"] in file_description_changes["files"]["removed"]:
                if not self._path_included_in_previous_metadata_version(
                    f["project_identifier"], f["file_path"]
                ):
                    # file is being removed.
                    # path is not included by other directories in the previous version,
                    # which means the actual set of files may change, if the path is not included
                    # in the new directory additions
                    files_to_remove.append(f["id"])
                    changed_projects["files_removed"].add(f["project_identifier"])
            elif f["identifier"] in file_description_changes["files"]["keep"]:
                files_to_keep.append(f["id"])

    def _path_included_in_previous_metadata_version(self, project, path):
        """
        Check if a path in a specific project is already included in the path of
        another directory included in the PREVIOUS VERSION dataset selected directories.
        """
        if not hasattr(self, "_previous_highest_level_dirs_by_project"):
            if "directories" not in self._initial_data["research_dataset"]:
                return False
            dir_identifiers = [
                d["identifier"] for d in self._initial_data["research_dataset"]["directories"]
            ]
            self._previous_highest_level_dirs_by_project = (
                self._get_top_level_parent_dirs_by_project(dir_identifiers)
            )
        return any(
            True
            for dr_path in self._previous_highest_level_dirs_by_project.get(project, [])
            if path != dr_path and path.startswith("%s/" % dr_path)
        )

    def delete(self, *args, **kwargs):
        from metax_api.services import CommonService

        self.add_post_request_callable(V3Integration(self, "delete"))

        if (self.api_meta["version"] == 3
            and not self.request.user.is_metax_v3
        ):
            raise ValidationError(
                {
                    "detail": [
                        "Deleting datasets that have been created or updated with API version 3 is allowed only for metax_service"
                    ]
                }
            )

        if kwargs.get("hard") or (
            self.catalog_is_harvested()
            and self.request.user.is_metax_v3
            and CommonService.get_boolean_query_param(self.request, "hard")
        ):
            _logger.info("Deleting dataset %s permanently" % self.identifier)
            self.add_post_request_callable(RabbitMQPublishRecord(self, "delete"))
            super().delete()
            return self.id

        if self.request and CommonService.get_boolean_query_param(self.request, "hard"):
            raise ValidationError(
                {
                    "detail": [
                        "Hard-deleting datasets is allowed only for metax_service service user and in harvested catalogs"
                    ]
                }
            )

        if self.state == self.STATE_DRAFT:
            _logger.info("Deleting draft dataset %s permanently" % self.identifier)

            if self.previous_dataset_version:
                self.previous_dataset_version.next_dataset_version = None
                super(Common, self.previous_dataset_version).save()

            crid = self.id
            super().delete()
            return crid

        elif self.state == self.STATE_PUBLISHED:
            if self.has_alternate_records():
                self._remove_from_alternate_record_set()
            if (
                catalog_allows_datacite_update(self.get_catalog_identifier())
                and is_metax_generated_doi_identifier(self.research_dataset["preferred_identifier"])
            ):
                self.add_post_request_callable(
                    DataciteDOIUpdate(self, self.research_dataset["preferred_identifier"], "delete")
                )

            if self._dataset_has_rems_managed_access() and settings.REMS["ENABLED"]:
                self._pre_rems_deletion("dataset deletion")
                super().save(update_fields=["rems_identifier", "access_granter"])

            self.add_post_request_callable(RabbitMQPublishRecord(self, "delete"))

            log_args = {
                "event": "dataset_deleted",
                "user_id": self.user_modified,
                "catalogrecord": {
                    "identifier": self.identifier,
                    "preferred_identifier": self.preferred_identifier,
                    "data_catalog": self.data_catalog.catalog_json["identifier"],
                },
            }
            if self.catalog_is_legacy():
                # delete permanently instead of only marking as 'removed'
                crid = self.id
                super().delete()
                return crid
            else:
                super().remove(*args, **kwargs)
                log_args["catalogrecord"]["date_removed"] = datetime_to_str(self.date_removed)
                log_args["catalogrecord"]["date_modified"] = datetime_to_str(self.date_modified)

            self.add_post_request_callable(DelayedLog(**log_args))
        return self.id

    def deprecate(self, timestamp=None):
        self.deprecated = True
        self.date_deprecated = self.date_modified = timestamp or get_tz_aware_now_without_micros()

        if self._dataset_has_rems_managed_access() and settings.REMS["ENABLED"]:
            self._pre_rems_deletion("dataset deprecation")
            super().save(update_fields=["rems_identifier", "access_granter"])

        super().save(update_fields=["deprecated", "date_deprecated", "date_modified"])
        self.add_post_request_callable(
            DelayedLog(
                event="dataset_deprecated",
                catalogrecord={
                    "identifier": self.identifier,
                    "date_deprecated": datetime_to_str(self.date_deprecated),
                },
            )
        )

    @property
    def identifiers_dict(self):
        try:
            return {
                "id": self.id,
                "identifier": self.identifier,
                "preferred_identifier": self.research_dataset["preferred_identifier"],
            }
        except:
            return {}

    @property
    def version_dict(self):
        try:
            val = {
                "identifier": self.identifier,
                "preferred_identifier": self.research_dataset["preferred_identifier"],
                "date_created": self.date_created.astimezone().isoformat(),
                "removed": self.removed,
            }
            if self.removed and self.date_removed:
                val["date_removed"] = self.date_removed
            return val
        except:
            return {}

    @property
    def preferred_identifier(self):
        try:
            return self.research_dataset["preferred_identifier"]
        except:
            return None

    @property
    def metadata_version_identifier(self):
        try:
            return self.research_dataset["metadata_version_identifier"]
        except:
            return None

    def preservation_dataset_origin_version_exists(self):
        """
        Helper method due to field preservation_dataset_origin_version being
        the "related" field of field preservation_dataset_version, and the
        attribute does not exist on the instance at all, until the field has
        a proper value. Django weirdness?
        """
        return hasattr(self, "preservation_dataset_origin_version")

    def catalog_publishes_to_etsin(self):
        return self.data_catalog.publish_to_etsin

    def catalog_publishes_to_ttv(self):
        return self.data_catalog.publish_to_ttv

    def catalog_versions_datasets(self):
        return self.data_catalog.catalog_json.get("dataset_versioning", False) is True

    def catalog_is_harvested(self):
        return self.data_catalog.catalog_json.get("harvested", False) is True

    def catalog_is_legacy(self):
        return self.data_catalog.catalog_json["identifier"] in LEGACY_CATALOGS

    def catalog_is_ida(self, data=None):
        if data:
            return (
                DataCatalog.objects.get(pk=data["data_catalog_id"]).catalog_json["identifier"]
                == IDA_CATALOG
            )
        return self.data_catalog.catalog_json["identifier"] == IDA_CATALOG

    def catalog_is_att(self, data=None):
        if data:
            return (
                DataCatalog.objects.get(pk=data["data_catalog_id"]).catalog_json["identifier"]
                == ATT_CATALOG
            )
        return self.data_catalog.catalog_json["identifier"] == ATT_CATALOG

    def catalog_is_pas(self):
        return self.data_catalog.catalog_json["identifier"] == PAS_CATALOG

    def catalog_is_dft(self):
        return self.data_catalog.catalog_json["identifier"] == DFT_CATALOG

    def get_catalog_identifier(self):
        return self.data_catalog.catalog_json["identifier"]

    def is_published(self):
        return self.state == self.STATE_PUBLISHED

    def has_alternate_records(self):
        return bool(self.alternate_record_set)

    def _save_as_draft(self):
        # Drafts are only available for V2 datasets
        return False

    def _generate_issued_date(self):
        if not (self.catalog_is_harvested()):
            if "issued" not in self.research_dataset:
                current_time = get_tz_aware_now_without_micros()
                self.research_dataset["issued"] = datetime_to_str(current_time)[0:10]

    def get_metadata_version_listing(self):
        entries = []
        for entry in self.research_dataset_versions.all():
            entries.append(
                {
                    "id": entry.id,
                    "date_created": entry.date_created,
                    "metadata_version_identifier": entry.metadata_version_identifier,
                }
            )
            if entry.stored_to_pas:
                # dont include null values
                entries[-1]["stored_to_pas"] = entry.stored_to_pas
        return entries

    def _get_user_info_for_rems(self):
        """
        Parses query parameter or token to fetch needed information for REMS user
        """
        if self.request.user.is_service:
            # use constant keys for easier validation
            user_info = {
                "userid": self.access_granter.get("userid"),
                "name": self.access_granter.get("name"),
                "email": self.access_granter.get("email"),
            }
        else:
            # end user api
            user_info = {
                "userid": self.request.user.token.get("CSCUserName"),
                "name": self.request.user.token.get("displayName"),
                "email": self.request.user.token.get("email"),
            }

        if any([v is None for v in user_info.values()]):
            raise Http400("Could not find the needed user information for REMS")

        if not all([isinstance(v, str) for v in user_info.values()]):
            raise Http400("user information fields must be string")

        return user_info

    def _validate_for_rems(self):
        """
        Ensures that all necessary information for REMS access
        """
        if self._access_type_is_permit() and not self.research_dataset["access_rights"].get(
            "license", False
        ):
            raise Http400("You must define license for dataset in order to make it REMS manageable")

        if self.request.user.is_service and not self.access_granter:
            raise Http400("Missing access_granter")

    def _assert_api_version(self):
        if not self.api_meta:
            # This should be possible only for test data
            _logger.warning(f"no api_meta found for {self.identifier}")
            return

        # RPC endpoints are disabled if the dataset has been created or updated with V3
        if (
            self.request
            and self.request.path.split("/")[1].lower() == "rpc"
            and self.api_meta["version"] == 3
        ):
            raise Http400("RPC endpoints are disabled for datasets that have been created or updated with API version 3")

        # Raise an error if the dataset has been created or updated with version that is greater than self.api_version
        # and payload includes api_meta version and the version is not 3
        if (
            self._initial_data["api_meta"] != None
            and self._initial_data["api_meta"]["version"] > self.api_version
            and self.api_meta["version"] != 3
        ):
            raise Http400("Please use the correct api version to edit this dataset")

        # Raise an error if the payload doesn't include api_meta, and the dataset has been updated or created with V3
        if (
            self.request
            and isinstance(self.request.data, dict)
            and self.request.data.get("api_meta") == None
            and self.api_meta["version"] == 3
        ):
            raise Http400("Please use the correct api version to edit this dataset")

    def _set_api_version(self):
        # TODO: Can possibly be deleted when v1 api is removed from use and all
        # datasets have been migrated to v2

        # If the user is metax_service,
        # Check that api_meta["version"] is 3
        # If using another user,
        # use the model's api_version and set it to api_meta

        if self.api_meta.get("version") == 3:
            if self.request and self.request.user.is_metax_v3 == False:
                raise Http400("Only metax_service user can set api version to 3")

        if self.request and self.request.user.is_metax_v3 == True:
            if self.api_meta.get("version") != 3:
                raise Http400("When using metax_service, api_meta['version'] needs to be 3")
            return
        self.api_meta["version"] = self.api_version



    def _pre_create_operations(self, pid_type=None):
        if not self._check_catalog_permissions(
            self.data_catalog.catalog_record_group_create,
            self.data_catalog.catalog_record_services_create,
        ):
            raise Http403(
                {"detail": ["You are not permitted to create datasets in this data catalog."]}
            )

        self.research_dataset["metadata_version_identifier"] = generate_uuid_identifier()
        if self.request.user.is_metax_v3:
            if not self.identifier:
                raise Http400(
                    {"detail": ["Incoming datasets from Metax V3 need to have an identifier"]}
                )
        else:
            self.identifier = generate_uuid_identifier()

        if self.catalog_is_pas():
            # todo: default identifier type could probably be a parameter of the data catalog
            pref_id_type = IdentifierType.DOI
        else:
            pref_id_type = pid_type or self._get_preferred_identifier_type_from_request()

        if self.catalog_is_harvested():
            # in harvested catalogs, the harvester is allowed to set the preferred_identifier.
            # do not overwrite.
            pass

        elif self.catalog_is_legacy():
            if "preferred_identifier" not in self.research_dataset:

                # Reportronic catalog does not need to validate unique identifiers
                # Raise validation error when not reportronic catalog
                if (
                    self.data_catalog.catalog_json["identifier"]
                    != settings.REPORTRONIC_DATA_CATALOG_IDENTIFIER
                ):
                    raise ValidationError(
                        {
                            "detail": [
                                "Selected catalog %s is a legacy catalog. Preferred identifiers are not "
                                "automatically generated for datasets stored in legacy catalogs, nor is "
                                "their uniqueness enforced. Please provide a value for dataset field "
                                "preferred_identifier."
                                % self.data_catalog.catalog_json["identifier"]
                            ]
                        }
                    )
            _logger.info(
                "Catalog %s is a legacy catalog - not generating pid"
                % self.data_catalog.catalog_json["identifier"]
            )
        elif self._save_as_draft():
            self.state = self.STATE_DRAFT
            self.research_dataset["preferred_identifier"] = "draft:%s" % self.identifier
            if self._get_preferred_identifier_type_from_request() == IdentifierType.DOI:
                self.use_doi_for_published = True
            else:
                self.use_doi_for_published = False
        else:
            if pref_id_type == IdentifierType.URN:
                self.research_dataset["preferred_identifier"] = generate_uuid_identifier(
                    urn_prefix=True
                )
            elif pref_id_type == IdentifierType.DOI:
                if not (self.catalog_is_ida() or self.catalog_is_pas()):
                    raise Http400("Cannot create DOI for other than datasets in IDA or PAS catalog")

                _logger.debug("pref_id_type == %s, generating doi" % pref_id_type)
                doi_id = generate_doi_identifier()
                self.research_dataset["preferred_identifier"] = doi_id
                self.preservation_identifier = doi_id
            else:
                _logger.debug(
                    "Identifier type not specified in the request. Using URN identifier for pref id"
                )
                self.research_dataset["preferred_identifier"] = generate_uuid_identifier(
                    urn_prefix=True
                )

        if not self.metadata_owner_org:
            # field metadata_owner_org is optional, but must be set. in case it is omitted,
            # derive from metadata_provider_org.
            self.metadata_owner_org = self.metadata_provider_org

        if "remote_resources" in self.research_dataset:
            self._calculate_total_remote_resources_byte_size()

        if (
            not ("files" in self.research_dataset or "directories" in self.research_dataset)
            and "total_files_byte_size" in self.research_dataset
        ):
            self.research_dataset.pop("total_files_byte_size")

        if self.cumulative_state == self.CUMULATIVE_STATE_CLOSED:
            raise Http400("Cannot create cumulative dataset with state closed")

        elif self.cumulative_state == self.CUMULATIVE_STATE_YES:
            if self.preservation_state > self.PRESERVATION_STATE_INITIALIZED:
                raise Http400("Dataset cannot be cumulative if it is in PAS process")

            self.date_cumulation_started = self.date_created

        self._generate_issued_date()

        self._set_api_version()

        # only new datasets need new EditorPermissions, copies already have one
        if not self.editor_permissions_id:
            self._add_editor_permissions()
            if self.metadata_provider_user:
                self._add_creator_editor_user_permission()

    def _post_create_operations(self):
        if "files" in self.research_dataset or "directories" in self.research_dataset:
            # files must be added after the record itself has been created, to be able
            # to insert into a many2many relation.
            self.files.add(*self._get_dataset_selected_file_ids())
            self._calculate_total_files_byte_size()
            super().save(update_fields=["research_dataset"])  # save byte size calculation
            self.calculate_directory_byte_sizes_and_file_counts()

            if self.cumulative_state == self.CUMULATIVE_STATE_YES:
                self.date_last_cumulative_addition = self.date_created

        other_record = self._check_alternate_records()
        if other_record:
            self._create_or_update_alternate_record_set(other_record)

        if self._save_as_draft():
            # do nothing
            pass
        else:

            self.state = self.STATE_PUBLISHED

            if self.catalog_versions_datasets():
                dvs = DatasetVersionSet()
                dvs.save()
                dvs.records.add(self)

            if (
                get_identifier_type(self.preferred_identifier) == IdentifierType.DOI
                or self.use_doi_for_published is True
            ):
                self._validate_cr_against_datacite_schema()
            self.add_post_request_callable(V3Integration(self, "create"))
            if (
                catalog_allows_datacite_update(self.get_catalog_identifier())
                and is_metax_generated_doi_identifier(self.research_dataset["preferred_identifier"])
            ):
                self.add_post_request_callable(
                    DataciteDOIUpdate(self, self.research_dataset["preferred_identifier"], "create")
                )

            if self._dataset_has_rems_managed_access() and settings.REMS["ENABLED"]:
                self._pre_rems_creation()
                super().save(update_fields=["rems_identifier", "access_granter"])

            super().save()

            self.add_post_request_callable(RabbitMQPublishRecord(self, "create"))

        _logger.info(
            "Created a new <CatalogRecord id: %d, "
            "identifier: %s, "
            "preferred_identifier: %s >" % (self.id, self.identifier, self.preferred_identifier)
        )

        log_args = {
            "catalogrecord": {
                "identifier": self.identifier,
                "preferred_identifier": self.preferred_identifier,
                "data_catalog": self.data_catalog.catalog_json["identifier"],
                "date_created": datetime_to_str(self.date_created),
                "metadata_owner_org": self.metadata_owner_org,
            },
            "user_id": self.user_created or self.service_created,
        }

        if self.previous_dataset_version:
            log_args["event"] = "dataset_version_created"
            log_args["catalogrecord"][
                "previous_version_preferred_identifier"
            ] = self.previous_dataset_version.preferred_identifier
        else:
            log_args["event"] = "dataset_created"

        self.add_post_request_callable(DelayedLog(**log_args))

    def _pre_update_operations(self):

        if not self._check_catalog_permissions(
            self.data_catalog.catalog_record_group_edit,
            self.data_catalog.catalog_record_services_edit,
        ):
            raise Http403(
                {"detail": ["You are not permitted to edit datasets in this data catalog."]}
            )


        # possibly raises 400
        self._assert_api_version()

        if self.field_changed("identifier"):
            # read-only
            self.identifier = self._initial_data["identifier"]

        if self.field_changed("research_dataset.metadata_version_identifier"):
            # read-only
            self.research_dataset["metadata_version_identifier"] = self._initial_data[
                "research_dataset"
            ]["metadata_version_identifier"]

        if self.field_changed("research_dataset.preferred_identifier"):
            if not (self.catalog_is_harvested() or self.catalog_is_legacy()):
                raise Http400(
                    "Cannot change preferred_identifier in datasets in non-harvested catalogs"
                )

        if self.field_changed("research_dataset.total_files_byte_size"):
            # read-only
            if "total_files_byte_size" in self._initial_data["research_dataset"]:
                self.research_dataset["total_files_byte_size"] = self._initial_data[
                    "research_dataset"
                ]["total_files_byte_size"]
            else:
                self.research_dataset.pop("total_files_byte_size")

        if self.field_changed("research_dataset.total_remote_resources_byte_size"):
            # read-only
            if "total_remote_resources_byte_size" in self._initial_data["research_dataset"]:
                self.research_dataset["total_remote_resources_byte_size"] = self._initial_data[
                    "research_dataset"
                ]["total_remote_resources_byte_size"]
            else:
                self.research_dataset.pop("total_remote_resources_byte_size")

        if self.field_changed("preservation_state"):
            if self.cumulative_state == self.CUMULATIVE_STATE_YES:
                raise Http400(
                    "Changing preservation state is not allowed while dataset cumulation is active"
                )
            self._handle_preservation_state_changed()

        if self.field_changed("deprecated") and self._initial_data["deprecated"] is True:
            raise Http400("Cannot change dataset deprecation state from true to false")

        if self.field_changed("date_deprecated") and self._initial_data["date_deprecated"]:
            raise Http400("Cannot change dataset deprecation date when it has been once set")

        if self.field_changed("preservation_identifier"):
            self.preservation_identifier = self._initial_data["preservation_identifier"]

        if not self.metadata_owner_org:
            # can not be updated to null
            self.metadata_owner_org = self._initial_data["metadata_owner_org"]

        if self.field_changed("metadata_provider_org"):
            # read-only after creating
            self.metadata_provider_org = self._initial_data["metadata_provider_org"]

        if self.field_changed("metadata_provider_user"):
            # read-only after creating
            self.metadata_provider_user = self._initial_data["metadata_provider_user"]

        if self.field_changed("data_catalog_id"):
            if self.catalog_is_att(self._initial_data) and self.catalog_is_ida():
                self.research_dataset.pop("remote_resources")
                self.research_dataset.pop("total_remote_resources_byte_size")
                self._handle_metadata_versioning()

        if settings.REMS["ENABLED"]:
            self._pre_update_handle_rems()

        if self.field_changed("research_dataset"):
            if self.preservation_state in (
                self.PRESERVATION_STATE_INVALID_METADATA,  # 40
                self.PRESERVATION_STATE_METADATA_VALIDATION_FAILED,  # 50
                self.PRESERVATION_STATE_REJECTED_BY_USER,
            ):  # 70
                # notifies the user in Hallintaliittyma that the metadata needs to be re-validated
                self.preservation_state = self.PRESERVATION_STATE_VALIDATED_METADATA_UPDATED  # 60
                self.preservation_state_modified = self.date_modified

            self.update_datacite = True
        else:
            self.update_datacite = False

        if self.field_changed("cumulative_state"):
            raise Http400(
                "Cannot change cumulative state using REST API. "
                "use API /rpc/datasets/change_cumulative_state to change cumulative state."
            )

        if self.catalog_is_pas():
            actual_files_changed, _ = self._files_changed()
            if actual_files_changed:
                _logger.info("File changes detected in PAS catalog dataset - aborting")
                raise Http400({"detail": ["File changes not permitted in PAS catalog"]})

        if self.catalog_versions_datasets() and (
            not self.preserve_version or self.cumulative_state == self.CUMULATIVE_STATE_YES
        ):

            if not self.field_changed("research_dataset"):
                # proceed directly to updating current record without any extra measures...
                return

            actual_files_changed, file_changes = self._files_changed()

            if actual_files_changed:

                if self.preservation_state > self.PRESERVATION_STATE_INITIALIZED:  # state > 0
                    raise Http400(
                        {
                            "detail": [
                                "Changing files is not allowed when dataset is in PAS process. Current "
                                "preservation_state = %d. In order to alter associated files, change preservation_state "
                                "back to 0." % self.preservation_state
                            ]
                        }
                    )

                elif self._files_added_for_first_time():
                    # first update from 0 to n files should not create a dataset version. all later updates
                    # will create new dataset versions normally.
                    self.files.add(*self._get_dataset_selected_file_ids())
                    self._calculate_total_files_byte_size()
                    self._handle_metadata_versioning()
                    self.calculate_directory_byte_sizes_and_file_counts()

                elif self.cumulative_state == self.CUMULATIVE_STATE_YES:
                    if file_changes["files_to_remove"] or file_changes["dirs_to_remove_by_project"]:
                        raise Http400(
                            "Cannot delete files or directories from cumulative dataset. "
                            "In order to remove files, close dataset cumulation."
                        )
                    self._handle_cumulative_file_addition(file_changes)

                else:
                    self._create_new_dataset_version()

            else:
                self._handle_metadata_versioning()

        else:
            # non-versioning catalogs, such as harvesters, or if an update
            # was forced to occur without version update.

            if self.preserve_version:

                changes = self._get_metadata_file_changes()

                if any(
                    (
                        changes["files"]["added"],
                        changes["files"]["removed"],
                        changes["directories"]["added"],
                        changes["directories"]["removed"],
                    )
                ):

                    raise ValidationError(
                        {
                            "detail": [
                                "currently trying to preserve version while making changes which may result in files "
                                "being changed is not supported."
                            ]
                        }
                    )

            if self.catalog_is_harvested() and self.field_changed(
                "research_dataset.preferred_identifier"
            ):
                self._handle_preferred_identifier_changed()

    def _post_update_operations(self):
        self.add_post_request_callable(V3Integration(self, "update"))
        if (
            get_identifier_type(self.preferred_identifier) == IdentifierType.DOI
            and self.update_datacite
        ):
            self._validate_cr_against_datacite_schema()
            if (
                catalog_allows_datacite_update(self.get_catalog_identifier())
                and is_metax_generated_doi_identifier(self.research_dataset["preferred_identifier"])
            ):
                self.add_post_request_callable(
                    DataciteDOIUpdate(self, self.research_dataset["preferred_identifier"], "update")
                )

        self.add_post_request_callable(RabbitMQPublishRecord(self, "update"))

        log_args = {
            "event": "dataset_updated",
            "user_id": self.user_modified or self.service_modified,
            "catalogrecord": {
                "identifier": self.identifier,
                "preferred_identifier": self.preferred_identifier,
                "data_catalog": self.data_catalog.catalog_json["identifier"],
                "date_modified": datetime_to_str(self.date_modified),
            },
        }

        self.add_post_request_callable(DelayedLog(**log_args))

    def _validate_cr_against_datacite_schema(self):
        from metax_api.services.datacite_service import (
            DataciteException,
            DataciteService,
            convert_cr_to_datacite_cr_json,
        )

        try:
            DataciteService().get_validated_datacite_json(
                convert_cr_to_datacite_cr_json(self), True
            )
        except DataciteException as e:
            raise Http400(str(e))

    def _pre_update_handle_rems(self):
        if self._dataset_rems_changed():
            if self._dataset_rems_access_type_changed():
                if self._dataset_has_rems_managed_access():
                    self._pre_rems_creation()
                else:
                    self._pre_rems_deletion(reason="access type change")

            elif self._dataset_license_changed() and self._dataset_has_rems_managed_access():
                if self._dataset_has_license():
                    self.add_post_request_callable(
                        REMSUpdate(
                            self,
                            "update",
                            rems_id=self.rems_identifier,
                            reason="license change",
                        )
                    )
                    self.rems_identifier = generate_uuid_identifier()
                    # make sure that access_granter is not changed during license update
                    self.access_granter = self._initial_data["access_granter"]

                else:
                    self._pre_rems_deletion(reason="license deletion")

        elif self.field_changed("access_granter"):
            # do not allow access_granter changes if no real REMS changes occur
            self.access_granter = self._initial_data["access_granter"]

    def _handle_cumulative_file_addition(self, file_changes):
        """
        This method adds files to dataset only if they are explicitly mentioned in research_dataset.
        Changes in already included directories are not checked.
        """
        sql_select_and_insert_files_by_dir_path = """
            insert into metax_api_catalogrecord_files (catalogrecord_id, file_id)
            select %s as catalogrecord_id, f.id
            from metax_api_file as f
            where f.active = true and f.removed = false
            and (
                COMPARE_PROJECT_AND_FILE_PATHS
            )
            and f.id not in (
                select file_id from
                metax_api_catalogrecord_files cr_f
                where catalogrecord_id = %s
            )
            returning id
        """
        sql_params_insert_dirs = [self.id]
        add_dirs_sql = []

        for project, dir_paths in file_changes["dirs_to_add_by_project"].items():
            for dir_path in dir_paths:
                add_dirs_sql.append(
                    "(f.project_identifier = %s and f.file_path like (%s || '/%%'))"
                )
                sql_params_insert_dirs.extend([project, dir_path])

        sql_select_and_insert_files_by_dir_path = sql_select_and_insert_files_by_dir_path.replace(
            "COMPARE_PROJECT_AND_FILE_PATHS", " or ".join(add_dirs_sql)
        )
        sql_params_insert_dirs.extend([self.id])

        sql_insert_single_files = """
            insert into metax_api_catalogrecord_files (catalogrecord_id, file_id)
            select %s as catalogrecord_id, f.id
            from metax_api_file as f
            where f.active = true and f.removed = false
            and f.id in %s
            and f.id not in (
                select file_id from
                metax_api_catalogrecord_files cr_f
                where catalogrecord_id = %s
            )
            returning id
            """
        sql_params_insert_single = [
            self.id,
            tuple(file_changes["files_to_add"]),
            self.id,
        ]

        with connection.cursor() as cr:
            n_files_added = 0
            if file_changes["files_to_add"]:
                cr.execute(sql_insert_single_files, sql_params_insert_single)
                n_files_added += cr.rowcount

            if file_changes["dirs_to_add_by_project"]:
                cr.execute(sql_select_and_insert_files_by_dir_path, sql_params_insert_dirs)
                n_files_added += cr.rowcount

        _logger.info("Added %d files to cumulative dataset %s" % (n_files_added, self.identifier))

        self._calculate_total_files_byte_size()
        self.calculate_directory_byte_sizes_and_file_counts()
        self._handle_metadata_versioning()
        self.date_last_cumulative_addition = self.date_modified

    def _files_added_for_first_time(self):
        """
        Find out if this update is the first time files are being added/changed since the dataset's creation.
        """
        if self.files(manager="objects_unfiltered").exists():
            # current version already has files
            return False

        if self.dataset_version_set.records.count() > 1:
            # for versioned catalogs, when a record is first created, the record is appended
            # to dataset_version_set. More than one dataset versions existing implies files
            # have changed already in the past.
            return False

        metadata_versions_with_files_exist = ResearchDatasetVersion.objects.filter(
            Q(
                Q(research_dataset__files__isnull=False)
                | Q(research_dataset__directories__isnull=False)
            ),
            catalog_record_id=self.id,
        ).exists()

        # metadata_versions_with_files_exist == True implies this "0 to n" update without
        # creating a new dataset version already occurred once
        return not metadata_versions_with_files_exist

    def _pre_rems_creation(self):
        """
        Ensure that all necessary information is avaliable for REMS access
        and save post request callable to create correspoding REMS entity.
        """
        self._validate_for_rems()
        user_info = self._get_user_info_for_rems()
        self.access_granter = user_info
        self.rems_identifier = generate_uuid_identifier()
        self.add_post_request_callable(REMSUpdate(self, "create", user_info=user_info))

    def _pre_rems_deletion(self, reason):
        """
        Delete rems information and save post request callable to close
        corresponding REMS entity.
        """
        self.add_post_request_callable(
            REMSUpdate(self, "close", rems_id=self.rems_identifier, reason=reason)
        )
        self.rems_identifier = None
        self.access_granter = None

    def _dataset_has_rems_managed_access(self):
        """
        Check if dataset uses REMS for managing access.
        """
        return self.catalog_is_ida() and self._access_type_is_permit()

    def _dataset_rems_access_type_changed(self):
        """
        Check if access type has changed so that REMS needs updating
        """
        return self._access_type_is_permit() != self._access_type_was_permit()

    def _dataset_rems_changed(self):
        """
        Check if dataset is updated so that REMS needs to be updated.
        """
        return self.catalog_is_ida() and (
            self._dataset_rems_access_type_changed() or self._dataset_license_changed()
        )

    def _dataset_has_license(self):
        """
        Check if dataset has license defined
        """
        from metax_api.services import CatalogRecordService as CRS

        return bool(CRS.get_research_dataset_license_url(self.research_dataset))

    def _dataset_license_changed(self):
        """
        Check if datasets license is changed. Only consider the first license in the license list
        """
        from metax_api.services import CatalogRecordService as CRS

        return CRS.get_research_dataset_license_url(
            self.research_dataset
        ) != CRS.get_research_dataset_license_url(self._initial_data["research_dataset"])

    def _calculate_total_files_byte_size(self, save_cr=False):
        rd = self.research_dataset
        if "files" in rd or "directories" in rd:
            rd["total_files_byte_size"] = (
                self.files.aggregate(Sum("byte_size"))["byte_size__sum"] or 0
            )
        else:
            rd["total_files_byte_size"] = 0
        if save_cr:
            self.research_dataset = rd
            super(Common, self).save(update_fields=["research_dataset"])

    def _calculate_total_remote_resources_byte_size(self):
        rd = self.research_dataset
        if "remote_resources" in rd:
            rd["total_remote_resources_byte_size"] = sum(
                rr["byte_size"] for rr in rd["remote_resources"] if "byte_size" in rr
            )
        else:
            rd["total_remote_resources_byte_size"] = 0

    def _get_dataset_selected_file_ids(self):
        """
        Parse research_dataset.files and directories, and return a list of ids
        of all unique individual files currently in the db.
        """
        file_ids = []
        file_changes = {"changed_projects": defaultdict(set)}

        if "files" in self.research_dataset:
            file_ids.extend(
                self._get_file_ids_from_file_list(self.research_dataset["files"], file_changes)
            )

        if "directories" in self.research_dataset:
            file_ids.extend(
                self._get_file_ids_from_dir_list(
                    self.research_dataset["directories"], file_ids, file_changes
                )
            )

        self._check_changed_files_permissions(file_changes)

        # note: possibly might wanto to use set() to remove duplicates instead of using ignore_files=list
        # to ignore already included files in _get_file_ids_from_dir_list()
        return file_ids

    def _get_file_ids_from_file_list(self, file_list, file_changes):
        """
        Simply retrieve db ids of files in research_dataset.files. Update file_changes dict with
        affected projects for permissions checking.
        """
        if not file_list:
            return []

        file_pids = [f["identifier"] for f in file_list]

        files = File.objects.filter(identifier__in=file_pids).values(
            "id", "identifier", "project_identifier"
        )
        file_ids = []
        for f in files:
            file_ids.append(f["id"])
            file_changes["changed_projects"]["files_added"].add(f["project_identifier"])

        if len(file_ids) != len(file_pids):
            missing_identifiers = [
                pid for pid in file_pids if pid not in set(f["identifier"] for f in files)
            ]
            raise ValidationError(
                {
                    "detail": [
                        "some requested files were not found. file identifiers not found:\n%s"
                        % "\n".join(missing_identifiers)
                    ]
                }
            )

        return file_ids

    def _get_file_ids_from_dir_list(self, dirs_list, ignore_files, file_changes):
        """
        The field research_dataset.directories can contain directories from multiple
        projects. Find the top-most dirs in each project used, and put their files -
        excluding those files already added from the field research_dataset.files - into
        the db relation dataset_files.

        Also update file_changes dict with affected projects for permissions checking.
        """
        if not dirs_list:
            return []

        dir_identifiers = [d["identifier"] for d in dirs_list]

        highest_level_dirs_by_project = self._get_top_level_parent_dirs_by_project(dir_identifiers)

        # empty queryset
        files = File.objects.none()

        for project_identifier, dir_paths in highest_level_dirs_by_project.items():
            for dir_path in dir_paths:
                files = files | File.objects.filter(
                    project_identifier=project_identifier,
                    file_path__startswith="%s/" % dir_path,
                )
            file_changes["changed_projects"]["files_added"].add(project_identifier)

        return files.exclude(id__in=ignore_files).values_list("id", flat=True)

    def _get_top_level_parent_dirs_by_project(self, dir_identifiers):
        """
        Find top level dirs that are not included by other directories in
        dir_identifiers, and return them in a dict, grouped by project.

        These directories are useful to know for finding files to add and remove by
        a file's file_path.
        """
        if not dir_identifiers:
            return {}

        dirs = (
            Directory.objects.filter(identifier__in=dir_identifiers)
            .values("project_identifier", "directory_path", "identifier")
            .order_by("project_identifier", "directory_path")
        )

        # skip deprecated datasets, since there might be deleted directories
        if len(dirs) != len(dir_identifiers) and not self.deprecated:
            missing_identifiers = [
                pid for pid in dir_identifiers if pid not in set(d["identifier"] for d in dirs)
            ]
            raise ValidationError(
                {
                    "detail": [
                        "some requested directories were not found. directory identifiers not found:\n%s"
                        % "\n".join(missing_identifiers)
                    ]
                }
            )

        # group directory paths by project
        dirs_by_project = defaultdict(list)

        for dr in dirs:
            dirs_by_project[dr["project_identifier"]].append(dr["directory_path"])

        top_level_dirs_by_project = defaultdict(list)

        for proj, dir_paths in dirs_by_project.items():

            for path in dir_paths:

                dir_is_root = [p.startswith("%s/" % path) for p in dir_paths if p != path]

                if all(dir_is_root):
                    # found the root dir. disregard all the rest of the paths, if there were any.
                    top_level_dirs_by_project[proj] = [path]
                    break
                else:
                    path_contained_by_other_paths = [
                        path.startswith("%s/" % p) for p in dir_paths if p != path
                    ]

                    if any(path_contained_by_other_paths):
                        # a child of at least one other path. no need to include it in the list
                        pass
                    else:
                        # "unique", not a child of any other path
                        top_level_dirs_by_project[proj].append(path)

        return top_level_dirs_by_project

    def _files_changed(self):
        file_changes = self._find_file_changes()

        if not file_changes["changed_projects"]:
            # no changes in directory or file entries were detected.
            return False, None
        elif self._files_added_for_first_time():
            return True, None

        # there are changes in directory or file entries. it may be that those changes can be
        # considered to be only "metadata description changes", and no real file changes have happened.

        # but in case of file or directory entry additions, the reason of adding can also be that the
        # user is trying to add files that have been frozen later. therefore if "parent directory is
        # already included", the file changes should still be executed in order to see if there were
        # any newly frozen files added to the dataset as a result. the database knows about the new files,
        # but the metadata descriptions do not.

        self._check_changed_files_permissions(file_changes)
        actual_files_changed = self._create_temp_record(file_changes)

        return actual_files_changed, file_changes

    def _create_temp_record(self, file_changes):
        """
        Create a temporary record to perform the file changes on, to see if there were real file changes.
        If no, discard it. If yes, the temp_record will be transformed into the new dataset version record.
        """
        actual_files_changed = False

        try:
            with transaction.atomic():

                temp_record = self._create_new_dataset_version_template()

                actual_files_changed = self._process_file_changes(
                    file_changes, temp_record.id, self.id
                )

                if actual_files_changed:

                    if self.cumulative_state == self.CUMULATIVE_STATE_YES:
                        _logger.info("Files changed, but dataset is cumulative")
                        raise DiscardRecord()

                    elif self.catalog_is_pas():
                        raise Http400("Cannot change files in a dataset in PAS catalog.")

                    self._new_version = temp_record
                else:
                    _logger.debug(
                        "no real file changes detected, discarding the temporary record..."
                    )
                    raise DiscardRecord()
        except DiscardRecord:
            # rolled back
            pass

        return actual_files_changed

    def _create_new_dataset_version_template(self):
        """
        Create a new version of current record, with unique fields and some relation fields
        popped, and new identifiers generated, so that the record can be used as a template
        for any kind of new version of the dataset.

        The record is saved once immediately, so that it will have a proper db id for later changes,
        such as adding relations.
        """
        new_version_template = self.__class__.objects.get(pk=self.id)
        new_version_template.id = None
        new_version_template.next_dataset_version = None
        new_version_template.previous_dataset_version = None
        new_version_template.dataset_version_set = None
        new_version_template.preservation_dataset_version = None
        new_version_template.identifier = generate_uuid_identifier()
        new_version_template.research_dataset[
            "metadata_version_identifier"
        ] = generate_uuid_identifier()
        new_version_template.preservation_identifier = None
        new_version_template.api_meta["version"] = self.api_version
        super(Common, new_version_template).save()
        return new_version_template

    def _check_changed_files_permissions(self, file_changes):
        """
        Ensure user belongs to projects of all changed files and dirs.

        Raises 403 on error.
        """
        if not self.request:  # pragma: no cover
            # when files associated with a dataset have been changed, the user should be
            # always known, i.e. the http request object is present. if its not, the code
            # is not being executed as a result of a api request. in that case, only allow
            # proceeding when the code is executed for testing: the code is being called directly
            # from a test case, to set up test conditions etc.
            assert executing_test_case(), "only permitted when setting up testing conditions"
            return

        projects_added = file_changes["changed_projects"].get("files_added", set())
        projects_removed = file_changes["changed_projects"].get("files_removed", set())
        project_changes = projects_added.union(projects_removed)

        from metax_api.services import CommonService

        allowed_projects = CommonService.get_list_query_param(self.request, "allowed_projects")

        if allowed_projects is not None:

            if not all(p in allowed_projects for p in project_changes):
                raise Http403(
                    {
                        "detail": [
                            "Unable to update dataset %s. You do not have permissions to all of the files and directories."
                            % self.identifier
                        ]
                    }
                )

        if self.request.user.is_service:
            # assumed the service knows what it is doing
            return

        from metax_api.services import AuthService

        user_projects = AuthService.get_user_projects(self.request)

        invalid_project_perms = [proj for proj in project_changes if proj not in user_projects]

        if invalid_project_perms:
            raise Http403(
                {
                    "detail": [
                        "Unable to add files to dataset. You are lacking project membership in the following projects: %s"
                        % ", ".join(invalid_project_perms)
                    ]
                }
            )

    def _handle_metadata_versioning(self):
        if not self.research_dataset_versions.exists():
            # when a record is initially created, there are no versions.
            # when the first new version is created, first add the initial version.
            first_rdv = ResearchDatasetVersion(
                date_created=self.date_created,
                metadata_version_identifier=self._initial_data["research_dataset"][
                    "metadata_version_identifier"
                ],
                preferred_identifier=self.preferred_identifier,
                research_dataset=self._initial_data["research_dataset"],
                catalog_record=self,
            )
            first_rdv.save()

        # create and add the new metadata version

        self.research_dataset["metadata_version_identifier"] = generate_uuid_identifier()

        new_rdv = ResearchDatasetVersion(
            date_created=self.date_modified,
            metadata_version_identifier=self.research_dataset["metadata_version_identifier"],
            preferred_identifier=self.preferred_identifier,
            research_dataset=self.research_dataset,
            catalog_record=self,
        )
        new_rdv.save()

    def _create_new_dataset_version(self):
        """
        Create a new dataset version of the record who calls this method.
        """
        assert hasattr(
            self, "_new_version"
        ), "self._new_version should have been set in a previous step"
        old_version = self

        if old_version.next_dataset_version_id:
            raise ValidationError(
                {"detail": ["Changing files in old dataset versions is not permitted."]}
            )

        _logger.info(
            "Files changed during CatalogRecord update. Creating new dataset version "
            "from old CatalogRecord having identifier %s" % old_version.identifier
        )
        _logger.debug(
            "Old CR metadata version identifier: %s" % old_version.metadata_version_identifier
        )

        new_version = self._new_version
        new_version.deprecated = False
        new_version.date_deprecated = None
        new_version.contract = None
        new_version.date_created = old_version.date_modified
        new_version.date_modified = None
        new_version.user_created = old_version.user_modified
        new_version.user_modified = None
        new_version.preservation_description = None
        new_version.preservation_state = 0
        new_version.preservation_state_modified = None
        new_version.previous_dataset_version = old_version
        new_version.preservation_identifier = None
        new_version.next_dataset_version_id = None
        new_version.service_created = old_version.service_modified or old_version.service_created
        new_version.service_modified = None
        new_version.alternate_record_set = None
        new_version.date_removed = None
        old_version.dataset_version_set.records.add(new_version)

        # note: copying research_dataset from the currently open instance 'old_version',
        # contains the new field data from the request. this effectively transfers
        # the changes to the new dataset version.
        new_version.research_dataset = deepcopy(old_version.research_dataset)
        new_version.research_dataset["metadata_version_identifier"] = generate_uuid_identifier()

        # This effectively means one cannot change identifier type for new catalog record versions
        pref_id_type = get_identifier_type(old_version.research_dataset["preferred_identifier"])
        if pref_id_type == IdentifierType.URN:
            new_version.research_dataset["preferred_identifier"] = generate_uuid_identifier(
                urn_prefix=True
            )
        elif pref_id_type == IdentifierType.DOI:
            doi_id = generate_doi_identifier()
            new_version.research_dataset["preferred_identifier"] = doi_id
            if self.catalog_is_ida():
                new_version.preservation_identifier = doi_id
        else:
            _logger.debug(
                "This code should never be reached. Using URN identifier for the new version pref id"
            )
            self.research_dataset["preferred_identifier"] = generate_uuid_identifier(
                urn_prefix=True
            )

        new_version._calculate_total_files_byte_size()

        if "remote_resources" in new_version.research_dataset:
            new_version._calculate_total_remote_resources_byte_size()

        # nothing must change in the now old version of research_dataset, so copy
        # from _initial_data so that super().save() does not change it later.
        old_version.research_dataset = deepcopy(old_version._initial_data["research_dataset"])
        old_version.next_dataset_version = new_version

        super(Common, new_version).save()

        new_version.calculate_directory_byte_sizes_and_file_counts()

        new_version.add_post_request_callable(V3Integration(new_version, "create"))
        if (
            catalog_allows_datacite_update(self.get_catalog_identifier())
            and is_metax_generated_doi_identifier(self.research_dataset["preferred_identifier"])
        ):
            self.add_post_request_callable(
                DataciteDOIUpdate(
                    new_version,
                    new_version.research_dataset["preferred_identifier"],
                    "create",
                )
            )

        new_version.add_post_request_callable(RabbitMQPublishRecord(new_version, "create"))

        old_version.new_dataset_version_created = new_version.identifiers_dict
        old_version.new_dataset_version_created["version_type"] = "dataset"

        _logger.info("New dataset version created, identifier %s" % new_version.identifier)
        _logger.debug(
            "New dataset version preferred identifer %s" % new_version.preferred_identifier
        )

    def _get_metadata_file_changes(self):
        """
        Check which files and directories selected in research_dataset have changed, and which to keep.
        This data will later be used when copying files from a previous version to a new version.

        Note: set removes duplicates. It is assumed that file listings do not include
        duplicate files.
        """
        if not self._field_is_loaded("research_dataset"):
            return {}

        if not self._field_initial_value_loaded("research_dataset"):  # pragma: no cover
            self._raise_field_not_tracked_error("research_dataset.files")

        changes = {}

        initial_files = set(
            f["identifier"] for f in self._initial_data["research_dataset"].get("files", [])
        )
        received_files = set(f["identifier"] for f in self.research_dataset.get("files", []))
        changes["files"] = {
            "keep": initial_files.intersection(received_files),
            "removed": initial_files.difference(received_files),
            "added": received_files.difference(initial_files),
        }

        initial_dirs = set(
            dr["identifier"] for dr in self._initial_data["research_dataset"].get("directories", [])
        )
        received_dirs = set(dr["identifier"] for dr in self.research_dataset.get("directories", []))
        changes["directories"] = {
            "keep": initial_dirs.intersection(received_dirs),
            "removed": initial_dirs.difference(received_dirs),
            "added": received_dirs.difference(initial_dirs),
        }

        return changes

    def calculate_directory_byte_sizes_and_file_counts(self):
        """
        Calculate directory byte_sizes and file_counts for all dirs selected for this cr.
        """
        _logger.info("Calculating directory byte_sizes and file_counts...")

        dir_identifiers = file_dir_identifiers = []

        if self.research_dataset.get("directories", None):
            dir_identifiers = [d["identifier"] for d in self.research_dataset["directories"]]

        file_dir_identifiers = []
        if self.research_dataset.get("files", None):
            try:
                file_dir_identifiers = [
                    File.objects.get(identifier=f["identifier"]).parent_directory.identifier
                    for f in self.research_dataset["files"]
                ]
            except Exception as e:
                _logger.error(e)

        if not dir_identifiers and not file_dir_identifiers:
            return

        dir_identifiers = set(dir_identifiers + file_dir_identifiers)

        highest_level_dirs_by_project = self._get_top_level_parent_dirs_by_project(dir_identifiers)

        if len(highest_level_dirs_by_project) == 0:
            return

        directory_data = {}

        for project_identifier, dir_paths in highest_level_dirs_by_project.items():
            dirs = Directory.objects.filter(
                project_identifier=project_identifier, directory_path__in=dir_paths
            )
            for dr in dirs:
                dr.calculate_byte_size_and_file_count_for_cr(self.id, directory_data)

        # assigning dir data to parent directories
        self.get_dirs_by_parent(dirs, directory_data)

        self._directory_data = directory_data
        super(Common, self).save(update_fields=["_directory_data"])

    def get_dirs_by_parent(self, ids, directory_data):
        dirs = Directory.objects.filter(id__in=ids, parent_directory_id__isnull=False).values_list(
            "id", "parent_directory_id"
        )
        ids = []

        if dirs:
            for dir in dirs:
                ids.append(dir[1])
                if directory_data.get(dir[1]):
                    children = Directory.objects.get(pk=dir[1]).child_directories.all()
                    if len(children) == 1 and children.first() == Directory.objects.get(pk=dir[0]):
                        directory_data[dir[1]] = deepcopy(directory_data[dir[0]])
                    else:
                        directory_data[dir[1]][0] += directory_data[dir[0]][0]
                        directory_data[dir[1]][1] += directory_data[dir[0]][1]
                else:
                    directory_data[dir[1]] = deepcopy(directory_data[dir[0]])
            return self.get_dirs_by_parent(ids, directory_data)
        return directory_data

    def _handle_preferred_identifier_changed(self):
        if self.has_alternate_records():
            self._remove_from_alternate_record_set()

        other_record = self._check_alternate_records()

        if other_record:
            self._create_or_update_alternate_record_set(other_record)

    def _handle_preservation_state_changed(self):
        """
        Check if change of value in preservation_state should lead to creating a new PAS version.
        """
        if self.request and self.request.user.is_metax_v3:
            return # Skip preservation state logic for datasets from Metax V3

        self.preservation_state_modified = self.date_modified


        old_value = self._initial_data["preservation_state"]
        new_value = self.preservation_state

        _logger.info("preservation_state changed from %d to %d" % (old_value, new_value))

        if (
            not self.preservation_dataset_origin_version_exists()
            and self.preservation_dataset_version is None
            and self.catalog_is_pas()
        ):
            # dataset was created directly into PAS catalog. do nothing, no rules
            # are enforced (for now)
            _logger.info(
                "preservation_state changed from %d to %d. (native PAS catalog dataset)"
                % (old_value, new_value)
            )
            return

        # standard cases where IDA catalog is involved ->

        if new_value == 0 and self.catalog_is_pas():

            _logger.info("Tried to set preservation_state to 0 on a PAS dataset. Aborting")

            raise Http400(
                {
                    "detail": [
                        "Can't set preservation_state to 0 on a PAS version. Set to %d or %d in order to conclude PAS process."
                        % (
                            self.PRESERVATION_STATE_IN_PAS,
                            self.PRESERVATION_STATE_REJECTED_FROM_PAS,
                        )
                    ]
                }
            )

        elif new_value <= self.PRESERVATION_STATE_ACCEPTED_TO_PAS and self.catalog_is_pas():
            raise Http400(
                {
                    "detail": [
                        "preservation_state values in PAS catalog should be over 80 (accepted to PAS)"
                    ]
                }
            )

        elif new_value > self.PRESERVATION_STATE_ACCEPTED_TO_PAS and not self.catalog_is_pas():
            raise Http400(
                {
                    "detail": [
                        "Maximum value of preservation_state in a non-PAS catalog is 80 (accepted to PAS)"
                    ]
                }
            )

        elif new_value == self.PRESERVATION_STATE_ACCEPTED_TO_PAS:

            if self.catalog_is_pas():
                raise Http400({"detail": ["Dataset is already in PAS catalog"]})
            elif self.preservation_dataset_version:
                raise Http400(
                    {
                        "detail": [
                            "Dataset already has a PAS version. Identifier: %s"
                            % self.preservation_dataset_version.identifier
                        ]
                    }
                )
            else:
                self._create_pas_version(self)

                _logger.info("Resetting preservation_state of original dataset to 0")
                self.preservation_state = self.PRESERVATION_STATE_INITIALIZED
                self.preservation_description = None
                self.preservation_reason_description = None

        elif new_value in (
            self.PRESERVATION_STATE_IN_PAS,
            self.PRESERVATION_STATE_REJECTED_FROM_PAS,
        ):
            _logger.info("PAS-process concluded")
        else:
            _logger.debug("preservation_state change not handled for these values")

    def _create_pas_version(self, origin_version):
        """
        Create PAS version to PAS catalog and add related links.
        """
        _logger.info("Creating new PAS dataset version...")

        if origin_version.preservation_dataset_version_id:

            msg = (
                "Dataset already has a PAS version. Identifier of PAS dataset: %s"
                % origin_version.preservation_dataset_version.identifier
            )

            _logger.info(msg)

            raise Http400({"detail": [msg]})

        try:
            pas_catalog = DataCatalog.objects.only("id").get(
                catalog_json__identifier=settings.PAS_DATA_CATALOG_IDENTIFIER
            )
        except DataCatalog.DoesNotExist:

            msg = "PAS catalog %s does not exist" % settings.PAS_DATA_CATALOG_IDENTIFIER

            _logger.info(msg)

            raise Http400({"detail": [msg]})

        research_dataset = deepcopy(origin_version.research_dataset)
        research_dataset.pop("preferred_identifier", None)
        research_dataset.pop("metadata_version_identifier", None)

        params = {
            "data_catalog": pas_catalog,
            "research_dataset": research_dataset,
            "contract": origin_version.contract,
            "date_created": origin_version.date_modified,
            "service_created": origin_version.service_modified,
            "user_created": origin_version.user_modified,
            "metadata_owner_org": origin_version.metadata_owner_org,
            "metadata_provider_org": origin_version.metadata_provider_org,
            "metadata_provider_user": origin_version.metadata_provider_user,
            "preservation_state": origin_version.preservation_state,
            "preservation_description": origin_version.preservation_description,
            "preservation_reason_description": origin_version.preservation_reason_description,
            "preservation_dataset_origin_version": origin_version,
            "editor_permissions_id": origin_version.editor_permissions_id,
        }

        # add information about other identifiers for this dataset
        other_identifiers_info_origin = {
            "notation": origin_version.preferred_identifier,
            "type": {
                "identifier": get_identifier_type(origin_version.preferred_identifier).value,
            },
        }

        try:
            params["research_dataset"]["other_identifier"].append(other_identifiers_info_origin)
        except KeyError:
            params["research_dataset"]["other_identifier"] = [other_identifiers_info_origin]

        # validate/populate fields according to reference data
        from metax_api.services import CatalogRecordService as CRS, RedisCacheService as cache

        CRS.validate_reference_data(params["research_dataset"], cache, request=origin_version.request)

        # finally create the pas copy dataset
        pas_version = self.__class__(**params)
        pas_version.request = origin_version.request
        pas_version.save(pid_type=IdentifierType.DOI)

        # ensure pas dataset contains exactly the same files as origin dataset. clear the result
        # that was achieved by calling save(), which processed research_dataset.files and research_dataset.directories
        pas_version.files.clear()
        pas_version.files.add(*origin_version.files.filter().values_list("id", flat=True))

        # link origin_version and pas copy
        origin_version.preservation_dataset_version = pas_version
        origin_version.new_dataset_version_created = pas_version.identifiers_dict
        origin_version.new_dataset_version_created["version_type"] = "pas"

        # add information about other identifiers for origin_dataset
        other_identifiers_info_pas = {
            "notation": pas_version.preferred_identifier,
            "type": {
                "identifier": get_identifier_type(pas_version.preferred_identifier).value,
            },
        }

        try:
            origin_version.research_dataset["other_identifier"].append(other_identifiers_info_pas)
        except KeyError:
            origin_version.research_dataset["other_identifier"] = [other_identifiers_info_pas]

        # need to validate ref data again for origin_version
        CRS.validate_reference_data(origin_version.research_dataset, cache, request=origin_version.request)

        self.add_post_request_callable(V3Integration(pas_version, "create"))
        self.add_post_request_callable(RabbitMQPublishRecord(pas_version, "create"))

        _logger.info("PAS dataset version created with identifier: %s" % pas_version.identifier)

    def _check_alternate_records(self):
        """
        Check if there exists records in other catalogs with identical preferred_identifier
        value, and return it.

        It is enough that the first match is returned, because the related alternate_record_set
        (if it exists) can be accessed through it, or, if alternate_record_set does not exist,
        then it is the only duplicate record, and will later be used for creating the record set.

        Note: Due to some limitations in the ORM, using select_related to get CatalogRecord and
        DataCatalog is preferable to using .only() to select specific fields, since that seems
        to cause multiple queries to the db for some reason (even as many as 4!!). The query
        below makes only one query. We are only interested in alternate_record_set though, since
        fetching it now saves another query later when checking if it already exists.
        """
        return (
            CatalogRecord.objects.select_related("data_catalog", "alternate_record_set")
            .filter(research_dataset__contains={"preferred_identifier": self.preferred_identifier})
            .exclude(Q(data_catalog__id=self.data_catalog_id) | Q(id=self.id))
            .first()
        )

    def _create_or_update_alternate_record_set(self, other_record):
        """
        Create a new one, or update an existing alternate_records set using the
        passed other_record.
        """
        if other_record.alternate_record_set:
            # append to existing alternate record set
            other_record.alternate_record_set.records.add(self)
        else:
            # create a new set, and add the current, and the other record to it.
            # note that there should ever only be ONE other alternate record
            # at this stage, if the alternate_record_set didnt exist already.
            ars = AlternateRecordSet()
            ars.save()
            ars.records.add(self, other_record)
            _logger.info(
                "Creating new alternate_record_set for preferred_identifier: %s, with records: %s and %s"
                % (
                    self.preferred_identifier,
                    self.metadata_version_identifier,
                    other_record.metadata_version_identifier,
                )
            )

    def _remove_from_alternate_record_set(self):
        """
        Remove record from previous alternate_records set, and delete the record set if
        necessary.
        """
        if self.alternate_record_set.records.count() <= 2:
            # only two records in the set, so the set can be deleted.
            # delete() takes care of the references in the other record,
            # since models.SET_NULL is used.
            self.alternate_record_set.delete()

        # note: save to db occurs in delete()
        self.alternate_record_set = None

    def add_post_request_callable(self, callable):
        """
        Wrapper in order to import CommonService in one place only...
        In case of drafts, skip other than logging
        In case of V3 integration, skip if integration is not enabled
        If the request was made by metax_service, skip V3 integration and Datacite
        """
        if not self.is_published() and not isinstance(callable, (DelayedLog, V3Integration)):
            _logger.debug(
                f"{self.identifier} is a draft, skipping other than logging and v3 integration post request callables"
            )
            return
        if isinstance(callable, V3Integration) and not settings.METAX_V3["INTEGRATION_ENABLED"]:
            return
        if self.request != None and self.request.user.is_metax_v3 and isinstance(callable, (V3Integration, DataciteDOIUpdate)):
            _logger.debug("Request made by metax_service, skip V3 integration and Datacite update")
            return

        from metax_api.services import CallableService

        CallableService.add_post_request_callable(callable)

    def __repr__(self):
        return (
            "<%s: %d, removed: %s, data_catalog: %s, metadata_version_identifier: %s, "
            "preferred_identifier: %s, file_count: %d >"
            % (
                "CatalogRecord",
                self.id,
                str(self.removed),
                self.data_catalog.catalog_json["identifier"],
                self.metadata_version_identifier,
                self.preferred_identifier,
                self.files.count(),
            )
        )

    def _get_preferred_identifier_type_from_request(self):
        """
        Get preferred identifier type as IdentifierType enum object

        :return: IdentifierType. Return None if parameter not given or is unrecognized. Calling code is then
        responsible for choosing which IdentifierType to use.
        """
        pid_type = self.request.query_params.get("pid_type", None)
        if pid_type == IdentifierType.DOI.value:
            pid_type = IdentifierType.DOI
        elif pid_type == IdentifierType.URN.value:
            pid_type = IdentifierType.URN
        return pid_type

    def change_cumulative_state(self, new_state):
        """
        Change field cumulative_state to new_state. Creates a new dataset version when
        a new cumulative period is started. Returns True if new dataset version is created, otherwise False.
        """
        # might raise 400
        self._assert_api_version()

        if self.next_dataset_version:
            raise Http400("Cannot change cumulative_state on old dataset version")

        cumulative_state_valid_values = [choice[0] for choice in self.CUMULATIVE_STATE_CHOICES]

        try:
            new_state = int(new_state)
            assert new_state in cumulative_state_valid_values
        except:
            raise Http400(
                "cumulative_state must be one of: %s"
                % ", ".join(str(x) for x in cumulative_state_valid_values)
            )

        _logger.info("Changing cumulative_state from %d to %d" % (self.cumulative_state, new_state))

        if self.cumulative_state == new_state:
            _logger.info("No change in cumulative_state")
            return False

        self.date_modified = get_tz_aware_now_without_micros()
        self.service_modified = self.request.user.username if self.request.user.is_service else None

        if not self.user_modified:
            # for now this will probably be true enough, since dataset ownership can not change
            self.user_modified = self.user_created

        if new_state == self.CUMULATIVE_STATE_NO:
            raise Http400(
                "Cumulative dataset cannot be set to non-cumulative dataset. "
                "If you want to stop active cumulation, set cumulative status to closed."
            )

        elif new_state == self.CUMULATIVE_STATE_CLOSED:

            if self.cumulative_state == self.CUMULATIVE_STATE_NO:
                raise Http400("Cumulation cannot be closed for non-cumulative dataset")

            self.date_cumulation_ended = self.date_modified

            self.cumulative_state = new_state

            super().save(
                update_fields=[
                    "cumulative_state",
                    "date_cumulation_ended",
                    "date_modified",
                ]
            )

        elif new_state == self.CUMULATIVE_STATE_YES:

            if self.preservation_state > self.PRESERVATION_STATE_INITIALIZED:
                raise Http400(
                    "Cumulative datasets are not allowed in PAS process. Change preservation_state "
                    "to 0 in order to change the dataset to cumulative."
                )

            new_version = self._create_new_dataset_version_template()
            new_version.date_cumulation_started = self.date_modified
            new_version.date_cumulation_ended = None
            new_version.cumulative_state = new_state
            super(Common, new_version).save()

            # add all files from previous version to new version
            new_version.files.add(*self.files.values_list("id", flat=True))

            self._new_version = new_version

            self._create_new_dataset_version()

            super().save()

        self.add_post_request_callable(V3Integration(self, "update"))
        self.add_post_request_callable(RabbitMQPublishRecord(self, "update"))

        return True if new_state == self.CUMULATIVE_STATE_YES else False

    def refresh_directory_content(self, dir_identifier):
        """
        Checks if there are new files frozen to given directory and adds those files to dataset.
        Creates new version on file addition if dataset is not cumulative.
        Returns True if new dataset version is created, False otherwise.
        """
        # might raise 400
        self._assert_api_version()

        if self.deprecated:
            raise Http400(
                "Cannot update files on deprecated dataset. "
                "You can remove all deleted files from dataset using API /rpc/datasets/fix_deprecated."
            )
        try:
            dir = Directory.objects.get(identifier=dir_identifier)
        except Directory.DoesNotExist:
            raise Http404(f"Directory '{dir_identifier}' could not be found")

        dir_identifiers = [d["identifier"] for d in self.research_dataset["directories"]]
        base_paths = Directory.objects.filter(identifier__in=dir_identifiers).values_list(
            "directory_path", flat=True
        )

        if dir.directory_path not in base_paths and not any(
            [dir.directory_path.startswith(f"{p}/") for p in base_paths]
        ):
            raise Http400(f"Directory '{dir_identifier}' is not included in this dataset")

        added_file_ids = self._find_new_files_added_to_dir(dir)

        if not added_file_ids:
            _logger.info("no change in directory content")
            return (False, 0)

        _logger.info(f"refreshing directory adds {len(added_file_ids)} files to dataset")
        self.date_modified = get_tz_aware_now_without_micros()
        self.service_modified = self.request.user.username if self.request.user.is_service else None

        if self.cumulative_state == self.CUMULATIVE_STATE_YES:
            self.files.add(*added_file_ids)
            self._calculate_total_files_byte_size()
            self.calculate_directory_byte_sizes_and_file_counts()
            self._handle_metadata_versioning()
            self.date_last_cumulative_addition = self.date_modified
        else:
            new_version = self._create_new_dataset_version_template()
            super(Common, new_version).save()

            # add all files from previous version in addition to new ones
            new_version.files.add(*added_file_ids, *self.files.values_list("id", flat=True))

            self._new_version = new_version
            self._create_new_dataset_version()

        super().save()

        self.add_post_request_callable(V3Integration(self, "update"))
        self.add_post_request_callable(RabbitMQPublishRecord(self, "update"))

        return (self.cumulative_state != self.CUMULATIVE_STATE_YES, len(added_file_ids))

    def _find_new_files_added_to_dir(self, dir):
        sql_insert_newly_frozen_files_by_dir_path = """
            select f.id
            from metax_api_file as f
            where f.active = true and f.removed = false
            and f.project_identifier = %s
            and f.file_path like (%s || '/%%')
            and f.id not in (
                select file_id from
                metax_api_catalogrecord_files cr_f
                where catalogrecord_id = %s
            )
        """
        sql_params_insert_new_files = [
            dir.project_identifier,
            dir.directory_path,
            self.id,
        ]

        with connection.cursor() as cr:
            cr.execute(sql_insert_newly_frozen_files_by_dir_path, sql_params_insert_new_files)
            added_file_ids = [v[0] for v in cr.fetchall()]

        return added_file_ids

    def fix_deprecated(self):
        """
        Deletes all removed files and directories from dataset and creates new, non-deprecated version.
        """
        # might raise 400
        self._assert_api_version()

        new_version = self._create_new_dataset_version_template()
        self._new_version = new_version
        self._fix_deprecated_research_dataset()
        self._copy_undeleted_files_from_old_version()
        self._create_new_dataset_version()
        super().save()
        self.add_post_request_callable(V3Integration(self, "update"))
        self.add_post_request_callable(RabbitMQPublishRecord(self, "update"))

    def _fix_deprecated_research_dataset(self):
        if self.research_dataset.get("files"):
            pid_list = [f["identifier"] for f in self.research_dataset["files"]]
            pid_list_fixed = File.objects.filter(identifier__in=pid_list).values_list(
                "identifier", flat=True
            )

            if len(pid_list_fixed) != len(pid_list):
                self.research_dataset["files"] = [
                    f for f in self.research_dataset["files"] if f["identifier"] in pid_list_fixed
                ]
                if not self.research_dataset["files"]:
                    del self.research_dataset["files"]

        if self.research_dataset.get("directories"):
            pid_list = [d["identifier"] for d in self.research_dataset["directories"]]
            pid_list_fixed = Directory.objects.filter(identifier__in=pid_list).values_list(
                "identifier", flat=True
            )

            if len(pid_list_fixed) != len(pid_list):
                self.research_dataset["directories"] = [
                    d
                    for d in self.research_dataset["directories"]
                    if d["identifier"] in pid_list_fixed
                ]
                if not self.research_dataset["directories"]:
                    del self.research_dataset["directories"]

    def _copy_undeleted_files_from_old_version(self):
        copy_undeleted_files_sql = """
            insert into metax_api_catalogrecord_files (catalogrecord_id, file_id)
            select %s as catalogrecord_id, file_id
            from metax_api_catalogrecord_files as cr_f
            inner join metax_api_file as f on f.id = cr_f.file_id
            where catalogrecord_id = %s
            and f.active = true and f.removed = false
            and f.id not in (
                select file_id from
                metax_api_catalogrecord_files cr_f
                where catalogrecord_id = %s
            )
            returning file_id
        """
        sql_params_copy_undeleted = [
            self._new_version.id,
            self.id,
            self._new_version.id,
        ]

        with connection.cursor() as cr:
            cr.execute(copy_undeleted_files_sql, sql_params_copy_undeleted)
            n_files_copied = cr.rowcount

        if DEBUG:
            _logger.debug("Added %d files to dataset %s" % (n_files_copied, self._new_version.id))

    def _add_editor_permissions(self):
        permissions = EditorPermissions.objects.create()
        self.editor_permissions = permissions

    def _add_creator_editor_user_permission(self):
        """
        Add creator permission to a newly created CatalogRecord.
        """
        perm = EditorUserPermission(
            editor_permissions=self.editor_permissions,
            user_id=self.metadata_provider_user,
            role=PermissionRole.CREATOR,
            date_created=self.date_created,
            date_modified=self.date_modified,
            user_created=self.user_created,
            user_modified=self.user_modified,
            service_created=self.service_created,
            service_modified=self.service_modified,
        )
        perm.save()


class RabbitMQPublishRecord:

    """
    Callable object to be passed to CommonService.add_post_request_callable(callable).

    Handles rabbitmq publishing.
    """

    def __init__(self, cr, routing_key):
        assert routing_key in (
            "create",
            "update",
            "delete",
        ), "invalid value for routing_key"
        self.cr = cr
        self.routing_key = routing_key

    def __call__(self):
        """
        The actual code that gets executed during CommonService.run_post_request_callables().
        """
        from metax_api.services import RabbitMQService as rabbitmq

        if self.routing_key == "delete":
            cr_json = {"identifier": self.cr.identifier}
        else:
            cr_json = self._to_json()
            # Send full data_catalog json
            cr_json["data_catalog"] = {"catalog_json": self.cr.data_catalog.catalog_json}

        try:
            if self.cr.catalog_publishes_to_etsin():

                _logger.info(
                    "Publishing CatalogRecord %s to RabbitMQ... exchange: datasets, routing_key: %s"
                    % (self.cr.identifier, self.routing_key)
                )

                rabbitmq.publish(
                    cr_json, routing_key=self.routing_key, exchange="datasets"
                )
            if self.cr.catalog_publishes_to_ttv():
                if self.cr.catalog_is_pas() and self.cr.preservation_state != self.cr.PRESERVATION_STATE_IN_PAS:
                    _logger.info("Not publishing the catalog record to TTV." \
                        " Catalog Record is in PAS catalog and preservation state is not" \
                        f" {self.cr.PRESERVATION_STATE_IN_PAS}")
                    return

                _logger.info(
                    "Publishing CatalogRecord %s to RabbitMQ... exchange: TTV-datasets, routing_key: %s"
                    % (self.cr.identifier, self.routing_key)
                )

                rabbitmq.publish(
                    cr_json, routing_key=self.routing_key, exchange="TTV-datasets"
                )

        except:
            # note: if we'd like to let the request be a success even if this operation fails,
            # we could simply not raise an exception here.
            _logger.exception("Publishing rabbitmq message failed")
            raise Http503(
                {"detail": ["failed to publish updates to rabbitmq. request is aborted."]}
            )

    def _to_json(self):
        serializer_class = self.cr.serializer_class
        return serializer_class(self.cr).data


class REMSUpdate:

    """
    Callable object to be passed to CommonService.add_post_request_callable(callable).

    Handles managing REMS resources when creating, updating and deleting datasets.
    """

    def __init__(self, cr, action, **kwargs):
        from metax_api.services.rems_service import REMSService

        assert action in ("close", "create", "update"), "invalid value for action"
        self.cr = cr
        self.user_info = kwargs.get("user_info")
        self.reason = kwargs.get("reason")
        self.rems_id = kwargs.get("rems_id")

        self.action = action
        self.rems = REMSService()

    def __call__(self):
        """
        The actual code that gets executed during CommonService.run_post_request_callables().

        rems_id is needed when rems entities are closed because saving operations have been done
        before this method is called. This is why the rems_id is saved to REMSUpdate so that
        it can be changed before saving and rems_service still knows what the resource identifier is.
        """
        _logger.info(
            "Publishing CatalogRecord %s update to REMS... action: %s"
            % (self.cr.identifier, self.action)
        )

        try:
            if self.action == "create":
                self.rems.create_rems_entity(self.cr, self.user_info)
            elif self.action == "close":
                self.rems.close_rems_entity(self.rems_id, self.reason)
            elif self.action == "update":
                self.rems.update_rems_entity(self.cr, self.rems_id, self.reason)

        except Exception as e:
            _logger.error(e)
            raise Http503({"detail": ["failed to publish updates to rems. request is aborted."]})


class DataciteDOIUpdate:
    """
    Callable object to be passed to CommonService.add_post_request_callable(callable).

    Handles creating, updating or deleting DOI metadata along with DOI identifier,
    and publishing of URL for resolving the DOI identifier in Datacite API.
    """

    def __init__(self, cr, doi_identifier, action):
        """
        Give doi_identifier as parameter since its location in cr is not always the same

        :param cr:
        :param doi_identifier:
        :param action:
        """
        from metax_api.services.datacite_service import DataciteService

        assert action in ("create", "update", "delete"), "invalid value for action"
        assert is_metax_generated_doi_identifier(doi_identifier)
        self.cr = cr
        self.doi_identifier = doi_identifier
        self.action = action
        self.dcs = DataciteService()

    def __call__(self):
        """
        The actual code that gets executed during CommonService.run_post_request_callables().
        Do not run for tests or in travis
        """

        if hasattr(settings, "DATACITE"):
            if not settings.DATACITE.get("ETSIN_URL_TEMPLATE", None):
                raise Exception(
                    "Missing configuration from settings for DATACITE: ETSIN_URL_TEMPLATE"
                )
        else:
            raise Exception("Missing configuration from settings: DATACITE")

        doi = extract_doi_from_doi_identifier(self.doi_identifier)
        if doi is None:
            return

        if self.action == "create":
            _logger.info(
                "Publishing CatalogRecord {0} metadata and url to Datacite API using DOI {1}".format(
                    self.cr.identifier, doi
                )
            )
        elif self.action == "update":
            _logger.info(
                "Updating CatalogRecord {0} metadata and url to Datacite API using DOI {1}".format(
                    self.cr.identifier, doi
                )
            )
        elif self.action == "delete":
            _logger.info(
                "Deleting CatalogRecord {0} metadata from Datacite API using DOI {1}".format(
                    self.cr.identifier, doi
                )
            )

        from metax_api.services.datacite_service import DataciteException

        try:
            if self.action == "create":
                try:
                    self._publish_to_datacite(doi)
                except Exception as e:
                    # Try to delete DOI in case the DOI got created but stayed in "draft" state
                    self.dcs.delete_draft_doi(doi)
                    raise (Exception(e))
            elif self.action == "update":
                self._publish_to_datacite(doi)
            elif self.action == "delete":
                # If metadata is in "findable" state, the operation below should transition the DOI to "registered"
                # state
                self.dcs.delete_doi_metadata(doi)
        except DataciteException as e:
            _logger.error(e)
            raise Http400(str(e))
        except Exception as e:
            _logger.error(e)
            _logger.exception("Datacite API interaction failed")
            raise Http503(
                {"detail": ["failed to publish updates to Datacite API. request is aborted."]}
            )

    def _publish_to_datacite(self, doi):
        from metax_api.services.datacite_service import convert_cr_to_datacite_cr_json

        cr_json = convert_cr_to_datacite_cr_json(self.cr)
        datacite_xml = self.dcs.convert_catalog_record_to_datacite_xml(cr_json, True, True)
        _logger.debug("Datacite XML to be sent to Datacite API: {0}".format(datacite_xml))

        # When the two operations below are successful, it should result in the DOI transitioning to
        # "findable" state
        self.dcs.create_doi_metadata(datacite_xml)
        self.dcs.register_doi_url(doi, settings.DATACITE["ETSIN_URL_TEMPLATE"] % self.cr.identifier)

class V3Integration:
    def __init__(self, cr, action):
        from metax_api.services.metax_v3_service import MetaxV3Service
        self.v3Service = MetaxV3Service()
        self.cr = cr
        self.action = action

    def __call__(self):
        from metax_api.services.metax_v3_service import MetaxV3UnavailableError
        try:
            if self.action == "create":
                cr_json = self._to_json()
                self.v3Service.create_dataset(cr_json, self._get_file_ids())
            if self.action == "delete":
                self.v3Service.delete_dataset(self.cr.identifier)
            if self.action == "update":
                cr_json = self._to_json()
                self.v3Service.update_dataset(self.cr.identifier, cr_json, self._get_file_ids())
        except MetaxV3UnavailableError as e:
            raise Http503(
                {"detail": ["Metax V3 temporarily unavailable, please try again later."]}
            )

    def _to_json(self):
        serializer_class = self.cr.serializer_class

        # Create request context for serializer
        http_request = HttpRequest()
        http_request.method = "GET"
        http_request.GET["include_user_metadata"] = "true"
        http_request.GET["file_details"] = "true"
        http_request.GET["file_fields"] = "file_path"
        http_request.GET["directory_path"] = "directory_path"
        http_request.GET["include_editor_permissions"] = "true"
        request = Request(http_request)
        request.user = AnonymousUser()
        request.user.is_service = True
        request.user.username = "metax"

        return serializer_class(self.cr, context={"request": request}).data

    def _get_file_ids(self):
        """Serialize dataset files in same format as /datasets/<id>/files?id_list=true."""
        queryset = self.cr.files(manager="objects_unfiltered").order_by("id").values_list("id", flat=True)
        return list(queryset)
