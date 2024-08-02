# This file is part of the Metax API service
#
# Copyright 2017-2018 Ministry of Education and Culture, Finland
#
# :author: CSC - IT Center for Science Ltd., Espoo Finland <servicedesk@csc.fi>
# :license: MIT
import logging
from collections import defaultdict
from typing import List, Optional
import os

from django.db import transaction
from django.conf import settings
from django.utils import timezone
from rest_framework.serializers import ValidationError

from metax_api.models import File
from metax_api.models.file_storage import FileStorage
from rest_framework.serializers import Serializer, ListSerializer, ModelSerializer
from rest_framework import serializers


DEBUG = settings.DEBUG
_logger = logging.getLogger(__name__)


class FileStorageIdentifierField(serializers.RelatedField):
    """Relation field that serializes into identifier of a FileStorage.

    FileStorages are cached in context to avoid repeated DB queries.
    """

    default_error_messages = {
        "does_not_exist": "File storage with {slug_name}={value} does not exist.",
        "invalid": "Invalid value.",
    }

    def __init__(self, **kwargs):
        kwargs["queryset"] = FileStorage.objects.all()
        super().__init__(**kwargs)

    def to_internal_value(self, data):
        queryset = self.get_queryset()
        try:
            cache = self.context.setdefault("file_storages", {})
            if data not in cache:
                cache[data] = queryset.get(file_storage_json__identifier=data)
            return cache[data]

        except FileStorage.DoesNotExist:
            self.fail("does_not_exist", slug_name="identifier", value=str(data))
        except (TypeError, ValueError):
            self.fail("invalid")

    def to_representation(self, obj):
        return obj.file_storage_json.get("identifier")


class FileSyncFromV3Serializer(ModelSerializer):
    file_path = serializers.CharField(max_length=None, trim_whitespace=False)
    file_storage = FileStorageIdentifierField() # E.g. urn:nbn:fi:att:file-storage-ida

    def to_internal_value(self, data):
        from metax_api.api.rest.base.serializers.file_serializer import FileSerializer

        for field in data.keys():
            if field not in self.Meta.fields:
                if field in FileSerializer.Meta.fields:
                    raise ValidationError(
                        {field: ["Value is determined from other fields and should be omitted"]}
                    )
                else:
                    raise ValidationError({field: ["Unknown field"]})

        value = super().to_internal_value(data)

        path = value["file_path"]
        if not path.startswith("/"):
            raise ValidationError({"file_path": [f"Value '{path}' should start with '/'"]})

        # Get service username from context request
        request = self.context["request"]
        value["service_created"] = request.user.username
        value["service_modified"] = request.user.username

        # If file is new, user_created is updated in db
        value["user_created"] = value["user_modified"]

        # Compute file name and format from path
        value["file_name"] = os.path.split(path)[1]
        value["file_format"] = os.path.splitext(path)[1][1:]

        # Use file modification date for timestamps missing from v3
        modified = value["file_modified"]
        value["checksum_checked"] = modified

        value["open_access"] = True # Always assume True, open_access is not used in V3
        return value

    class Meta:
        model = File
        fields = (
            "id",
            "byte_size",
            # Dict-style checksum not supported "checksum"
            "checksum_algorithm",
            "checksum_checked",
            "checksum_value",
            "parent_directory",
            "file_deleted",
            "file_frozen",
            "file_modified",
            "file_path", # Used also for determining file_name and file_format
            "file_storage",
            "file_uploaded",
            "identifier",
            "file_characteristics",
            "file_characteristics_extension",
            "pas_compatible",
            "project_identifier",
            # Common fields
            "user_modified", # Used also for user_created
            "date_modified",
            "date_created",
            "removed",
            "date_removed",
        )

        extra_kwargs = {
            "id": {"read_only": False, "allow_null": True},
            "date_created": {"required": True, "allow_null": False},
            "removed": {"required": True},
        }


class FilesSyncFromV3Service:

    # Fields that may be updated on existing files
    updated_fields = [
        "file_characteristics",
        "file_characteristics_extension",
        "removed",
        "file_modified",
        "file_deleted",
        "date_modified",
        "date_removed",
        "checksum_algorithm",
        "checksum_value",
        "checksum_checked",
        "byte_size",
        "parent_directory_id",
        "user_modified",
        "service_modified",
    ]

    # Fields that raise an error when trying to change them
    const_fields = ["identifier", "file_path", "project_identifier"]

    @classmethod
    def _check_unique_identifier(cls, files: List[File]):
        """Check that new file identifiers don't conflict with existing data."""
        ids = [file.id for file in files]
        other_files = File.objects.exclude(id__in=ids)  # only non-removed files are checked
        conflicts = (
            other_files.filter(identifier__in=[file.identifier for file in files])
            .values_list("identifier", flat=True)
            .distinct()
        )
        if conflicts:
            raise ValidationError(
                {"identifier": [f"Values conflict with another file: {sorted(conflicts)}"]}
            )

    @classmethod
    def _check_unique_file_path(cls, files: List[File]):
        """Check that new file paths don't conflict with existing data."""
        files_by_project = defaultdict(list)
        for file in files:
            files_by_project[file.project_identifier].append(file)

        for project, project_files in files_by_project.items():
            other_files = File.objects.filter(project_identifier=project).exclude(
                id__in=[file.id for file in project_files]
            )  # only non-removed files are checked
            conflicts = (
                other_files.filter(file_path__in=[file.file_path for file in project_files])
                .values_list("file_path", flat=True)
                .distinct()
            )
            if conflicts:
                raise ValidationError(
                    {
                        "identifier": [
                            f"Values conflict with another file in project {project}: {sorted(conflicts)}"
                        ]
                    }
                )

    @classmethod
    def _check_unique_files(cls, files: List[File]):
        cls._check_unique_identifier(files)
        cls._check_unique_file_path(files)

    @classmethod
    def _assign_directories(cls, request, files_data: List[dict]):
        from metax_api.services.file_service import FileService

        user_info = {}
        user_info["service_created"] = request.user.username
        user_info["service_modified"] = request.user.username

        files_by_storage_project = {}
        for file in files_data:
            storage_project = (file["file_storage"], file["project_identifier"])
            files_by_storage_project.setdefault(storage_project, []).append(file)

        for (
            file_storage,
            project_identifier,
        ), project_files in files_by_storage_project.items():
            common_info = {
                **user_info,
                "file_storage": file_storage,
                "project_identifier": project_identifier,
                "date_created": timezone.now(),
            }

            # Create missing directories, assign directories to file data
            FileService._create_directories_from_file_list(
                common_info=common_info,
                initial_data_list=project_files,
            )
            # Directories are assigned to 'parent_directory' as id values and
            # they need to be moved to 'parent_directory_id' to be able to save them
            for file_data in project_files:
                file_data["parent_directory_id"] = file_data.pop("parent_directory")

    @classmethod
    def _upsert_from_v3(cls, files_data: List[dict]):
        """Update, create or mark files removed from V3 sync endpoint.

        - If input file has id, get file with id (regardless of removal status).
        - If input file has no id, get non-removed file with identifier.
        - If file was found, update existing file.
        - If file was not found, create new file (use id if one was provided).
        """
        ids = [file["id"] for file in files_data if file["id"] is not None]
        all_files_by_id = {file.id: file for file in File.objects_unfiltered.filter(id__in=ids)}
        identifiers_without_id = [file["identifier"] for file in files_data if file["id"] is None]
        nonremoved_files_by_identifier = {
            file.identifier: file
            for file in File.objects.filter(identifier__in=identifiers_without_id)
        }

        changed_fields = set() # Keep track of which fields need updating
        created_files = []
        updated_files = []
        unchanged_files = []
        unique_check_files = [] # Files that need uniqueness checks (created non-removed files)
        for file_data in files_data:
            file_id = file_data["id"]
            file_identifier = file_data["identifier"]
            file: Optional[File] = all_files_by_id.get(file_id) or nonremoved_files_by_identifier.get(
                file_identifier
            )
            if file:
                # File with id found, update existing values
                changed = False

                for field in cls.const_fields:
                    new_value = file_data.get(field)
                    if getattr(file, field) != new_value:
                        raise ValidationError(
                            {
                                field: f"Value does not match existing value for file {file.id} ({file.identifier})"
                            }
                        )
                for field in cls.updated_fields:
                    new_value = file_data.get(field)
                    if getattr(file, field) != new_value:
                        changed = True
                        setattr(file, field, new_value)
                        changed_fields.add(field)
                if changed:
                    updated_files.append(file)
                else:
                    unchanged_files.append(file)
            else:
                # No id or id does not exist yet
                file = File(**file_data)
                created_files.append(file)
                if not file.removed:
                    unique_check_files.append(file)

        if updated_files:
            # Because bulk_update becomes much slower for each updated field,
            # specify only fields that actually need to be updated
            _logger.info(f"Sync from V3: Updating {len(updated_files)} existing files...")
            File.objects_unfiltered.bulk_update(
                updated_files, fields=changed_fields, batch_size=1000
            )
        if created_files:
            _logger.info(f"Sync from V3: Creating {len(updated_files)} new files...")
            created_files = File.objects_unfiltered.bulk_create(created_files, batch_size=1000)

        # Raise error and rollback transaction if the changes caused uniqueness conflicts.
        cls._check_unique_files(unique_check_files)

        return sorted(created_files + updated_files + unchanged_files, key=lambda f: f.id)

    @classmethod
    def sync_from_v3(cls, request, file_data):
        """Synchronize files from V3.

        Creates, updates, or soft deletes files and
        creates and deletes directories as necessary.

        Datasets that contain removed files are deprecated.
        """
        from metax_api.services.file_service import FileService

        with transaction.atomic():
            cls._assign_directories(request, file_data)

            files: List[File] = cls._upsert_from_v3(file_data)

            # If files were removed, check if we need to remove directories
            projects = set(file.project_identifier for file in files if file.removed)
            for project in projects:
                # Note that directories are hard deleted, so removed files
                # get parent_directory=None if their parent directory no longer exists
                FileService._find_and_delete_empty_directories(project)

            # Deprecate datasets if needed
            FileService._mark_datasets_as_deprecated([file.id for file in files if file.removed])

            return [
                {
                    "id": file.id,
                    "identifier": file.identifier,
                    "file_storage": file.file_storage.file_storage_json.get("identifier"),
                }
                for file in files
            ]
