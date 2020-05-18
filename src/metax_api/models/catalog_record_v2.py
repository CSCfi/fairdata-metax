# This file is part of the Metax API service
#
# Copyright 2017-2018 Ministry of Education and Culture, Finland
#
# :author: CSC - IT Center for Science Ltd., Espoo Finland <servicedesk@csc.fi>
# :license: MIT

from collections import defaultdict
from copy import deepcopy
import logging

from django.conf import settings
from django.db.models import Q

from metax_api.exceptions import Http400, Http403
from metax_api.utils import (
    datetime_to_str,
    DelayedLog,
    generate_doi_identifier,
    generate_uuid_identifier,
    get_identifier_type,
    get_tz_aware_now_without_micros,
    IdentifierType,
)
from .common import Common
from .directory import Directory
from .file import File
from .catalog_record import (
    CatalogRecord,
    DataciteDOIUpdate,
    DatasetVersionSet,
    RabbitMQPublishRecord,
)


_logger = logging.getLogger(__name__)


"""
A version V2 proxy model for CatalogRecord.

Note that in this class, calling super().some_method() actually calls CatalogRecord.some_method(),
instead of CommonModel.some_method() !!! Therefore, if wishing to call the actual CommonModel method,
or even the base Model, super(Common, self).some_method() should be called instead. Calling just
super().some_method() can cause some unnecessary processing and prevents editing some fields, so
may want to avoid that.

At least UNTIL this class fully replaces the V1 version.
"""


class CatalogRecordV2(CatalogRecord):

    class Meta:
        # CatalogRecordV2 operates on the same database table as CatalogRecord model. Only the class
        # behaviour may differ from base class.
        proxy = True

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        from metax_api.api.rest.v2.serializers import CatalogRecordSerializerV2
        self.serializer_class = CatalogRecordSerializerV2

    def save(self, *args, **kwargs):
        if self._operation_is_create():
            self._pre_create_operations()
            super(CatalogRecord, self).save(*args, **kwargs)
            self._post_create_operations(pid_type=kwargs.pop('pid_type', None))
        else:
            self._pre_update_operations()
            super(CatalogRecord, self).save(*args, **kwargs)
            self._post_update_operations()

    def _pre_create_operations(self):

        if not self._check_catalog_permissions(self.data_catalog.catalog_record_group_create,
                self.data_catalog.catalog_record_services_create):
            raise Http403({ 'detail': [ 'You are not permitted to create datasets in this data catalog.' ]})

        self.research_dataset['metadata_version_identifier'] = generate_uuid_identifier()
        self.identifier = generate_uuid_identifier()

    def _post_create_operations(self, pid_type=None):

        if 'files' in self.research_dataset or 'directories' in self.research_dataset:

            # files must be added after the record itself has been created, to be able
            # to insert into a many2many relation.

            file_changes = {
                'files': self.research_dataset.get('files', []),
                'directories': self.research_dataset.get('directories', []),
            }

            self.change_files(file_changes, operation_is_create=True)

        if self._save_as_draft():

            _logger.debug('Saving new dataset as draft')

            self.research_dataset['preferred_identifier'] = 'draft:%s' % self.identifier

            super(Common, self).save(update_fields=['research_dataset'])

            _logger.info(
                'Created a new <CatalogRecord id: %d, identifier: %s, state: draft>'
                % (self.id, self.identifier)
            )

        else:
            self.publish_dataset(pid_type=pid_type)

        # logs correctly whether dataset is created into draft state, or also published

        log_args = {
            'catalogrecord': {
                'identifier': self.identifier,
                'preferred_identifier': self.preferred_identifier,
                'data_catalog': self.data_catalog.catalog_json['identifier'],
                'date_created': datetime_to_str(self.date_created),
                'metadata_owner_org': self.metadata_owner_org,
                'state': self.state,
            },
            'user_id': self.user_created or self.service_created,
        }

        self.add_post_request_callable(DelayedLog(**log_args))

    def publish_dataset(self, pid_type=None):
        """
        Execute actions necessary to make the dataset publicly findable, in the following order:
        - set status to 'published'
        - generate preferred_identifier according to requested pid_type
        - publish objects to REMS as necessary
        - publish metadata to datacite as necessary
        - publish message to rabbitmq

        Note: The last three steps are executed at the very end of the HTTP request, but they are
        queued in this method.
        """

        _logger.debug('Publishing dataset...')

        if self.state == self.STATE_PUBLISHED:
            raise Http400('Dataset is already published.')

        self.state = self.STATE_PUBLISHED

        if self.catalog_is_pas():
            _logger.debug('Catalog is PAS - Using DOI as pref_id_type')
            # todo: default identifier type could probably be a parameter of the data catalog
            pref_id_type = IdentifierType.DOI
        else:
            pref_id_type = pid_type or self._get_preferred_identifier_type_from_request()

        if self.catalog_is_harvested():
            _logger.debug('Note: Catalog is harvested')
            # in harvested catalogs, the harvester is allowed to set the preferred_identifier.
            # do not overwrite.
            pass

        elif self.catalog_is_legacy():
            if 'preferred_identifier' not in self.research_dataset:
                raise Http400(
                    'Selected catalog %s is a legacy catalog. Preferred identifiers are not '
                    'automatically generated for datasets stored in legacy catalogs, nor is '
                    'their uniqueness enforced. Please provide a value for dataset field '
                    'preferred_identifier.' % self.data_catalog.catalog_json['identifier']
                )
            _logger.info(
                'Catalog %s is a legacy catalog - not generating pid'
                % self.data_catalog.catalog_json['identifier']
            )
        else:
            if pref_id_type == IdentifierType.URN:
                self.research_dataset['preferred_identifier'] = generate_uuid_identifier(urn_prefix=True)
            elif pref_id_type == IdentifierType.DOI:
                if not (self.catalog_is_ida() or self.catalog_is_pas()):
                    raise Http400("Cannot create DOI for other than datasets in IDA or PAS catalog")

                _logger.debug('pref_id_type == %s, generating doi' % pref_id_type)
                doi_id = generate_doi_identifier()
                self.research_dataset['preferred_identifier'] = doi_id
                self.preservation_identifier = doi_id
            else:
                _logger.info("Identifier type not specified in the request. Using URN identifier for pref id")
                # todo better to raise validation error instead
                self.research_dataset['preferred_identifier'] = generate_uuid_identifier(urn_prefix=True)

        if not self.metadata_owner_org:
            # field metadata_owner_org is optional, but must be set. in case it is omitted,
            # derive from metadata_provider_org.
            self.metadata_owner_org = self.metadata_provider_org

        if 'remote_resources' in self.research_dataset:
            self._calculate_total_remote_resources_byte_size()

        if self.cumulative_state == self.CUMULATIVE_STATE_CLOSED:
            raise Http400('Cannot publish cumulative dataset with state closed')

        elif self.cumulative_state == self.CUMULATIVE_STATE_YES:
            if self.preservation_state > self.PRESERVATION_STATE_INITIALIZED:
                raise Http400('Dataset cannot be cumulative if it is in PAS process')

            self.date_cumulation_started = self.date_created

        other_record = self._check_alternate_records()
        if other_record:
            self._create_or_update_alternate_record_set(other_record)

        if self.catalog_versions_datasets():
            dvs = DatasetVersionSet()
            dvs.save()
            dvs.records.add(self)

        if get_identifier_type(self.preferred_identifier) == IdentifierType.DOI:

            self._validate_cr_against_datacite_schema()

            self.add_post_request_callable(
                DataciteDOIUpdate(self, self.research_dataset['preferred_identifier'], 'create')
            )

        if self._dataset_has_rems_managed_access() and settings.REMS['ENABLED']:
            self._pre_rems_creation()

        super(Common, self).save()

        _logger.info(
            'Published <CatalogRecord id: %d, identifier: %s, preferred_identifier: %s, state: published>'
            % (self.id, self.identifier, self.preferred_identifier)
        )

        self.add_post_request_callable(RabbitMQPublishRecord(self, 'create'))

    def _pre_update_operations(self):

        if not self._check_catalog_permissions(self.data_catalog.catalog_record_group_edit,
                self.data_catalog.catalog_record_services_edit):
            raise Http403({ 'detail': [ 'You are not permitted to edit datasets in this data catalog.' ]})

        if self.field_changed('identifier'):
            # read-only
            self.identifier = self._initial_data['identifier']

        if self.field_changed('research_dataset.metadata_version_identifier'):
            # read-only
            self.research_dataset['metadata_version_identifier'] = \
                self._initial_data['research_dataset']['metadata_version_identifier']

        if self.field_changed('research_dataset.preferred_identifier'):
            if not (self.catalog_is_harvested() or self.catalog_is_legacy()):
                raise Http400("Cannot change preferred_identifier in datasets in non-harvested catalogs")

        if self.field_changed('research_dataset.total_files_byte_size'):
            # read-only
            if 'total_files_byte_size' in self._initial_data['research_dataset']:
                self.research_dataset['total_files_byte_size'] = \
                    self._initial_data['research_dataset']['total_files_byte_size']
            else:
                self.research_dataset.pop('total_files_byte_size')

        if self.field_changed('research_dataset.total_remote_resources_byte_size'):
            # read-only
            if 'total_remote_resources_byte_size' in self._initial_data['research_dataset']:
                self.research_dataset['total_remote_resources_byte_size'] = \
                    self._initial_data['research_dataset']['total_remote_resources_byte_size']
            else:
                self.research_dataset.pop('total_remote_resources_byte_size')

        if self.field_changed('preservation_state'):
            if self.cumulative_state == self.CUMULATIVE_STATE_YES:
                raise Http400('Changing preservation state is not allowed while dataset cumulation is active')
            self._handle_preservation_state_changed()

        if self.field_changed('deprecated') and self._initial_data['deprecated'] is True:
            raise Http400("Cannot change dataset deprecation state from true to false")

        if self.field_changed('date_deprecated') and self._initial_data['date_deprecated']:
            raise Http400("Cannot change dataset deprecation date when it has been once set")

        if self.field_changed('preservation_identifier'):
            self.preservation_identifier = self._initial_data['preservation_identifier']

        if not self.metadata_owner_org:
            # can not be updated to null
            self.metadata_owner_org = self._initial_data['metadata_owner_org']

        if self.field_changed('metadata_provider_org'):
            # read-only after creating
            self.metadata_provider_org = self._initial_data['metadata_provider_org']

        if self.field_changed('metadata_provider_user'):
            # read-only after creating
            self.metadata_provider_user = self._initial_data['metadata_provider_user']

        if settings.REMS['ENABLED']:
            self._pre_update_handle_rems()

        if self.field_changed('cumulative_state'):
            raise Http400(
                "Cannot change cumulative state using REST API. "
                "Use API /rpc/datasets/change_cumulative_state to change cumulative state."
            )

        # do not permit editing files or directories through general save to /rest/datasets/pid
        if 'files' in self._initial_data['research_dataset']:
            self.research_dataset['files'] = self._initial_data['research_dataset']['files']

        if 'directories' in self._initial_data['research_dataset']:
            self.research_dataset['directories'] = self._initial_data['research_dataset']['directories']

        if self.field_changed('research_dataset'):

            self.update_datacite = True

            if self.preservation_state in (
                    self.PRESERVATION_STATE_INVALID_METADATA,           # 40
                    self.PRESERVATION_STATE_METADATA_VALIDATION_FAILED, # 50
                    self.PRESERVATION_STATE_VALID_METADATA):            # 70

                # notifies the user in Hallintaliittyma that the metadata needs to be re-validated
                self.preservation_state = self.PRESERVATION_STATE_VALIDATED_METADATA_UPDATED # 60
                self.preservation_state_modified = self.date_modified

            if self.catalog_versions_datasets():
                if self.preserve_version:
                    # do nothing
                    pass
                else:
                    self._handle_metadata_versioning()

            elif self.catalog_is_harvested():
                if self.field_changed('research_dataset.preferred_identifier'):
                    self._handle_preferred_identifier_changed()

        else:
            self.update_datacite = False

    def _update_dataset_specific_metadata(self, file_changes, operation_is_create=False):
        """
        Take dataset-specific metadata (also knows as "user metadata") from file_changes and persist
        them in research_dataset.files and research_dataset.directories.

        For completely new entries, the entry must include all necessary fields required by the dataset's
        schema. For existing entries, the existing entry is updated either by replacing it, or field by field,
        if using PATCH.
        """
        _logger.debug('Updating files dataset-specific metadata...')

        for object_type in ('files', 'directories'):

            if not file_changes.get(object_type):
                _logger.debug('No objects of type %s - continuing' % object_type)
                continue

            # filter in only entries that ADD files. makes no sense to keep entries that exclude stuff
            add_entries = [
                obj for obj in file_changes[object_type]
                if obj.get('exclude', False) is False
                and obj.get('delete', False) is False
            ]

            # remove the exclude and delete -keys, if they happen to exist. after that, if the obj contains
            # more than 1 key (the obj identifier), we know that it must contain dataset-specific metadata too.
            for obj in add_entries:
                obj.pop('exclude', None)
                obj.pop('delete', None)

            add_entries = [ obj for obj in add_entries if len(obj) > 1 ]

            _logger.debug('Received %d add entries' % len(add_entries))

            if operation_is_create:

                _logger.debug('Note: operation_is_create=True - replacing all %s with received entries' % object_type)

                if add_entries:
                    # if there are no entries to add, do NOT set an empty array!
                    self.research_dataset[object_type] = add_entries

                break

            # else -> update type operation

            _logger.debug('Update operation type = %s' % self.request.META['REQUEST_METHOD'])

            # take entries that delete metadata, and process them at the end
            delete_entries = set(
                obj['identifier'] for obj in file_changes[object_type] if obj.get('delete', False) is True
            )

            _logger.debug('Received %d delete entries' % len(delete_entries))

            new_entries = []

            num_updated = 0

            # for all existing entries, update the dataset-specific metadata if the new received
            # entry contained any. for competely new entries, collect them into a separate array
            # and append it to research_dataset[object_type] after this loop.
            for received_obj in add_entries:

                for current_obj in self.research_dataset.get(object_type, []):

                    if received_obj['identifier'] == current_obj['identifier']:

                        if self.request.META['REQUEST_METHOD'] in ('POST', 'PUT'):
                            # replace object: drop all keys, and add only received keys below since re-assigning the
                            # current_obj a new reference does not change the actual object in the array.
                            current_obj.clear()

                        for field_name, field_value in received_obj.items():
                            # if a received value was null (None in python), the field is left out. therefore
                            # using null values is a way to clear fields for the user.
                            if field_value is not None:
                                current_obj[field_name] = field_value

                        num_updated += 1

                        break

                else:
                    new_entries.append(received_obj)

            try:
                self.research_dataset[object_type] += new_entries
            except KeyError:
                self.research_dataset[object_type] = new_entries

            # finally, delete all dataset-specific metadata entries as requested
            self.research_dataset[object_type] = [
                obj for obj in self.research_dataset[object_type]
                if obj['identifier'] not in delete_entries
            ]

            if len(self.research_dataset[object_type]) == 0:
                # do not leave empty arrays in the dict
                del self.research_dataset[object_type]

            _logger.debug('Number of %s metadata entries added: %d' % (object_type, len(new_entries)))
            _logger.debug('Number of %s metadata entries updated: %d' % (object_type, num_updated))
            _logger.debug('Number of %s metadata entries deleted: %d' % (object_type, len(delete_entries)))

        super(Common, self).save(update_fields=['research_dataset'])

    def _files_added_for_first_time(self):
        """
        Find out if this update is the first time files are being added/changed since the dataset's creation.
        """
        return not self.files.exists()

    def change_files(self, file_changes, operation_is_create=False):
        """
        Modify the set of files the dataset consists of: Add, or exclude files from the dataset.

        This method is called both when:
        - dataset is first created
        - when dataset is files are changed using a separate api for that purpse

        This method should be the sole means of adding or removing files from a dataset. One exception kind of
        remains: When a new version is created from a deprecated dataset, then files with "removed=True" status
        are automatically removed from the new version of the dataset, effectively restoring the dataset to a
        "working" condition.

        If the entry includes dataset-specific metadata (also knows as "user metadata"), then that metadata is
        persisted in research_dataset.files or research_dataset.directories objects, while all the "addition entries"
        or "exclusion entries" are just processed, but not persisted in those previously mentioned objects.

        To underline: When this method is called when adding files to a dataset, the same request, and the same
        file or directory entries, can also contain user-provided metadata in it, which will be saved. This is useful,
        so both adding the file and saving the metadata can be done in one request to Metax API.

        When files are excluded, this method also takes care of removing all previously persisted metadata entries
        from research_dataset.files and research_dataset.directories objects.

        The order of processing the received entries is:
        1) directory add and exclude entries in the order the were provided in
        2) individual file add and exclude entries, add-entries first (although here the order does not really matter)

        Parameter file_changes is a dict that looks similar to research_dataset files and directories:
            {
                'files': [
                    { 'identifier': 'abc1' },                       # add file. entry not persisted
                    { 'identifier': 'abc2', 'title': 'something' }, # add file with dataset-specific metadata
                    { 'identifier': 'abc3', 'exclude': true },      # remove file. removes related metadata entry
                                                                    # from research_dataset
                ],
                'directories': [
                    { 'identifier': 'def1' },                 # add all files from dir and its sub dirs
                    { 'identifier': 'def2', 'exclude': true } # exlude all files from dir and its sub dirs. removes
                                                              # all related metadata entries from research_dataset
                ]
            }

        Parameter operation_is_create tells the method whether adding files during dataset initial create.
            Here we can't trust in method _operation_is_create(), since it tries to detect creating phase from
            presence of self.id, which in this case is already there since we are so late in the create process.
        """
        _logger.debug('Changing dataset included files...')
        _logger.debug('Note: operation_is_create=%r' % operation_is_create)

        if self.deprecated:
            raise Http400(
                'Changing files of a deprecated dataset is not permitted. Please create a new dataset version first.'
            )

        assert type(file_changes) is dict
        assert hasattr(self, 'request') and self.request is not None

        if not (file_changes.get('files') or file_changes.get('directories')):
            _logger.debug('Received data does not include files or directories - returning')
            return

        # create an instance of the serializer for later validations
        serializer = self.serializer_class(self)

        if operation_is_create:
            # todo: probably it would be best to leave this timestamp field empty on initial create
            if self.cumulative_state == self.CUMULATIVE_STATE_YES:
                self.date_last_cumulative_addition = self.date_created
        else:

            # operation is update

            _logger.debug('self.state == %s' % self.state)

            if self.state == self.STATE_DRAFT:
                # "normal case". file changes are generally allowed only on draft datasets.
                pass
            else:

                # dataset is published

                if self._files_added_for_first_time():
                    # first update from 0 to n files should be allowed even for a published dataset, and
                    # without needing to create new dataset versions, so this is permitted. subsequent
                    # file changes will require creating a new draft version first.
                    pass
                elif self.cumulative_state == self.CUMULATIVE_STATE_YES:

                    # cumulative datasets permit adding new files, but not excluding. raise error
                    # if user is trying to remove files. while we could just ignore the exlusions,
                    # raising an error and stopping the operation altogether should be the path of
                    # least astonishment

                    for object_type in ('files', 'directories'):
                        for obj in file_changes.get(object_type, []):
                            if obj.get('exclude', False) is True:
                                raise Http400(
                                    'Excluding files from a cumulative dataset is not permitted. '
                                    'Please create a new dataset version first.'
                                )
                else:
                    # published dataset with no special status. changing files is not permitted.
                    raise Http400(
                        'Changing files of a published dataset is not permitted. '
                        'Please create a new dataset version first. '
                        'If you need to continuously add new files to a published dataset, '
                        'consider creating a new cumulative dataset.'
                    )

            # validating received data in this method is only necessary for update operations. for
            # create operations, the data will already have been validated.
            serializer.validate_research_dataset_files(file_changes)

            # for now the only user who can modify the dataset is the owner, so this is ok.
            # at some point it would be best to provide request user in http header.
            self.user_modified = self.metadata_provider_user
            self.date_modified = get_tz_aware_now_without_micros()

            if self.cumulative_state == self.CUMULATIVE_STATE_YES:
                self.date_last_cumulative_addition = self.date_modified

        # for counting changes at the end
        files_before_set = set(id for id in self.files.all().values_list('id', flat=True))

        files_excluded = False

        for dr in file_changes.get('directories', []):

            # process directory add end exclude entries in the order they are provided

            files = self._get_dataset_selected_file_ids(
                dr['identifier'], 'directory', exclude=dr.get('exclude', False))

            if dr.get('exclude', False) is False:
                _logger.debug('Found %d files to add based on received directory objects' % len(files))
                self.files.add(*files)
            else:
                _logger.debug('Found %d files to exclude based on received directory objects' % len(files))
                files_excluded = True
                self.files.remove(*files)

        # process individual file add and exclude entries. order does not matter.

        files_add = [
            f['identifier'] for f in file_changes.get('files', [])
            if f.get('exclude', False) is False
        ]

        files_exclude = [
            f['identifier'] for f in file_changes.get('files', [])
            if f.get('exclude', False) is True
        ]

        file_ids_add = self._get_dataset_selected_file_ids(files_add, 'file', exclude=False)
        file_ids_exclude = self._get_dataset_selected_file_ids(files_exclude, 'file', exclude=True)

        _logger.debug('Found %d files to add based on received file objects' % len(file_ids_add))
        _logger.debug('Found %d files to exclude based on received file objects' % len(file_ids_exclude))

        self.files.add(*file_ids_add)
        self.files.remove(*file_ids_exclude)

        # do final checking that resulting dataset contains files only from a single project
        projects = self.files.all().values_list('project_identifier', flat=True).distinct('project_identifier')
        if len(projects) > 1:
            raise Http400('All added files must be from the same project')

        if file_ids_exclude:
            files_excluded = True

        if files_excluded:
            # directories or individual files were excluded. ensure
            # dataset-specific metadata does not contain entries for files that
            # are no longer part of the dataset.
            self._clear_non_included_file_metadata_entries()

        # when adding new files, the entries can contain dataset-specific metadata
        self._update_dataset_specific_metadata(file_changes, operation_is_create=operation_is_create)

        # update total file size, and numbers for directories that are shown when browsing files
        self._calculate_total_files_byte_size()
        self.calculate_directory_byte_sizes_and_file_counts()

        # after all is said and done, ensure we are still adhering to the schema.
        serializer.validate_json_schema(self.research_dataset)

        # ensure everything is saved, even if some of the methods do a save by themselves
        super(Common, self).save()

        # count effect of performed actions: number of files added and excluded
        file_ids_after = self.files.all().values_list('id', flat=True)

        if operation_is_create:
            files_added_count = len(file_ids_after)
        else:
            files_after_set = set(id for id in file_ids_after)
            files_added_count = len(files_after_set.difference(files_before_set))
            files_excluded_count = len(files_before_set.difference(files_after_set))

        ret = {
            'files_added': files_added_count,
        }

        if not operation_is_create:
            # operation is update. for a create operation, there would never be removed files
            ret['files_removed'] = files_excluded_count

        return ret

    def _get_dataset_selected_file_ids(self, identifier_list, identifier_type, exclude=False):
        """
        Return a list of ids of all unique individual files currently in the db.
        """
        assert identifier_type in ('file', 'directory')

        if type(identifier_list) is not list:
            # this method can accept a single identifier too
            identifier_list = [identifier_list]

        file_ids = []
        file_changes = { 'changed_projects': defaultdict(set) }

        if identifier_type == 'file':
            file_ids = self._get_file_ids_from_file_list(identifier_list, file_changes, exclude)

        elif identifier_type == 'directory':
            file_ids = self._get_file_ids_from_dir_list(identifier_list, file_changes, exclude)

        self._check_changed_files_permissions(file_changes)

        return file_ids

    def _get_file_ids_from_file_list(self, file_identifiers, file_changes, exclude):
        """
        Simply retrieve db ids of files in provided file_identifiers list. Update file_changes dict with
        affected projects for permissions checking.
        """
        if not file_identifiers:
            return []

        if exclude:
            # retrieve files for the purpose excluding files from the dataset. search only from current dataset files
            files = self.files \
                .filter(identifier__in=file_identifiers) \
                .values('id', 'identifier', 'project_identifier')
        else:
            # retrieve files for the purpose of adding new files. search only from files not already part of the dataset
            files = File.objects \
                .filter(identifier__in=file_identifiers) \
                .exclude(id__in=self.files.all()) \
                .values('id', 'identifier', 'project_identifier')

        if len(files) != len(file_identifiers):

            missing_identifiers = [
                pid for pid in file_identifiers if pid not in set([f['identifier'] for f in files])
            ]

            # ensure these were not already included in the dataset

            from_record = self.files.filter(identifier__in=missing_identifiers).values_list('identifier', flat=True)

            if len(missing_identifiers) == len(from_record):
                # all files were already part of the dataset. no files to add
                return []

            # otherwise, some were actually not found

            missing_identifiers = [ f for f in missing_identifiers if f not in from_record ]

            raise Http400({
                'detail': ['Some requested files were not found. File identifiers not found:'],
                'data': missing_identifiers
            })

        file_changes['changed_projects']['files_added'].add(files[0]['project_identifier'])

        return [ f['id'] for f in files ]

    def _get_file_ids_from_dir_list(self, dir_identifiers, file_changes, exclude):
        """
        Retrieve file ids for all files in requested directories and their subdirectories.

        Update file_changes dict with affected projects for later permissions checking.
        """
        if not dir_identifiers:
            return []

        dirs = Directory.objects.filter(identifier__in=dir_identifiers).values('project_identifier', 'directory_path')

        if len(dirs) == 0:
            raise Http400('no directories matched given identifiers')

        elif len(dirs) != len(dir_identifiers):
            missing_identifiers = [
                pid for pid in dir_identifiers if pid not in set([dr['identifier'] for dr in dirs])
            ]

            raise Http400({
                'detail': ['Some requested directories were not found. Directory identifiers not found:'],
                'data': missing_identifiers
            })

        project_identifier = dirs[0]['project_identifier']

        file_filter = Q()

        for dr in dirs:

            if dr['project_identifier'] != project_identifier:
                raise Http400('All added files must be from the same project')

            if dr['directory_path'] == '/':
                file_filter = Q()
                break
            else:
                file_filter |= Q(file_path__startswith='%s/' % dr['directory_path'])

        # results in ((path like x or path like y...) and project = z)
        file_filter &= Q(project_identifier=project_identifier)

        file_changes['changed_projects']['files_added'].add(project_identifier)

        if exclude:
            # retrieve files for the purpose excluding files from the dataset. search only from current dataset files
            file_ids = self.files \
                .filter(file_filter) \
                .values_list('id', flat=True)
        else:
            # retrieve files for the purpose of adding new files. search only from files not already part of the dataset
            file_ids = File.objects \
                .filter(file_filter) \
                .exclude(id__in=self.files.all()) \
                .values_list('id', flat=True)

        return [ id for id in file_ids ]

    def _clear_non_included_file_metadata_entries(self, raise_on_not_found=False):
        """
        Keep research_dataset tidy by removing file and directory metadata entries if file
        or directory has been completely excluded from dataset.

        Parameter raise_on_not_found on can be used to raise an error if any metadata entry is
        not actually an included file in the dataset.
        """
        _logger.debug('Clearing non-included file metadata entries...')
        _logger.debug('Note: raise_on_not_found=%r' % raise_on_not_found)

        # files
        file_identifiers = [ f['identifier'] for f in self.research_dataset.get('files', []) ]

        if file_identifiers:

            included_files = set(
                idf for idf in self.files.filter(identifier__in=file_identifiers).values_list('identifier', flat=True)
            )

            if raise_on_not_found:

                # note: use of this parameter should only be relevant when updating dataset-specific metadata.

                not_included_files = [ idf for idf in file_identifiers if idf not in included_files ]

                if not_included_files:
                    raise Http400({
                        'detail': [
                            'The following files are not included in the dataset. '
                            'Please add them to the dataset first.'
                        ],
                        'data': not_included_files
                    })

            self.research_dataset['files'] = [
                f for f in self.research_dataset['files'] if f['identifier'] in included_files
            ]

        # dirs
        dir_identifiers = [ dr['identifier'] for dr in self.research_dataset.get('directories', []) ]

        if dir_identifiers:

            current_dir_entries = [
                dr for dr
                in Directory.objects.filter(identifier__in=dir_identifiers).values('identifier', 'directory_path')
            ]

            included_dirs = set()

            for dr in current_dir_entries:

                dir_has_files = self.files.filter(file_path__startswith='%s/' % dr['directory_path']).exists()

                if dir_has_files:
                    # directory or one of its sub directories has at least one file. therefore
                    # this directory metadata entry should remain.
                    included_dirs.add(dr['identifier'])

            if raise_on_not_found:

                # note: use of this parameter should only be relevant when updating dataset-specific metadata.

                not_included_dirs = [ idf for idf in dir_identifiers if idf not in included_dirs ]

                if not_included_dirs:
                    raise Http400({
                        'detail': 'The following directories do not contain any files in the dataset. '
                                  'Please add files to the dataset first.',
                        'data': not_included_dirs
                    })

            self.research_dataset['directories'] = [
                dr for dr in self.research_dataset['directories'] if dr['identifier'] in included_dirs
            ]

    def update_files_dataset_specific_metadata(self, md_changes):
        """
        Update contents of fields research_dataset.files and research_dataset.directories, i.e. "user metadata"
        or "dataset-specific metadata".
        """
        _logger.info('Updating dataset file metadata...')

        serializer = self.serializer_class(self)

        # note: this does json schema validation, and its output from the api is not user friendly
        # at all, but its better than nothing...

        if self.request.META['REQUEST_METHOD'] == 'PUT':
            # do not validate for patch, since it can contain only partial fields
            serializer.validate_research_dataset_files(md_changes)

        for object_type in ('files', 'directories'):
            for obj in md_changes.get(object_type, []):
                if 'identifier' not in obj:
                    raise Http400('\'identifier\' is a required field in all metadata entries.')

        self._update_dataset_specific_metadata(md_changes, operation_is_create=False)
        self._clear_non_included_file_metadata_entries(raise_on_not_found=True)

        # ensure we didn't break anything, and validate the saved data

        files_and_dirs = {}

        for object_type in ('files', 'directories'):
            if object_type in self.research_dataset:
                files_and_dirs[object_type] = self.research_dataset[object_type]

        serializer.validate_research_dataset_files(files_and_dirs)

        self.user_modified = self.metadata_provider_user
        self.date_modified = get_tz_aware_now_without_micros()

        super(Common, self).save()

    def create_new_version(self):
        """
        A method to "explicitly" create a new version of a dataset, which is called from a particular
        RPC API endpoint.
        """
        _logger.info('Creating new dataset version...')

        if not self.catalog_versions_datasets():
            raise Http400('Data catalog does not allow dataset versioning')

        self._new_version = self._create_new_dataset_version_template()
        self._create_new_dataset_version()

    def _create_new_dataset_version(self):
        """
        Create a new dataset version of the record who calls this method.
        """
        assert hasattr(self, '_new_version'), 'self._new_version should have been set in a previous step'

        old_version = self

        if old_version.next_dataset_version_id:
            raise Http400(
                'Dataset already has a next version: %s' % old_version.next_dataset_version.identifier
            )

        _logger.info('Creating new dataset version from old CatalogRecord: %s' % old_version.identifier)

        new_version = self._new_version
        new_version.state = self.STATE_DRAFT
        new_version.deprecated = False
        new_version.date_deprecated = None
        new_version.contract = None
        new_version.date_created = get_tz_aware_now_without_micros()
        new_version.date_modified = None
        new_version.preservation_description = None
        new_version.preservation_state = 0
        new_version.preservation_state_modified = None
        new_version.previous_dataset_version = old_version
        new_version.preservation_identifier = None
        new_version.next_dataset_version_id = None
        new_version.alternate_record_set = None
        new_version.date_removed = None
        new_version.date_cumulation_started = None
        new_version.date_cumulation_ended = None
        new_version.cumulative_state = self.CUMULATIVE_STATE_NO
        new_version.user_modified = None
        new_version.service_modified = None

        if self.request.user.is_service:
            new_version.service_created = self.request.user.username
            new_version.user_created = old_version.metadata_provider_user
        else:
            new_version.service_created = None
            new_version.user_created = self.request.user.username

        new_version.research_dataset = deepcopy(old_version.research_dataset)
        new_version.research_dataset['metadata_version_identifier'] = generate_uuid_identifier()

        # temporary "pid" until draft is published
        new_version.research_dataset['preferred_identifier'] = 'draft:%s' % self.identifier

        if old_version.files.exists():
            # copy all files from previous version to new version.
            # note: discards removed files in the process, since the default manager for files
            # already has filter "removed=False"
            new_version.files.add(*old_version.files.all())

            if old_version.deprecated:
                new_version._clear_non_included_file_metadata_entries()
                new_version._calculate_total_files_byte_size()
                new_version.calculate_directory_byte_sizes_and_file_counts()
            else:
                new_version._directory_data = deepcopy(old_version._directory_data)

        old_version.dataset_version_set.records.add(new_version)
        old_version.next_dataset_version = new_version

        if new_version.editor:
            # some of the old editor fields cant be true in the new version, so keep
            # only the ones that make sense. it is up to the editor, to update other fields
            # they see as relevant. we also dont want null values in there
            old_editor = deepcopy(new_version.editor)
            new_version.editor = {}
            if 'owner_id' in old_editor:
                new_version.editor['owner_id'] = old_editor['owner_id']
            if 'creator_id' in old_editor:
                new_version.editor['creator_id'] = old_editor['creator_id']
            if 'identifier' in old_editor:
                # todo this probably does not make sense... ?
                new_version.editor['identifier'] = old_editor['identifier']

        super(Common, new_version).save()
        super(Common, old_version).save()

        _logger.info('New dataset version draft created, identifier %s' % new_version.identifier)

        log_args = {
            'catalogrecord': {
                'identifier': new_version.identifier,
                'preferred_identifier': new_version.preferred_identifier,
                'data_catalog': new_version.data_catalog.catalog_json['identifier'],
                'date_created': datetime_to_str(new_version.date_created),
                'metadata_owner_org': new_version.metadata_owner_org,
                'state': new_version.state,
            },
            'user_id': new_version.user_created or new_version.service_created,
        }

        log_args['event'] = 'dataset_version_created'
        log_args['catalogrecord']['previous_version_preferred_identifier'] \
            = new_version.previous_dataset_version.preferred_identifier

        self.add_post_request_callable(DelayedLog(**log_args))

    def calculate_directory_byte_sizes_and_file_counts(self):
        """
        Calculate directory byte_sizes and file_counts for all dirs selected for this cr.
        """
        if not self.files.exists():
            return

        _logger.info('Calculating directory byte_sizes and file_counts...')

        parent_dir = self.files.first().parent_directory

        while parent_dir.parent_directory is not None:
            parent_dir = parent_dir.parent_directory

        root_dir = parent_dir

        directory_data = {}

        root_dir.calculate_byte_size_and_file_count_for_cr(self.id, directory_data)

        self._directory_data = directory_data
        super(Common, self).save(update_fields=['_directory_data'])
