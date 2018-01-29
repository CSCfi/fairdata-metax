from collections import defaultdict
from copy import deepcopy
import logging

from django.conf import settings
from django.contrib.postgres.fields import JSONField, ArrayField
from django.db import connection, models
from django.db.models import Q, Sum
from django.http import Http404
from rest_framework.serializers import ValidationError

from metax_api.utils import get_tz_aware_now_without_micros
from metax_api.utils.utils import generate_identifier
from .common import Common, CommonManager
from .contract import Contract
from .data_catalog import DataCatalog
from .directory import Directory
from .file import File

DEBUG = settings.DEBUG
_logger = logging.getLogger(__name__)


class AlternateRecordSet(models.Model):

    """
    A table which contains records that share a common preferred_identifier,
    but belong to different data catalogs.

    Note! Does not inherit from model Common, so does not have timestmap fields,
    and a delete is an actual delete.
    """

    id = models.BigAutoField(primary_key=True, editable=False)

    def print_records(self): # pragma: no cover
        for r in self.records.all():
            print(r.__repr__())


class VersionSet(models.Model):

    """
    A table which contains records that are versions of each other.

    Note! Does not inherit from model Common, so does not have timestmap fields,
    and a delete is an actual delete.
    """

    id = models.BigAutoField(primary_key=True, editable=False)

    def print_records(self): # pragma: no cover
        for r in self.records.all():
            print(r.__repr__())


class CatalogRecordManager(CommonManager):

    def get(self, *args, **kwargs):
        if kwargs.get('using_dict', None):
            # for a simple "just get me the instance that equals this dict i have" search.
            # preferred_identifier is not a valid search key, since it wouldnt necessarily
            # work during an update (if preferred_identifier is being updated).

            # this is useful if during a request the url does not contain the identifier (bulk update),
            # and in generic operations where the type of object being handled is not known (also bulk operations).
            row = kwargs.pop('using_dict')
            if row.get('id', None):
                kwargs['id'] = row['id']
            elif row.get('research_dataset', None) and row['research_dataset'].get('urn_identifier', None):
                kwargs['research_dataset__contains'] = { 'urn_identifier': row['research_dataset']['urn_identifier'] }
            else:
                raise ValidationError(
                    'this operation requires an identifying key to be present: '
                    'id, or research_dataset ->> urn_identifier')
        return super(CatalogRecordManager, self).get(*args, **kwargs)

    def get_id(self, urn_identifier=None):
        """
        Takes urn_identifier, and returns the plain pk of the record. Useful for debugging,
        and some other situations.
        """
        if not urn_identifier:
            raise ValidationError('urn_identifier is a required keyword argument')
        cr = super(CatalogRecordManager, self).filter(
            **{ 'research_dataset__contains': {'urn_identifier': urn_identifier } }
        ).values('id').first()
        if not cr:
            raise Http404
        return cr['id']


class CatalogRecord(Common):

    PRESERVATION_STATE_NOT_IN_PAS = 0
    PRESERVATION_STATE_PROPOSED_MIDTERM = 1
    PRESERVATION_STATE_PROPOSED_LONGTERM = 2
    PRESERVATION_STATE_IN_PACKAGING_SERVICE = 3
    PRESERVATION_STATE_IN_DISSEMINATION = 4
    PRESERVATION_STATE_IN_MIDTERM_PAS = 5
    PRESERVATION_STATE_IN_LONGTERM_PAS = 6
    PRESERVATION_STATE_LONGTERM_PAS_REJECTED = 7
    PRESERVATION_STATE_MIDTERM_PAS_REJECTED = 8

    PRESERVATION_STATE_CHOICES = (
        (PRESERVATION_STATE_NOT_IN_PAS, 'Not in PAS'),
        (PRESERVATION_STATE_PROPOSED_MIDTERM, 'Proposed for midterm'),
        (PRESERVATION_STATE_PROPOSED_LONGTERM, 'Proposed for longterm'),
        (PRESERVATION_STATE_IN_PACKAGING_SERVICE, 'In packaging service'),
        (PRESERVATION_STATE_IN_DISSEMINATION, 'In dissemination'),
        (PRESERVATION_STATE_IN_MIDTERM_PAS, 'In midterm PAS'),
        (PRESERVATION_STATE_IN_LONGTERM_PAS, 'In longterm PAS'),
        (PRESERVATION_STATE_LONGTERM_PAS_REJECTED, 'Longterm PAS rejected'),
        (PRESERVATION_STATE_MIDTERM_PAS_REJECTED, 'Midterm PAS rejected'),
    )

    # MODEL FIELD DEFINITIONS #

    alternate_record_set = models.ForeignKey(
        AlternateRecordSet, on_delete=models.SET_NULL, null=True, related_name='records',
        help_text='Records which are duplicates of this record, but in another catalog.')

    contract = models.ForeignKey(Contract, null=True, on_delete=models.DO_NOTHING, related_name='records')

    data_catalog = models.ForeignKey(DataCatalog, on_delete=models.DO_NOTHING, related_name='records')

    dataset_group_edit = models.CharField(
        max_length=200, blank=True, null=True,
        help_text='Group which is allowed to edit the dataset in this catalog record.')

    deprecated = models.BooleanField(
        default=False, help_text='Is True when files attached to a dataset have been deleted in IDA.')

    files = models.ManyToManyField(File)

    mets_object_identifier = ArrayField(models.CharField(max_length=200), null=True)

    editor = JSONField(null=True, help_text='Editor specific fields, such as owner_id, modified, record_identifier')

    preservation_description = models.CharField(
        max_length=200, blank=True, null=True, help_text='Reason for accepting or rejecting PAS proposal.')

    preservation_reason_description = models.CharField(
        max_length=200, blank=True, null=True, help_text='Reason for PAS proposal from the user.')

    preservation_state = models.IntegerField(
        choices=PRESERVATION_STATE_CHOICES, default=PRESERVATION_STATE_NOT_IN_PAS, help_text='Record state in PAS.')

    preservation_state_modified = models.DateTimeField(null=True, help_text='Date of last preservation state change.')

    research_dataset = JSONField()

    next_version = models.OneToOneField('self', on_delete=models.DO_NOTHING, null=True,
        related_name='+')

    previous_version = models.OneToOneField('self', on_delete=models.DO_NOTHING, null=True,
        related_name='+')

    version_set = models.ForeignKey(
        VersionSet, on_delete=models.SET_NULL, null=True, related_name='records',
        help_text='Records which are versions of each other.')

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
    Used to signal to the serializer (or other interested parties), that the object
    being serialized had a new version created in the current request save operation.
    The serializer then places a 'publish in rabbitmq' key in the dict-representation,
    so that the view knows to publish it just before returning. The publishing needs to be done
    as late as possible (= not here in the model right after the new version object
    was created), because if the request is interrupted for whatever reason after publishing,
    the new version will not get created after all, but the publish message already left.
    """
    next_version_created_in_current_request = False

    objects = CatalogRecordManager()

    class Meta:
        ordering = ['id']

    def __init__(self, *args, **kwargs):
        super(CatalogRecord, self).__init__(*args, **kwargs)
        self.track_fields(
            'preservation_state',
            'research_dataset',
            'research_dataset.files',
            'research_dataset.directories',
            'research_dataset.total_ida_byte_size',
            'research_dataset.total_remote_resources_byte_size',
            'research_dataset.urn_identifier',
            'research_dataset.preferred_identifier',
        )

    def print_files(self): # pragma: no cover
        for f in self.files.all():
            print(f)

    def save(self, *args, **kwargs):
        if self._operation_is_create():
            self._pre_create_operations()
            super(CatalogRecord, self).save(*args, **kwargs)
            self._post_create_operations()
            _logger.info('Created a new <CatalogRecord id: %d, urn_identifier: %s, preferred_identifier: %s >'
                % (self.id, self.urn_identifier, self.preferred_identifier))
        else:
            self._pre_update_operations()
            super(CatalogRecord, self).save(*args, **kwargs)

    def save_as_new_version(self):
        """
        Note: This method is executed by the new version - never by the old one.
        """
        self._pre_create_operations()

        file_changes = self._find_file_changes()

        self._generate_urn_identifier()

        # note: save() updates self._initial_data with data received from the request, so after this
        # point checking changed fields need to be compared with self.previous_version, instead of
        # self._initial_data, which is what field_changed() would do.
        super(CatalogRecord, self).save()

        # files can be processed only after creating the new version in the db, because
        # the new recorde's id is needed for relations.
        actual_files_changed = self._process_file_changes(file_changes)

        if actual_files_changed and self.preferred_identifier == self.previous_version.preferred_identifier:
            _logger.debug(
                'Files changed during version change, but preferred_identifier not updated: '
                'Forcing preferred_identifier change'
            )
            # force preferred_identifier change if not already changed by the user
            self.research_dataset['preferred_identifier'] = self.urn_identifier

        # note: this new version was implicitly placed in the same
        # alternate_record_set as the previous version.

        if self.preferred_identifier != self.previous_version.preferred_identifier:
            # in case the previous version had an alternate_record_set,
            # the prev version will keep staying there, since preferred_identifier
            # was changed for the new version.
            self._handle_preferred_identifier_changed()
        else:
            if self.has_alternate_records():
                # remove previous previous version from alternate_record_set.
                # alternate_record_set should always have the newest versions
                # of records holding a specific preferred_identifier.
                self.previous_version.alternate_record_set = None

        # save file size calculations, changed preferred_identifier, and any other fields
        super(CatalogRecord, self).save()

    def _process_file_changes(self, file_changes):
        """
        Process any changes in files between versions.

        Copy files from the previous version to the new version. Omit any files from the copy according
        to files_to_remove and dirs_to_remove_by_project, and separately add any new files according to
        files_to_add and dirs_to_add_by_project.

        Returns a boolean actual_files_changed
        """
        actual_files_changed = False

        files_to_add, files_to_remove, dirs_to_add_by_project, dirs_to_remove_by_project = file_changes

        if files_to_add or files_to_remove or dirs_to_add_by_project or dirs_to_remove_by_project:

            if DEBUG:
                _logger.debug('Detected the following file changes:')

            sql_copy_files_from_prev_version = '''
                insert into metax_api_catalogrecord_files (catalogrecord_id, file_id)
                select %s as catalogrecord_id, file_id
                from metax_api_catalogrecord_files as cr_f
                inner join metax_api_file as f on f.id = cr_f.file_id
                where catalogrecord_id = %s
            '''
            sql_params_copy = [self.id, self.previous_version.id]

            if files_to_remove:
                # in the above sql, dont copy files which were considered to have unique
                # paths, and were concluded to be removed based on them being misisng from
                # research_dataset.files, and not included by other dirs.
                sql_copy_files_from_prev_version += '''
                    and file_id not in %s'''
                sql_params_copy.append(tuple(files_to_remove))

                if DEBUG:
                    _logger.debug('File ids to remove: %s' % files_to_remove)

            if dirs_to_remove_by_project:
                # in the above sql, dont copy files which should be removed due to directories
                # being removed from research_dataset.directories, and not included by other dirs.
                for project, dir_paths in dirs_to_remove_by_project.items():
                    for dir_path in dir_paths:
                        sql_copy_files_from_prev_version += '''
                            and not (f.project_identifier = %s and f.file_path like (%s || '/%%'))'''
                        sql_params_copy.extend([project, dir_path])

                if DEBUG:
                    _logger.debug('Directory paths to remove, by project:')
                    for project, dir_paths in dirs_to_remove_by_project.items():
                        _logger.debug('\tProject: %s' % project)
                        for dir_path in dir_paths:
                            _logger.debug('\t\t%s' % dir_path)

            if dirs_to_add_by_project:
                # sql to add files by directory path that were not previously included.
                # also takes care of "path is already included by another dir, but i want to check if there
                # are new files to add in there"
                sql_select_and_insert_files_by_dir_path = '''
                    insert into metax_api_catalogrecord_files (catalogrecord_id, file_id)
                    select %s as catalogrecord_id, f.id
                    from metax_api_file as f
                    where f.active = true and f.removed = false
                    and f.id not in (
                        select file_id from
                        metax_api_catalogrecord_files cr_f
                        where catalogrecord_id = %s
                    )
                '''
                sql_params_insert_dirs = [self.id, self.id]

                for project, dir_paths in dirs_to_add_by_project.items():
                    for dir_path in dir_paths:
                        sql_select_and_insert_files_by_dir_path += '''
                            and (f.project_identifier = %s and f.file_path like (%s || '/%%'))'''
                        sql_params_insert_dirs.extend([project, dir_path])

                if DEBUG:
                    _logger.debug('Directory paths to add, by project:')
                    for project, dir_paths in dirs_to_add_by_project.items():
                        _logger.debug('\tProject: %s' % project)
                        for dir_path in dir_paths:
                            _logger.debug('\t\t%s' % dir_path)

            if files_to_add:
                # sql to add any singular files which were not covered by any directory path
                # being added. also takes care of "path is already included by another dir,
                # but this file did not necessarily exist yet at that time, so add it in case
                # its a new file"
                sql_insert_single_files = '''
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
                '''
                sql_params_insert_single = [self.id, tuple(files_to_add), self.id]

                if DEBUG:
                    _logger.debug('File ids to add: %s' % files_to_add)

            sql_detect_files_changed = '''
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
            '''
            sql_params_files_changed = [self.id, self.previous_version_id, self.previous_version_id, self.id]

            with connection.cursor() as cr:
                cr.execute(sql_copy_files_from_prev_version, sql_params_copy)

                if dirs_to_add_by_project:
                    cr.execute(sql_select_and_insert_files_by_dir_path, sql_params_insert_dirs)

                if files_to_add:
                    cr.execute(sql_insert_single_files, sql_params_insert_single)

                cr.execute(sql_detect_files_changed, sql_params_files_changed)

                # fetchall() returns i.e. [(False, ), (True, )] or just [(False, )]
                actual_files_changed = any(v[0] for v in cr.fetchall())

            self._calculate_total_ida_byte_size()

            if DEBUG:
                _logger.debug('Actual files changed during version change: %s' % str(actual_files_changed))
        else:
            # no files to specifically add or remove - just copy all from previous version

            _logger.debug('No files changes detected between versions - copying files from previous version')

            sql_copy_files_from_prev_version = '''
                insert into metax_api_catalogrecord_files (catalogrecord_id, file_id)
                select %s as catalogrecord_id, file_id
                from metax_api_catalogrecord_files
                where catalogrecord_id = %s
            '''
            with connection.cursor() as cr:
                cr.execute(sql_copy_files_from_prev_version, [self.id, self.previous_version.id])

            self.research_dataset['total_ida_byte_size'] = \
                self.previous_version.research_dataset['total_ida_byte_size']

        return actual_files_changed

    def _find_file_changes(self):
        """
        Find new additions and removed entries in research_metadata.files and directories, and return
        - lists of files to add and remove
        - lists of dirs to add and remove, grouped by project
        """
        file_description_changes = self._get_selected_files_changed()

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

        self._find_new_dirs_to_add(file_description_changes, dirs_to_add_by_project, dirs_to_remove_by_project)
        self._find_new_files_to_add(file_description_changes, files_to_add, files_to_remove)

        return files_to_add, files_to_remove, dirs_to_add_by_project, dirs_to_remove_by_project

    def _find_new_dirs_to_add(self, file_description_changes, dirs_to_add_by_project, dirs_to_remove_by_project):
        """
        Based on changes in research_metadata.directories (parameter file_description_changes), find out which
        directories should be left out when copying files from the previous version to the new version,
        and which new directories should be added.
        """
        if 'directories' not in file_description_changes:
            return

        dir_identifiers = list(file_description_changes['directories']['removed']) + \
            list(file_description_changes['directories']['added'])

        dir_details = Directory.objects.filter(identifier__in=dir_identifiers) \
            .values('project_identifier', 'identifier', 'directory_path')

        for dr in dir_details:
            if dr['identifier'] in file_description_changes['directories']['added']:
                # include all new dirs found, to add files from an entirely new directory,
                # or to search an already existing directory for new files (and sub dirs).
                # as a result the actual set of files may or may not change.
                dirs_to_add_by_project[dr['project_identifier']].append(dr['directory_path'])
            elif dr['identifier'] in file_description_changes['directories']['removed']:
                if not self._path_included_in_previous_version(dr['project_identifier'], dr['directory_path']):
                    # only remove dirs that are not included by other directory paths.
                    # as a result the actual set of files may change, if the path is not included
                    # in the new directory additions
                    dirs_to_remove_by_project[dr['project_identifier']].append(dr['directory_path'])

        if len(dir_identifiers) != len(dir_details):
            existig_dirs = set( d['identifier'] for d in dir_details )
            missing_identifiers = [ d for d in dir_identifiers if d not in existig_dirs ]
            raise ValidationError({'detail': ['the following directory identifiers were not found:\n%s'
                % '\n'.join(missing_identifiers) ]})

    def _find_new_files_to_add(self, file_description_changes, files_to_add, files_to_remove):
        """
        Based on changes in research_metadata.files (parameter file_description_changes), find out which
        files should be left out when copying files from the previous version to the new version,
        and which new files should be added.
        """
        if 'files' not in file_description_changes:
            return

        file_identifiers = list(file_description_changes['files']['removed']) + \
            list(file_description_changes['files']['added'])

        file_details = File.objects.filter(identifier__in=file_identifiers) \
            .values('id', 'project_identifier', 'identifier', 'file_path')

        for f in file_details:
            if f['identifier'] in file_description_changes['files']['added']:
                # include all new files even if it already included by another directory's path,
                # to check later that it is not a file that was created later.
                # as a result the actual set of files may or may not change.
                files_to_add.append(f['id'])
            elif f['identifier'] in file_description_changes['files']['removed']:
                if not self._path_included_in_previous_version(f['project_identifier'], f['file_path']):
                    # file is being removed.
                    # path is not included by other directories in the previous version,
                    # which means the actual set of files may change, if the path is not included
                    # in the new directory additions
                    files_to_remove.append(f['id'])

        if len(file_identifiers) != len(file_details):
            existig_files = set( f['identifier'] for f in file_details )
            missing_identifiers = [ f for f in file_identifiers if f not in existig_files ]
            raise ValidationError({'detail': ['the following file identifiers were not found:\n%s'
                % '\n'.join(missing_identifiers) ]})

    def _path_included_in_previous_version(self, project, path):
        """
        Check if a path in a specific project is already included in the path of
        another directory included in the PREVIOUS VERSION dataset selected directories.
        """
        if not hasattr(self, '_previous_highest_level_dirs_by_project'):
            if 'directories' not in self.previous_version.research_dataset:
                return False
            dir_identifiers = [ d['identifier'] for d in self.previous_version.research_dataset['directories'] ]
            self._previous_highest_level_dirs_by_project = self._get_top_level_parent_dirs_by_project(dir_identifiers)
        return any(
            True for dr_path in self._previous_highest_level_dirs_by_project[project]
            if path != dr_path and path.startswith(dr_path)
        )

    def delete(self, *args, **kwargs):
        if self.has_alternate_records():
            self._remove_from_alternate_record_set()
        super(CatalogRecord, self).delete(*args, **kwargs)

    def can_be_proposed_to_pas(self):
        return self.preservation_state in (
            CatalogRecord.PRESERVATION_STATE_NOT_IN_PAS,
            CatalogRecord.PRESERVATION_STATE_LONGTERM_PAS_REJECTED,
            CatalogRecord.PRESERVATION_STATE_MIDTERM_PAS_REJECTED)

    @property
    def preferred_identifier(self):
        try:
            return self.research_dataset['preferred_identifier']
        except:
            return None

    @property
    def urn_identifier(self):
        try:
            return self.research_dataset['urn_identifier']
        except:
            return None

    def catalog_versions_datasets(self):
        return self.data_catalog.catalog_json.get('dataset_versioning', False) is True

    def has_alternate_records(self):
        return bool(self.alternate_record_set)

    def has_versions(self):
        return bool(self.version_set)

    def _pre_create_operations(self):
        self._generate_urn_identifier()
        if 'remote_resources' in self.research_dataset:
            self._calculate_total_remote_resources_byte_size()

    def _post_create_operations(self):
        if 'files' in self.research_dataset or 'directories' in self.research_dataset:
            # files must be added after the record itself has been created, to be able
            # to insert into a many2many relation.
            self.files.add(*self._get_dataset_selected_file_ids())
            self._calculate_total_ida_byte_size()
            super(CatalogRecord, self).save() # save byte size calculation

        other_record = self._check_alternate_records()
        if other_record:
            self._create_or_update_alternate_record_set(other_record)

    def _pre_update_operations(self):
        if self.field_changed('research_dataset.urn_identifier'):
            # read-only
            self.research_dataset['urn_identifier'] = self._initial_data['research_dataset']['urn_identifier']

        if self.field_changed('research_dataset.total_ida_byte_size'):
            # read-only
            if 'total_ida_byte_size' in self._initial_data['research_dataset']:
                self.research_dataset['total_ida_byte_size'] = \
                    self._initial_data['research_dataset']['total_ida_byte_size']
            else:
                self.research_dataset.pop('total_ida_byte_size')

        if self.field_changed('research_dataset.total_remote_resources_byte_size'):
            # read-only
            if 'total_remote_resources_byte_size' in self._initial_data['research_dataset']:
                self.research_dataset['total_remote_resources_byte_size'] = \
                    self._initial_data['research_dataset']['total_remote_resources_byte_size']
            else:
                self.research_dataset.pop('total_remote_resources_byte_size')

        if self.field_changed('preservation_state'):
            self.preservation_state_modified = get_tz_aware_now_without_micros()

        if self.catalog_versions_datasets() and not self.preserve_version:
            if self.field_changed('research_dataset'):
                if not self.next_version_id:
                    self._create_new_version()
                else:
                    raise ValidationError({ 'detail': [
                        'modifying dataset metadata of old versions not permitted'
                    ]})
            else:
                # edits to any other field than research_dataset are OK even in
                # old versions of a CR.
                pass
        else:
            # non-versioning catalogs, such as harvesters, or if an update
            # was forced to occur without version update.

            if self.preserve_version and self._get_selected_files_changed():
                raise ValidationError({ 'detail': [
                    'currently trying to preserve version while making changes which may result in files '
                    'being changed is not supported.'
                ]})

            if self.field_changed('research_dataset.preferred_identifier'):
                self._handle_preferred_identifier_changed()

    def _calculate_total_ida_byte_size(self):
        rd = self.research_dataset
        if 'files' in rd or 'directories' in rd:
            rd['total_ida_byte_size'] = self.files.aggregate(Sum('byte_size'))['byte_size__sum']
        else:
            rd['total_ida_byte_size'] = 0

    def _calculate_total_remote_resources_byte_size(self):
        rd = self.research_dataset
        if 'remote_resources' in rd:
            rd['total_remote_resources_byte_size'] = sum(
                rr['byte_size'] for rr in rd['remote_resources'] if 'byte_size' in rr
            )
        else:
            rd['total_remote_resources_byte_size'] = 0

    def _get_dataset_selected_file_ids(self):
        """
        Parse research_dataset.files and directories, and return a list of ids
        of all unique individual files currently in the db.
        """
        file_ids = []

        if 'files' in self.research_dataset:
            file_ids.extend(self._get_file_ids_from_file_list(self.research_dataset['files']))

        if 'directories' in self.research_dataset:
            file_ids.extend(self._get_file_ids_from_dir_list(self.research_dataset['directories'],
                ignore_files=file_ids))

        # note: possibly might wanto to use set() to remove duplicates instead of using ignore_files=list
        # to ignore already included files in _get_file_ids_from_dir_list()
        return file_ids

    def _get_file_ids_from_file_list(self, file_list):
        """
        Simply retrieve db ids of files in research_dataset.files
        """
        if not file_list:
            return []

        file_pids = [ f['identifier'] for f in file_list ]

        files = File.objects.filter(identifier__in=file_pids).values('id', 'identifier')
        file_ids = [ f['id'] for f in files ]

        if len(file_ids) != len(file_pids):
            missing_identifiers = [ pid for pid in file_pids if pid not in set(f['identifier'] for f in files)]
            raise ValidationError({ 'detail': [
                'some requested files were not found. file identifiers not found:\n%s'
                % '\n'.join(missing_identifiers)
            ]})

        return file_ids

    def _get_file_ids_from_dir_list(self, dirs_list, ignore_files=[]):
        """
        The field research_dataset.directories can contain directories from multiple
        projects. Find the top-most dirs in each project used, and put their files -
        excluding those files already added from the field research_dataset.files - into
        the db relation dataset_files.
        """
        if not dirs_list:
            return []

        dir_identifiers = [ d['identifier'] for d in dirs_list ]

        highest_level_dirs_by_project = self._get_top_level_parent_dirs_by_project(dir_identifiers)

        # empty queryset
        files = File.objects.none()

        for project_identifier, dir_paths in highest_level_dirs_by_project.items():
            for dir_path in dir_paths:
                files = files | File.objects.filter(
                    project_identifier=project_identifier,
                    file_path__startswith='%s/' % dir_path
                )

        return files.exclude(id__in=ignore_files).values_list('id', flat=True)

    def _get_top_level_parent_dirs_by_project(self, dir_identifiers):
        """
        Find top level dirs that are not included by other directories in
        dir_identifiers, and return them in a dict, grouped by project.

        These directories are useful to know for finding files to add and remove by
        a file's file_path.
        """
        if not dir_identifiers:
            return {}

        dirs = Directory.objects.filter(identifier__in=dir_identifiers) \
            .values('project_identifier', 'directory_path', 'identifier') \
            .order_by('project_identifier', 'directory_path')

        if len(dirs) != len(dir_identifiers):
            missing_identifiers = [ pid for pid in dir_identifiers if pid not in set(d['identifier'] for d in dirs)]
            raise ValidationError({ 'detail': [
                'some requested directories were not found. directory identifiers not found:\n%s'
                % '\n'.join(missing_identifiers)
            ]})

        # group directory paths by project
        dirs_by_project = defaultdict(list)

        for dr in dirs:
            dirs_by_project[dr['project_identifier']].append(dr['directory_path'])

        top_level_dirs_by_project = defaultdict(list)

        for proj, dir_paths in dirs_by_project.items():
            for path in dir_paths:
                path_contained_by_other_paths = ( p.startswith(path) for p in dir_paths if p != path )

                if all(path_contained_by_other_paths):
                    # a 'root' level directory, every other directory is inside this dir
                    top_level_dirs_by_project[proj].append(path)
                    break
                elif any(path_contained_by_other_paths):
                    # a child of at least one other path. no need to include it in the list
                    pass
                else:
                    # "unique", not a child of any other path
                    top_level_dirs_by_project[proj].append(path)

        return top_level_dirs_by_project

    def _create_new_version(self):
        """
        Create a new version of the record who calls this method.

        - Creates a new version_set if required
        - Sets links next_version and previous_version to related objects
        - Forces preferred_identifier change if necessary
        """
        _logger.info('Creating new version from CatalogRecord %s...' % self.urn_identifier)
        new_version = CatalogRecord.objects.get(pk=self.id)
        new_version.id = None # setting id to None creates a new row
        new_version.contract = None
        new_version.date_created = self.date_modified
        new_version.date_modified = self.date_modified
        new_version.next_version = None
        new_version.user_created = self.user_modified
        new_version.user_modified = self.user_modified
        new_version.preservation_description = None
        new_version.preservation_state = 0
        new_version.preservation_state_modified = None
        new_version.previous_version = self
        # note: copying research_dataset from the currently open instance 'self',
        # contains the new field data from the request. this effectively transfers
        # the changes to the new
        new_version.research_dataset = deepcopy(self.research_dataset)
        new_version.research_dataset.pop('urn_identifier')
        new_version.service_created = self.service_modified or self.service_created
        new_version.service_modified = None

        # nothing must change in the now old version of research_dataset, so copy
        # from _initial_data so that super().save() does not change it later.
        self.research_dataset = deepcopy(self._initial_data['research_dataset'])

        # some of the old editor fields cant be true in the new version, so keep
        # only the ones that make sense. it is up to the editor, to update other fields
        # they see as relevant. we also dont want null values in there
        old_editor = deepcopy(new_version.editor)
        new_version.editor = {}
        if 'owner_id' in old_editor:
            new_version.editor['owner_id'] = old_editor['owner_id']
        if 'owner_id' in old_editor:
            new_version.editor['creator_id'] = old_editor['owner_id']
        if 'identifier' in old_editor:
            new_version.editor['identifier'] = old_editor['identifier']

        new_version.save_as_new_version()

        if self.has_versions():
            # if a version_set existed, the new version inherited it from the previous version.
            pass
        else:
            vs = VersionSet()
            vs.save()
            vs.records.add(self, new_version)
            vs.save()

        self.next_version = new_version
        self.next_version_created_in_current_request = True

        _logger.info('New CatalogRecord version %s created' % new_version.urn_identifier)

    def _get_selected_files_changed(self):
        """
        Check if files or directories selected in research_dataset have changed.

        Since there are objects and not simple values, easier to do a custom check
        because the target of checking is a specific field in the object. If a need
        arises, more magic can be applied to track_fields() to be more generic.

        Note: set removes duplicates. It is assumed that file listings do not include
        duplicate files.
        """
        if not self._field_is_loaded('research_dataset'):
            return {}

        if not self._field_initial_value_loaded('research_dataset'): # pragma: no cover
            self._raise_field_not_tracked_error('research_dataset.files')

        changes = {}

        initial_files = set( f['identifier'] for f in self._initial_data['research_dataset'].get('files', []) )
        received_files = set( f['identifier'] for f in self.research_dataset.get('files', []) )
        if initial_files != received_files:
            changes['files'] = {
                'removed': initial_files.difference(received_files),
                'added': received_files.difference(initial_files),
            }

        initial_dirs = set(dr['identifier'] for dr in self._initial_data['research_dataset'].get('directories', []))
        received_dirs = set( dr['identifier'] for dr in self.research_dataset.get('directories', []) )
        if initial_dirs != received_dirs:
            changes['directories'] = {
                'removed': initial_dirs.difference(received_dirs),
                'added': received_dirs.difference(initial_dirs),
            }

        return changes

    def _generate_urn_identifier(self):
        """
        Field urn_identifier in research_dataset is always generated, and it can not be changed later.
        If preferred_identifier is missing during create, copy urn_identifier to it also.
        """
        urn_identifier = generate_identifier()
        self.research_dataset['urn_identifier'] = urn_identifier
        if not self.research_dataset.get('preferred_identifier', None):
            self.research_dataset['preferred_identifier'] = urn_identifier

    def _handle_preferred_identifier_changed(self):
        if self.has_alternate_records():
            self._remove_from_alternate_record_set()

        other_record = self._check_alternate_records()

        if other_record:
            self._create_or_update_alternate_record_set(other_record)

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
        return CatalogRecord.objects.select_related('data_catalog', 'alternate_record_set') \
            .filter(research_dataset__contains={ 'preferred_identifier': self.preferred_identifier }) \
            .exclude(Q(data_catalog__id=self.data_catalog_id) | Q(id=self.id)) \
            .first()

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
            _logger.info('Creating new alternate_record_set for preferred_identifier: %s, with records: %s and %s' %
                (self.preferred_identifier, self.urn_identifier, other_record.urn_identifier))

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

    def __repr__(self):
        return '<%s: %d, removed: %s, data_catalog: %s, urn_identifier: %s, preferred_identifier: %s, file_count: %d >'\
            % (
                'CatalogRecord',
                self.id,
                str(self.removed),
                self.data_catalog.catalog_json['identifier'],
                self.urn_identifier,
                self.preferred_identifier,
                self.files.count(),
            )
