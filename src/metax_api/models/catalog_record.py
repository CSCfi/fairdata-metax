from collections import defaultdict
from copy import deepcopy
import logging

from django.conf import settings
from django.contrib.postgres.fields import JSONField, ArrayField
from django.db import connection, models, transaction
from django.db.models import Q, Sum
from django.http import Http404
from rest_framework.serializers import ValidationError

from metax_api.exceptions import Http400
from metax_api.utils import get_tz_aware_now_without_micros
from metax_api.utils.utils import generate_identifier
from .common import Common, CommonManager
from .contract import Contract
from .data_catalog import DataCatalog
from .directory import Directory
from .file import File

DEBUG = settings.DEBUG
_logger = logging.getLogger(__name__)


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

    def print_records(self): # pragma: no cover
        for r in self.records.all():
            print(r.__repr__())


class DatasetVersionSet(models.Model):

    """
    A table which contains records, that are different dataset versions of each other.

    Note! Does not inherit from model Common, so does not have timestmap fields,
    and a delete is an actual delete.
    """
    id = models.BigAutoField(primary_key=True, editable=False)

    def get_listing(self):
        """
        Return a list of record preferred_identifiers that belong in the same dataset version chain.
        Latest first.
        """
        return [
            {
                'identifier': r.identifier,
                'preferred_identifier': r.preferred_identifier,
                'removed': r.removed,
                'date_created': r.date_created.astimezone().isoformat()
            }
            for r in self.records(manager='objects_unfiltered').all().order_by('-date_created')
        ]

    def print_records(self): # pragma: no cover
        for r in self.records.all():
            print(r.__repr__())


class ResearchDatasetVersion(models.Model):

    date_created = models.DateTimeField()
    stored_to_pas = models.DateTimeField(null=True)
    metadata_version_identifier = models.CharField(max_length=200, unique=True)
    preferred_identifier = models.CharField(max_length=200)
    research_dataset = JSONField()
    catalog_record = models.ForeignKey('CatalogRecord', on_delete=models.DO_NOTHING,
        related_name='research_dataset_versions')

    class Meta:
        indexes = [
            models.Index(fields=['metadata_version_identifier']),
        ]

    def __str__(self):
        return self.__repr__()

    def __repr__(self):
        return '<%s: %d, cr: %d, metadata_version_identifier: %s, stored_to_pas: %s>' \
            % (
                'ResearchDatasetVersion',
                self.id,
                self.catalog_record_id,
                self.metadata_version_identifier,
                str(self.stored_to_pas),
            )


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
            elif row.get('identifier', None):
                kwargs['identifier'] = row['identifier']
            elif row.get('research_dataset', None) and row['research_dataset'].get('metadata_version_identifier', None):
                # todo probably remove at some point
                kwargs['research_dataset__contains'] = {
                    'metadata_version_identifier': row['research_dataset']['metadata_version_identifier']
                }
            else:
                raise ValidationError(
                    'this operation requires an identifying key to be present: id, or identifier')
        return super(CatalogRecordManager, self).get(*args, **kwargs)

    def get_id(self, metadata_version_identifier=None): # pragma: no cover
        """
        Takes metadata_version_identifier, and returns the plain pk of the record. Useful for debugging
        """
        if not metadata_version_identifier:
            raise ValidationError('metadata_version_identifier is a required keyword argument')
        cr = super(CatalogRecordManager, self).filter(
            **{ 'research_dataset__contains': {'metadata_version_identifier': metadata_version_identifier } }
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

    identifier = models.CharField(max_length=200, unique=True, null=False)

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

    next_dataset_version = models.OneToOneField('self', on_delete=models.DO_NOTHING, null=True,
        related_name='+')

    previous_dataset_version = models.OneToOneField('self', on_delete=models.DO_NOTHING, null=True,
        related_name='+')

    dataset_version_set = models.ForeignKey(
        DatasetVersionSet, on_delete=models.SET_NULL, null=True, related_name='records',
        help_text='Records which are different dataset versions of each other.')

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
    # new_metadata_version_created_in_current_request = False
    new_dataset_version_created_in_current_request = False

    objects = CatalogRecordManager()

    class Meta:
        indexes = [
            models.Index(fields=['data_catalog']),
            models.Index(fields=['identifier']),
        ]
        ordering = ['id']

    def __init__(self, *args, **kwargs):
        super(CatalogRecord, self).__init__(*args, **kwargs)
        self.track_fields(
            'deprecated',
            'identifier',
            'preservation_state',
            'research_dataset',
            'research_dataset.files',
            'research_dataset.directories',
            'research_dataset.total_ida_byte_size',
            'research_dataset.total_remote_resources_byte_size',
            'research_dataset.metadata_version_identifier',
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
            _logger.info(
                'Created a new <CatalogRecord id: %d, '
                'metadata_version_identifier: %s, '
                'preferred_identifier: %s >'
                % (self.id, self.metadata_version_identifier, self.preferred_identifier)
            )
        else:
            self._pre_update_operations()
            super(CatalogRecord, self).save(*args, **kwargs)

    def _process_file_changes(self, file_changes, new_record_id, old_record_id):
        """
        Process any changes in files between versions.

        Copy files from the previous version to the new version. Only copy files and directories according
        to files_to_keep and dirs_to_keep_by_project, and separately add any new files according to
        files_to_add and dirs_to_add_by_project.

        Returns a boolean actual_files_changed
        """
        actual_files_changed = False

        (files_to_add,
            files_to_remove,
            files_to_keep,
            dirs_to_add_by_project,
            dirs_to_remove_by_project,
            dirs_to_keep_by_project) = file_changes

        if files_to_add or files_to_remove or dirs_to_add_by_project or dirs_to_remove_by_project:

            # note: if only files_to_keep and dirs_to_keep_by_project contained entries,
            # it means there were no file changes. this block is then not executed.

            if DEBUG:
                _logger.debug('Detected the following file changes:')

            if files_to_keep:
                # sql to copy single files from the previous version to the new version. only copy those
                # files which have been listed in research_dataset.files
                sql_copy_files_from_prev_version = '''
                    insert into metax_api_catalogrecord_files (catalogrecord_id, file_id)
                    select %s as catalogrecord_id, file_id
                    from metax_api_catalogrecord_files as cr_f
                    inner join metax_api_file as f on f.id = cr_f.file_id
                    where catalogrecord_id = %s
                    and file_id in %s
                '''
                sql_params_copy_files = [new_record_id, old_record_id, tuple(files_to_keep)]

                if DEBUG:
                    _logger.debug('File ids to keep: %s' % files_to_keep)

            if dirs_to_keep_by_project:
                # sql top copy files from entire directories. only copy files from the upper level dirs found
                # by processing research_dataset.directories.
                sql_copy_dirs_from_prev_version = '''
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
                '''
                sql_params_copy_dirs = [new_record_id, old_record_id]

                copy_dirs_sql = []

                for project, dir_paths in dirs_to_keep_by_project.items():
                    for dir_path in dir_paths:
                        copy_dirs_sql.append("(f.project_identifier = %s and f.file_path like (%s || '/%%'))")
                        sql_params_copy_dirs.extend([project, dir_path])

                sql_params_copy_dirs.extend([new_record_id])

                sql_copy_dirs_from_prev_version = sql_copy_dirs_from_prev_version.replace(
                    'COMPARE_PROJECT_AND_FILE_PATHS',
                    ' or '.join(copy_dirs_sql)
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
                    _logger.debug('Directory paths to keep, by project:')
                    for project, dir_paths in dirs_to_keep_by_project.items():
                        _logger.debug('\tProject: %s' % project)
                        for dir_path in dir_paths:
                            _logger.debug('\t\t%s' % dir_path)

            if dirs_to_add_by_project:
                # sql to add new files by directory path that were not previously included.
                # also takes care of "path is already included by another dir, but i want to check if there
                # are new files to add in there"
                sql_select_and_insert_files_by_dir_path = '''
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
                '''
                sql_params_insert_dirs = [new_record_id]

                add_dirs_sql = []

                for project, dir_paths in dirs_to_add_by_project.items():
                    for dir_path in dir_paths:
                        add_dirs_sql.append("(f.project_identifier = %s and f.file_path like (%s || '/%%'))")
                        sql_params_insert_dirs.extend([project, dir_path])

                sql_select_and_insert_files_by_dir_path = sql_select_and_insert_files_by_dir_path.replace(
                    'COMPARE_PROJECT_AND_FILE_PATHS',
                    ' or '.join(add_dirs_sql)
                )

                sql_params_insert_dirs.extend([new_record_id])

                if DEBUG:
                    _logger.debug('Directory paths to add, by project:')
                    for project, dir_paths in dirs_to_add_by_project.items():
                        _logger.debug('\tProject: %s' % project)
                        for dir_path in dir_paths:
                            _logger.debug('\t\t%s' % dir_path)

            if files_to_add:
                # sql to add any new singular files which were not covered by any directory path
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
                sql_params_insert_single = [new_record_id, tuple(files_to_add), new_record_id]

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
            sql_params_files_changed = [
                new_record_id,
                old_record_id,
                old_record_id,
                new_record_id
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
                _logger.debug('Actual files changed during version change: %s' % str(actual_files_changed))
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

        self._find_new_dirs_to_add(file_description_changes,
                                   dirs_to_add_by_project,
                                   dirs_to_remove_by_project,
                                   dirs_to_keep_by_project)

        self._find_new_files_to_add(file_description_changes,
                                    files_to_add,
                                    files_to_remove,
                                    files_to_keep)

        return (
            files_to_add,
            files_to_remove,
            files_to_keep,
            dirs_to_add_by_project,
            dirs_to_remove_by_project,
            dirs_to_keep_by_project
        )

    def _find_new_dirs_to_add(self, file_description_changes, dirs_to_add_by_project, dirs_to_remove_by_project,
            dirs_to_keep_by_project):
        """
        Based on changes in research_metadata.directories (parameter file_description_changes), find out which
        directories should be kept when copying files from the previous version to the new version,
        and which new directories should be added.
        """
        assert 'directories' in file_description_changes

        dir_identifiers = list(file_description_changes['directories']['removed']) + \
            list(file_description_changes['directories']['added'])

        dir_details = Directory.objects.filter(identifier__in=dir_identifiers) \
            .values('project_identifier', 'identifier', 'directory_path')

        if len(dir_identifiers) != len(dir_details):
            existig_dirs = set( d['identifier'] for d in dir_details )
            missing_identifiers = [ d for d in dir_identifiers if d not in existig_dirs ]
            raise ValidationError({'detail': ['the following directory identifiers were not found:\n%s'
                % '\n'.join(missing_identifiers) ]})

        for dr in dir_details:
            if dr['identifier'] in file_description_changes['directories']['added']:
                # include all new dirs found, to add files from an entirely new directory,
                # or to search an already existing directory for new files (and sub dirs).
                # as a result the actual set of files may or may not change.
                dirs_to_add_by_project[dr['project_identifier']].append(dr['directory_path'])
            elif dr['identifier'] in file_description_changes['directories']['removed']:
                if not self._path_included_in_previous_metadata_version(dr['project_identifier'], dr['directory_path']):
                    # only remove dirs that are not included by other directory paths.
                    # as a result the actual set of files may change, if the path is not included
                    # in the new directory additions
                    dirs_to_remove_by_project[dr['project_identifier']].append(dr['directory_path'])

        # when keeping directories when copying, only the top level dirs are required
        top_dirs_by_project = self._get_top_level_parent_dirs_by_project(
            file_description_changes['directories']['keep'])

        for project, dirs in top_dirs_by_project.items():
            dirs_to_keep_by_project[project] = dirs

    def _find_new_files_to_add(self, file_description_changes, files_to_add, files_to_remove, files_to_keep):
        """
        Based on changes in research_metadata.files (parameter file_description_changes), find out which
        files should be kept when copying files from the previous version to the new version,
        and which new files should be added.
        """
        assert 'files' in file_description_changes

        file_identifiers = list(file_description_changes['files']['removed']) + \
            list(file_description_changes['files']['added']) + \
            list(file_description_changes['files']['keep'])

        file_details = File.objects.filter(identifier__in=file_identifiers) \
            .values('id', 'project_identifier', 'identifier', 'file_path')

        if len(file_identifiers) != len(file_details):
            existig_files = set( f['identifier'] for f in file_details )
            missing_identifiers = [ f for f in file_identifiers if f not in existig_files ]
            raise ValidationError({'detail': ['the following file identifiers were not found:\n%s'
                % '\n'.join(missing_identifiers) ]})

        for f in file_details:
            if f['identifier'] in file_description_changes['files']['added']:
                # include all new files even if it already included by another directory's path,
                # to check later that it is not a file that was created later.
                # as a result the actual set of files may or may not change.
                files_to_add.append(f['id'])
            elif f['identifier'] in file_description_changes['files']['removed']:
                if not self._path_included_in_previous_metadata_version(f['project_identifier'], f['file_path']):
                    # file is being removed.
                    # path is not included by other directories in the previous version,
                    # which means the actual set of files may change, if the path is not included
                    # in the new directory additions
                    files_to_remove.append(f['id'])
            elif f['identifier'] in file_description_changes['files']['keep']:
                files_to_keep.append(f['id'])

    def _path_included_in_previous_metadata_version(self, project, path):
        """
        Check if a path in a specific project is already included in the path of
        another directory included in the PREVIOUS VERSION dataset selected directories.
        """
        if not hasattr(self, '_previous_highest_level_dirs_by_project'):
            if 'directories' not in self._initial_data['research_dataset']:
                return False
            dir_identifiers = [
                d['identifier'] for d in self._initial_data['research_dataset']['directories']
            ]
            self._previous_highest_level_dirs_by_project = self._get_top_level_parent_dirs_by_project(dir_identifiers)
        return any(
            True for dr_path in self._previous_highest_level_dirs_by_project.get(project, [])
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
    def metadata_version_identifier(self):
        try:
            return self.research_dataset['metadata_version_identifier']
        except:
            return None

    def catalog_versions_datasets(self):
        return self.data_catalog.catalog_json.get('dataset_versioning', False) is True

    def catalog_is_harvested(self):
        return self.data_catalog.catalog_json.get('harvested', False) is True

    def has_alternate_records(self):
        return bool(self.alternate_record_set)

    def get_metadata_version_listing(self):
        entries = []
        for entry in self.research_dataset_versions.all():
            entries.append({
                'id': entry.id,
                'date_created': entry.date_created,
                'metadata_version_identifier': entry.metadata_version_identifier,
            })
            if entry.stored_to_pas:
                # dont include null values
                entries[-1]['stored_to_pas'] = entry.stored_to_pas
        return entries

    def _pre_create_operations(self):
        if self.catalog_is_harvested():
            # in harvested catalogs, the harvester is allowed to set the preferred_identifier.
            # do not overwrite. note: if the value was left empty, an error would have been
            # raised in the serializer validation.
            pass
        else:
            self.research_dataset['preferred_identifier'] = generate_identifier()

        self.research_dataset['metadata_version_identifier'] = generate_identifier(urn=False)

        self.identifier = generate_identifier(urn=False)

        if 'remote_resources' in self.research_dataset:
            self._calculate_total_remote_resources_byte_size()

    def _post_create_operations(self):
        if self.catalog_versions_datasets():
            dvs = DatasetVersionSet()
            dvs.save()
            dvs.records.add(self)

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
        if self.field_changed('identifier'):
            # read-only
            self.identifier = self._initial_data['identifier']

        if self.field_changed('research_dataset.metadata_version_identifier'):
            # read-only
            self.research_dataset['metadata_version_identifier'] = \
                self._initial_data['research_dataset']['metadata_version_identifier']

        if self.field_changed('research_dataset.preferred_identifier'):
            if not self.catalog_is_harvested():
                raise Http400("Cannot change preferred_identifier in datasets in non-harvested catalogs")

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

        if self.field_changed('deprecated') and self._initial_data['deprecated'] is True:
            raise Http400("Cannot change dataset deprecation state from true to false")

        if self.catalog_versions_datasets() and not self.preserve_version:
            if self.field_changed('research_dataset'):
                if self._files_changed():
                    self._create_new_dataset_version()
                else:
                    self._handle_metadata_versioning()
        else:
            # non-versioning catalogs, such as harvesters, or if an update
            # was forced to occur without version update.

            if self.preserve_version:

                changes = self._get_metadata_file_changes()

                if any((changes['files']['added'], changes['files']['removed'],
                        changes['directories']['added'], changes['directories']['removed'])):

                    raise ValidationError({
                        'detail': [
                            'currently trying to preserve version while making changes which may result in files '
                            'being changed is not supported.'
                        ]
                    })

            if self.catalog_is_harvested() and self.field_changed('research_dataset.preferred_identifier'):
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

    def _files_changed(self):
        file_changes = self._find_file_changes()
        if False:
            # can also check presence of added, removed etc and return false immediately
            pass

        try:
            with transaction.atomic():
                # create temporary record to perform the file changes on, to see if there were real file changes.
                # if no, discard it. if yes, the temp_record will be transformed into the new dataset version record.
                temp_record = CatalogRecord.objects.get(pk=self.id)
                temp_record.id = None
                temp_record.next_dataset_version = None
                temp_record.previous_dataset_version = None
                temp_record.identifier = generate_identifier()
                temp_record.research_dataset['metadata_version_identifier'] = generate_identifier()
                super(Common, temp_record).save()
                actual_files_changed = self._process_file_changes(file_changes, temp_record.id, self.id)

                if actual_files_changed:
                    self._new_version = temp_record
                    return True
                else:
                    _logger.debug('no real file changes detected, discarding the temporary record...')
                    raise DiscardRecord()
        except DiscardRecord:
            # rolled back
            pass
        return False

    def _handle_metadata_versioning(self):
        if not self.research_dataset_versions.exists():
            # when a record is initially created, there are no versions.
            # when the first new version is created, first add the initial version.
            first_rdv = ResearchDatasetVersion(
                date_created=self.date_created,
                metadata_version_identifier=self._initial_data['research_dataset']['metadata_version_identifier'],
                preferred_identifier=self.preferred_identifier,
                research_dataset=self._initial_data['research_dataset'],
                catalog_record=self,
            )
            first_rdv.save()

        # create and add the new metadata version

        self.research_dataset['metadata_version_identifier'] = generate_identifier()

        new_rdv = ResearchDatasetVersion(
            date_created=self.date_modified,
            metadata_version_identifier=self.research_dataset['metadata_version_identifier'],
            preferred_identifier=self.preferred_identifier,
            research_dataset=self.research_dataset,
            catalog_record=self,
        )
        new_rdv.save()

    def _create_new_dataset_version(self):
        """
        Create a new dataset version of the record who calls this method.
        """
        assert hasattr(self, '_new_version'), 'self._new_version should have been set in a previous step'
        old_version = self

        if old_version.next_dataset_version_id:
            raise ValidationError({ 'detail': ['Changing files in old dataset versions is not permitted.'] })
            _logger.info(
                'Files changed during CatalogRecord update. Creating new dataset version '
                'from CatalogRecord %s...' % old_version.metadata_version_identifier
            )

        new_version = self._new_version
        new_version.contract = None
        new_version.date_created = old_version.date_modified
        new_version.date_modified = None
        new_version.user_created = old_version.user_modified
        new_version.user_modified = None
        new_version.preservation_description = None
        new_version.preservation_state = 0
        new_version.preservation_state_modified = None
        new_version.previous_dataset_version_id = old_version.id
        new_version.next_dataset_version_id = None
        new_version.service_created = old_version.service_modified or old_version.service_created
        new_version.service_modified = None
        new_version.alternate_record_set = None
        new_version.dataset_version_set.records.add(new_version)

        # note: copying research_dataset from the currently open instance 'old_version',
        # contains the new field data from the request. this effectively transfers
        # the changes to the new dataset version.
        new_version.research_dataset = deepcopy(old_version.research_dataset)
        new_version.research_dataset['metadata_version_identifier'] = generate_identifier()
        new_version.research_dataset['preferred_identifier'] = generate_identifier()
        new_version._calculate_total_ida_byte_size()

        if 'remote_resources' in new_version.research_dataset:
            new_version._calculate_total_remote_resources_byte_size()

        # nothing must change in the now old version of research_dataset, so copy
        # from _initial_data so that super().save() does not change it later.
        old_version.research_dataset = deepcopy(old_version._initial_data['research_dataset'])
        old_version.next_dataset_version_id = new_version.id

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

        _logger.info('New dataset version %s created' % new_version.preferred_identifier)
        old_version.new_dataset_version_created_in_current_request = True

    def _get_metadata_file_changes(self):
        """
        Check which files and directories selected in research_dataset have changed, and which to keep.
        This data will later be used when copying files from a previous version to a new version.

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
        changes['files'] = {
            'keep': initial_files.intersection(received_files),
            'removed': initial_files.difference(received_files),
            'added': received_files.difference(initial_files),
        }

        initial_dirs = set(dr['identifier'] for dr in self._initial_data['research_dataset'].get('directories', []))
        received_dirs = set( dr['identifier'] for dr in self.research_dataset.get('directories', []) )
        changes['directories'] = {
            'keep': initial_dirs.intersection(received_dirs),
            'removed': initial_dirs.difference(received_dirs),
            'added': received_dirs.difference(initial_dirs),
        }

        return changes

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
                (self.preferred_identifier, self.metadata_version_identifier, other_record.metadata_version_identifier))

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
        return '<%s: %d, removed: %s, data_catalog: %s, metadata_version_identifier: %s, ' \
            'preferred_identifier: %s, file_count: %d >' \
            % (
                'CatalogRecord',
                self.id,
                str(self.removed),
                self.data_catalog.catalog_json['identifier'],
                self.metadata_version_identifier,
                self.preferred_identifier,
                self.files.count(),
            )
