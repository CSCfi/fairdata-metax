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
from django.contrib.postgres.fields import JSONField, ArrayField
from django.db import connection, models, transaction
from django.db.models import Q, Sum
from django.http import Http404
from rest_framework.serializers import ValidationError

from metax_api.exceptions import Http400, Http403, Http503
from metax_api.utils import get_tz_aware_now_without_micros, generate_doi_identifier, generate_uuid_identifier, \
    executing_test_case, extract_doi_from_doi_identifier, IdentifierType, get_identifier_type, \
    parse_timestamp_string_to_tz_aware_datetime, is_metax_generated_doi_identifier
from .common import Common, CommonManager
from .contract import Contract
from .data_catalog import DataCatalog
from .directory import Directory
from .file import File


READ_METHODS = ('GET', 'HEAD', 'OPTIONS')
DEBUG = settings.DEBUG
_logger = logging.getLogger(__name__)


ACCESS_TYPES = {
    'open': 'http://uri.suomi.fi/codelist/fairdata/access_type/code/open',
    'login': 'http://uri.suomi.fi/codelist/fairdata/access_type/code/login',
    'permit': 'http://uri.suomi.fi/codelist/fairdata/access_type/code/permit',
    'embargo': 'http://uri.suomi.fi/codelist/fairdata/access_type/code/embargo',
    'restricted': 'http://uri.suomi.fi/codelist/fairdata/access_type/code/restricted'
}


LEGACY_CATALOGS = settings.LEGACY_CATALOGS


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

    PRESERVATION_STATE_INITIALIZED = 0
    PRESERVATION_STATE_PROPOSED = 10
    PRESERVATION_STATE_TECHNICAL_METADATA_GENERATED = 20
    PRESERVATION_STATE_TECHNICAL_METADATA_GENERATED_FAILED = 30
    PRESERVATION_STATE_INVALID_METADATA = 40
    PRESERVATION_STATE_METADATA_VALIDATION_FAILED = 50
    PRESERVATION_STATE_VALIDATED_METADATA_UPDATED = 60
    PRESERVATION_STATE_VALID_METADATA = 70
    PRESERVATION_STATE_METADATA_CONFIRMED = 75
    PRESERVATION_STATE_ACCEPTED_TO_PAS = 80
    PRESERVATION_STATE_IN_PACKAGING_SERVICE = 90
    PRESERVATION_STATE_PACKAGING_FAILED = 100
    PRESERVATION_STATE_SIP_IN_INGESTION = 110
    PRESERVATION_STATE_IN_PAS = 120
    PRESERVATION_STATE_REJECTED_FROM_PAS = 130
    PRESERVATION_STATE_IN_DISSEMINATION = 140

    PRESERVATION_STATE_CHOICES = (
        (PRESERVATION_STATE_INITIALIZED, 'Initialized'),
        (PRESERVATION_STATE_PROPOSED, 'Proposed for digital preservation'),
        (PRESERVATION_STATE_TECHNICAL_METADATA_GENERATED, 'Technical metadata generated'),
        (PRESERVATION_STATE_TECHNICAL_METADATA_GENERATED_FAILED, 'Technical metadata generation failed'),
        (PRESERVATION_STATE_INVALID_METADATA, 'Invalid metadata'),
        (PRESERVATION_STATE_METADATA_VALIDATION_FAILED, 'Metadata validation failed'),
        (PRESERVATION_STATE_VALIDATED_METADATA_UPDATED, 'Validated metadata updated'),
        (PRESERVATION_STATE_VALID_METADATA, 'Valid metadata'),
        (PRESERVATION_STATE_METADATA_CONFIRMED, 'Metadata confirmed'),
        (PRESERVATION_STATE_ACCEPTED_TO_PAS, 'Accepted to digital preservation'),
        (PRESERVATION_STATE_IN_PACKAGING_SERVICE, 'in packaging service'),
        (PRESERVATION_STATE_PACKAGING_FAILED, 'Packaging failed'),
        (PRESERVATION_STATE_SIP_IN_INGESTION, 'SIP sent to ingestion in digital preservation service'),
        (PRESERVATION_STATE_IN_PAS, 'in digital preservation'),
        (PRESERVATION_STATE_REJECTED_FROM_PAS, 'Rejected in digital preservation service'),
        (PRESERVATION_STATE_IN_DISSEMINATION, 'in dissemination'),
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

    date_deprecated = models.DateTimeField(null=True)

    _directory_data = JSONField(null=True, help_text='Stores directory data related to browsing files and directories')

    files = models.ManyToManyField(File)

    identifier = models.CharField(max_length=200, unique=True, null=False)

    mets_object_identifier = ArrayField(models.CharField(max_length=200), null=True)

    metadata_owner_org = models.CharField(max_length=200, null=True,
        help_text='Actually non-nullable, but is derived from field metadata_provider_org if omitted.')

    metadata_provider_org = models.CharField(max_length=200, null=False, help_text='Non-modifiable after creation')

    metadata_provider_user = models.CharField(max_length=200, null=False, help_text='Non-modifiable after creation')

    editor = JSONField(null=True, help_text='Editor specific fields, such as owner_id, modified, record_identifier')

    preservation_description = models.CharField(
        max_length=200, blank=True, null=True, help_text='Reason for accepting or rejecting PAS proposal.')

    preservation_reason_description = models.CharField(
        max_length=200, blank=True, null=True, help_text='Reason for PAS proposal from the user.')

    preservation_state = models.IntegerField(
        choices=PRESERVATION_STATE_CHOICES, default=PRESERVATION_STATE_INITIALIZED, help_text='Record state in PAS.')

    preservation_state_modified = models.DateTimeField(null=True, help_text='Date of last preservation state change.')

    preservation_identifier = models.CharField(max_length=200, unique=True, null=True)

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
    Signals to the serializer the need to populate field 'new_version_created'.
    """
    new_dataset_version_created = False

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
            'date_deprecated',
            'deprecated',
            'identifier',
            'metadata_owner_org',
            'metadata_provider_org',
            'metadata_provider_user',
            'preservation_state',
            'preservation_identifier',
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

    def user_has_access(self, request):
        """
        In the future, will probably be more involved checking...
        """
        if request.user.is_service:
            return True

        elif request.method in READ_METHODS:
            return True

        # write operation
        return self.user_is_owner(request)

    def user_is_owner(self, request):
        if self.editor and 'owner_id' in self.editor:
            return request.user.username == self.editor['owner_id']
        elif self.metadata_provider_user:
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
        if request.user.is_service:
            # knows what they are doing
            return True
        elif self.user_is_owner(request):
            # can see sensitive fields
            return True
        else:
            # unknown user
            return False

    def _access_type_is_open(self):
        from metax_api.services import CatalogRecordService as CRS
        return CRS.get_research_dataset_access_type(self.research_dataset) == ACCESS_TYPES['open']

    def _access_type_is_login(self):
        from metax_api.services import CatalogRecordService as CRS
        return CRS.get_research_dataset_access_type(self.research_dataset) == ACCESS_TYPES['login']

    def _access_type_is_embargo(self):
        from metax_api.services import CatalogRecordService as CRS
        return CRS.get_research_dataset_access_type(self.research_dataset) == ACCESS_TYPES['embargo']

    def _embargo_is_available(self):
        if not self.research_dataset.get('access_rights', {}).get('available', False):
            return False
        try:
            return get_tz_aware_now_without_micros() >= parse_timestamp_string_to_tz_aware_datetime(
                self.research_dataset.get('access_rights', {}).get('available', {}))
        except Exception as e:
            _logger.error(e)
            return False

    def authorized_to_see_catalog_record_files(self, request):
        return self.user_is_privileged(request) or self._access_type_is_open() or self._access_type_is_login() or \
            (self._access_type_is_embargo() and self._embargo_is_available())

    def save(self, *args, **kwargs):
        if self._operation_is_create():
            self._pre_create_operations()
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

        files_to_add              = file_changes['files_to_add']
        files_to_remove           = file_changes['files_to_remove']
        files_to_keep             = file_changes['files_to_keep']
        dirs_to_add_by_project    = file_changes['dirs_to_add_by_project']
        dirs_to_remove_by_project = file_changes['dirs_to_remove_by_project']
        dirs_to_keep_by_project   = file_changes['dirs_to_keep_by_project']

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

        # list of projects involved in current changes. handy later when checking user
        # permissions for changed files: no need to retrieve related project identifiers again
        changed_projects = defaultdict(set)

        self._find_new_dirs_to_add(file_description_changes,
                                   dirs_to_add_by_project,
                                   dirs_to_remove_by_project,
                                   dirs_to_keep_by_project)

        self._find_new_files_to_add(file_description_changes,
                                    files_to_add,
                                    files_to_remove,
                                    files_to_keep,
                                    changed_projects)

        # involved projects from single files (research_dataset.files) were accumulated
        # in the previous method, but for dirs, its just handy to get the keys from below
        # variables...
        for project_identifier in dirs_to_add_by_project.keys():
            changed_projects['files_added'].add(project_identifier)

        for project_identifier in dirs_to_remove_by_project.keys():
            changed_projects['files_removed'].add(project_identifier)

        return {
            'files_to_add': files_to_add,
            'files_to_remove': files_to_remove,
            'files_to_keep': files_to_keep,
            'dirs_to_add_by_project': dirs_to_add_by_project,
            'dirs_to_remove_by_project': dirs_to_remove_by_project,
            'dirs_to_keep_by_project': dirs_to_keep_by_project,
            'changed_projects': changed_projects,
        }

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

    def _find_new_files_to_add(self, file_description_changes, files_to_add, files_to_remove, files_to_keep,
            changed_projects):
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
                changed_projects['files_added'].add(f['project_identifier'])
            elif f['identifier'] in file_description_changes['files']['removed']:
                if not self._path_included_in_previous_metadata_version(f['project_identifier'], f['file_path']):
                    # file is being removed.
                    # path is not included by other directories in the previous version,
                    # which means the actual set of files may change, if the path is not included
                    # in the new directory additions
                    files_to_remove.append(f['id'])
                    changed_projects['files_removed'].add(f['project_identifier'])
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
            if path != dr_path and path.startswith('%s/' % dr_path)
        )

    def delete(self, *args, **kwargs):
        if self.has_alternate_records():
            self._remove_from_alternate_record_set()
        if get_identifier_type(self.preferred_identifier) == IdentifierType.DOI:
            self.add_post_request_callable(DataciteDOIUpdate(self, self.research_dataset['preferred_identifier'],
                                                             'delete'))
        self.add_post_request_callable(RabbitMQPublishRecord(self, 'delete'))
        if self.catalog_is_legacy():
            # delete permanently instead of only marking as 'removed'
            super(Common, self).delete()
        else:
            super().delete(*args, **kwargs)

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

    def catalog_is_legacy(self):
        return self.data_catalog.catalog_json['identifier'] in LEGACY_CATALOGS

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
        pref_id_type = self._get_preferred_identifier_type_from_request()
        if self.catalog_is_harvested():
            # in harvested catalogs, the harvester is allowed to set the preferred_identifier.
            # do not overwrite.
            pass
        elif self.catalog_is_legacy():
            if 'preferred_identifier' not in self.research_dataset:
                raise ValidationError({
                    'detail': [
                        'Selected catalog %s is a legacy catalog. Preferred identifiers are not '
                        'automatically generated for datasets stored in legacy catalogs, nor is '
                        'their uniqueness enforced. Please provide a value for dataset field '
                        'preferred_identifier.'
                        % self.data_catalog.catalog_json['identifier']
                    ]
                })
            _logger.info(
                'Catalog %s is a legacy catalog - not generating pid'
                % self.data_catalog.catalog_json['identifier']
            )
        else:
            if pref_id_type == IdentifierType.URN:
                self.research_dataset['preferred_identifier'] = generate_uuid_identifier(urn_prefix=True)
            elif pref_id_type == IdentifierType.DOI:
                doi_id = generate_doi_identifier()
                self.research_dataset['preferred_identifier'] = doi_id
                if self.dataset_in_ida_data_catalog():
                    self.preservation_identifier = doi_id
            else:
                _logger.debug("Identifier type not specified in the request. Using URN identifier for pref id")
                self.research_dataset['preferred_identifier'] = generate_uuid_identifier(urn_prefix=True)

        self.research_dataset['metadata_version_identifier'] = generate_uuid_identifier()
        self.identifier = generate_uuid_identifier()

        if not self.metadata_owner_org:
            # field metadata_owner_org is optional, but must be set. in case it is omitted,
            # derive from metadata_provider_org.
            self.metadata_owner_org = self.metadata_provider_org

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
            super().save(update_fields=['research_dataset']) # save byte size calculation
            self.calculate_directory_byte_sizes_and_file_counts()

        other_record = self._check_alternate_records()
        if other_record:
            self._create_or_update_alternate_record_set(other_record)

        if get_identifier_type(self.preferred_identifier) == IdentifierType.DOI:
            self.add_post_request_callable(DataciteDOIUpdate(self, self.research_dataset['preferred_identifier'],
                                                             'create'))

        if self._dataset_is_access_restricted():
            self.add_post_request_callable(REMSUpdate(self), 'create')

        self.add_post_request_callable(RabbitMQPublishRecord(self, 'create'))

        _logger.info(
            'Created a new <CatalogRecord id: %d, '
            'identifier: %s, '
            'preferred_identifier: %s >'
            % (self.id, self.identifier, self.preferred_identifier)
        )

    def _pre_update_operations(self):
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

        if self._dataset_restricted_access_changed():
            # todo check if restriction_grounds and access_type changed
            pass

        if self.field_changed('research_dataset'):
            self.update_datacite = True
        else:
            self.update_datacite = False

        if self.catalog_versions_datasets() and not self.preserve_version:

            if not self.field_changed('research_dataset'):
                # proceed directly to updating current record without any extra measures...
                return

            if self._files_changed():

                if self.preservation_state > self.PRESERVATION_STATE_INITIALIZED: # state > 0
                    raise Http400({ 'detail': [
                        'Changing files is not allowed when dataset is in a PAS process. Current '
                        'preservation_state = %d. In order to alter associated files, change preservation_state '
                        'back to 0.' % self.preservation_state
                    ]})

                if self._files_added_for_first_time():
                    # first update from 0 to n files should not create a dataset version. all later updates
                    # will create new dataset versions normally.
                    self.files.add(*self._get_dataset_selected_file_ids())
                    self._calculate_total_ida_byte_size()
                    self._handle_metadata_versioning()
                    self.calculate_directory_byte_sizes_and_file_counts()
                else:
                    self._create_new_dataset_version()

            else:
                if self.preservation_state in (
                        self.PRESERVATION_STATE_INVALID_METADATA,           # 40
                        self.PRESERVATION_STATE_METADATA_VALIDATION_FAILED, # 50
                        self.PRESERVATION_STATE_VALID_METADATA):            # 70
                    # notifies the user in Hallintaliittyma that the metadata needs to be re-validated
                    self.preservation_state = self.PRESERVATION_STATE_VALIDATED_METADATA_UPDATED # 60
                    self.preservation_state_modified = get_tz_aware_now_without_micros()

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

    def _post_update_operations(self):
        if get_identifier_type(self.preferred_identifier) == IdentifierType.DOI and \
                self.update_datacite:
            self.add_post_request_callable(DataciteDOIUpdate(self, self.research_dataset['preferred_identifier'],
                                                             'update'))

        self.add_post_request_callable(RabbitMQPublishRecord(self, 'update'))

    def _files_added_for_first_time(self):
        """
        Find out if this update is the first time files are being added/changed since the dataset's creation.
        """
        if self.files.exists():
            # current version already has files
            return False

        if self.dataset_version_set.records.count() > 1:
            # for versioned catalogs, when a record is first created, the record is appended
            # to dataset_version_set. more than one dataset versions existing implies files
            # have changed already in the past.
            return False

        metadata_versions_with_files_exist = ResearchDatasetVersion.objects.filter(
            Q(Q(research_dataset__files__isnull=False) | Q(research_dataset__directories__isnull=False)),
            catalog_record_id=self.id) \
            .exists()

        # metadata_versions_with_files_exist == True implies this "0 to n" update without
        # creating a new dataset version already occurred once
        return not metadata_versions_with_files_exist

    def _dataset_is_access_restricted(self):
        """
        Check using logic x and y if dataset uses REMS for managing access.
        """
        return False

    def _dataset_restricted_access_changed(self):
        """
        Check using logic x and y if dataset uses REMS for managing access.
        """
        return False

    def dataset_in_ida_data_catalog(self):
        return self.data_catalog.catalog_json['identifier'] == 'urn:nbn:fi:att:data-catalog-ida'

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
        file_changes = { 'changed_projects': defaultdict(set) }

        if 'files' in self.research_dataset:
            file_ids.extend(self._get_file_ids_from_file_list(self.research_dataset['files'], file_changes))

        if 'directories' in self.research_dataset:
            file_ids.extend(self._get_file_ids_from_dir_list(self.research_dataset['directories'],
                file_ids, file_changes))

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

        file_pids = [ f['identifier'] for f in file_list ]

        files = File.objects.filter(identifier__in=file_pids).values('id', 'identifier', 'project_identifier')
        file_ids = []
        for f in files:
            file_ids.append(f['id'])
            file_changes['changed_projects']['files_added'].add(f['project_identifier'])

        if len(file_ids) != len(file_pids):
            missing_identifiers = [ pid for pid in file_pids if pid not in set(f['identifier'] for f in files)]
            raise ValidationError({ 'detail': [
                'some requested files were not found. file identifiers not found:\n%s'
                % '\n'.join(missing_identifiers)
            ]})

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
            file_changes['changed_projects']['files_added'].add(project_identifier)

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
            if dr['directory_path'] == '/':
                raise ValidationError({ 'detail': [
                    'Adding the filesystem root directory ("/") to a dataset is not allowed. Identifier of the '
                    'offending directory: %s' % dr['directory_path']
                ]})
            dirs_by_project[dr['project_identifier']].append(dr['directory_path'])

        top_level_dirs_by_project = defaultdict(list)

        for proj, dir_paths in dirs_by_project.items():

            for path in dir_paths:

                dir_is_root = [ p.startswith('%s/' % path) for p in dir_paths if p != path ]

                if all(dir_is_root):
                    # found the root dir. disregard all the rest of the paths, if there were any.
                    top_level_dirs_by_project[proj] = [path]
                    break
                else:
                    path_contained_by_other_paths = [ path.startswith('%s/' % p) for p in dir_paths if p != path ]

                    if any(path_contained_by_other_paths):
                        # a child of at least one other path. no need to include it in the list
                        pass
                    else:
                        # "unique", not a child of any other path
                        top_level_dirs_by_project[proj].append(path)

        return top_level_dirs_by_project

    def _files_changed(self):
        file_changes = self._find_file_changes()

        if not file_changes['changed_projects']:
            return False

        self._check_changed_files_permissions(file_changes)

        try:
            with transaction.atomic():
                # create temporary record to perform the file changes on, to see if there were real file changes.
                # if no, discard it. if yes, the temp_record will be transformed into the new dataset version record.

                # note: when this code is executed, at this point it should be reasonably certain that associated
                # files did in fact change, and the following operation is necessary. consider the exception
                # raising and rollback at the end as an extra safety measure.

                temp_record = CatalogRecord.objects.get(pk=self.id)
                temp_record.id = None
                temp_record.next_dataset_version = None
                temp_record.previous_dataset_version = None
                temp_record.dataset_version_set = None
                temp_record.identifier = generate_uuid_identifier()
                temp_record.research_dataset['metadata_version_identifier'] = generate_uuid_identifier()
                temp_record.preservation_identifier = None
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

    def _check_changed_files_permissions(self, file_changes):
        '''
        Ensure user belongs to projects of all changed files and dirs.

        Raises 403 on error.
        '''
        if not self.request:
            # when files associated with a dataset have been changed, the user should be
            # always known, i.e. the http request object is present. if its not, the code
            # is not being executed as a result of a api request. in that case, only allow
            # proceeding when the code is executed for testing: the code is being called directly
            # from a test case, to set up test conditions etc.
            assert executing_test_case(), 'only permitted when setting up testing conditions'
            return

        if self.request.user.is_service:
            # assumed the service knows what it is doing
            return

        projects_added = file_changes['changed_projects'].get('files_added', set())
        projects_removed = file_changes['changed_projects'].get('files_removed', set())
        project_changes = projects_added.union(projects_removed)

        from metax_api.services import AuthService
        user_projects = AuthService.extract_file_projects_from_token(self.request.user.token)

        invalid_project_perms = [ proj for proj in project_changes if proj not in user_projects ]

        if invalid_project_perms:
            raise Http403({
                'detail': [
                    'Unable to add files to dataset. You are lacking project membership in the following projects: %s'
                    % ', '.join(invalid_project_perms)
                ]
            })

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

        self.research_dataset['metadata_version_identifier'] = generate_uuid_identifier()

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
            'from old CatalogRecord having identifier %s' % old_version.identifier
        )
        _logger.debug(
            'Old CR metadata version identifier: %s' % old_version.metadata_version_identifier
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
        new_version.research_dataset['metadata_version_identifier'] = generate_uuid_identifier()

        # This effectively means one cannot change identifier type for new catalog record versions
        pref_id_type = get_identifier_type(old_version.research_dataset['preferred_identifier'])
        if pref_id_type == IdentifierType.URN:
            new_version.research_dataset['preferred_identifier'] = generate_uuid_identifier(urn_prefix=True)
        elif pref_id_type == IdentifierType.DOI:
            doi_id = generate_doi_identifier()
            new_version.research_dataset['preferred_identifier'] = doi_id
            if self.dataset_in_ida_data_catalog():
                new_version.preservation_identifier = doi_id
        else:
            _logger.debug("This code should never be reached. Using URN identifier for the new version pref id")
            self.research_dataset['preferred_identifier'] = generate_uuid_identifier(urn_prefix=True)

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

        new_version.calculate_directory_byte_sizes_and_file_counts()

        if pref_id_type == IdentifierType.DOI:
            self.add_post_request_callable(DataciteDOIUpdate(new_version,
                                                             new_version.research_dataset['preferred_identifier'],
                                                             'create'))

        new_version.add_post_request_callable(RabbitMQPublishRecord(new_version, 'create'))

        old_version.new_dataset_version_created = True

        _logger.info('New dataset version created, identifier %s' % new_version.identifier)
        _logger.debug('New dataset version preferred identifer %s' % new_version.preferred_identifier)

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

    def calculate_directory_byte_sizes_and_file_counts(self):
        """
        Calculate directory byte_sizes and file_counts for all dirs selected for this cr.
        Since file changes will create a new dataset version, these values will never change.
        """
        if not self.research_dataset.get('directories', None):
            return

        _logger.info('Calculating directory byte_sizes and file_counts...')

        dir_identifiers = [ d['identifier'] for d in self.research_dataset['directories'] ]

        highest_level_dirs_by_project = self._get_top_level_parent_dirs_by_project(dir_identifiers)

        directory_data = {}

        for project_identifier, dir_paths in highest_level_dirs_by_project.items():
            dirs = Directory.objects.filter(project_identifier=project_identifier, directory_path__in=dir_paths)
            for dr in dirs:
                dr.calculate_byte_size_and_file_count_for_cr(self.id, directory_data)

        self._directory_data = directory_data
        super(Common, self).save(update_fields=['_directory_data'])

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

    def add_post_request_callable(self, *args, **kwargs):
        """
        Wrapper in order to import CommonService in one place only...
        """
        from metax_api.services import CallableService
        CallableService.add_post_request_callable(*args, **kwargs)

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

    def _get_preferred_identifier_type_from_request(self):
        """
        Get preferred identifier type as IdentifierType enum object

        :return: IdentifierType. Return None if parameter not given or is unrecognized. Calling code is then
        responsible for choosing which IdentifierType to use.
        """
        pid_type = self.request.query_params.get('pid_type', None)
        if pid_type == IdentifierType.DOI.value:
            pid_type = IdentifierType.DOI
        elif pid_type == IdentifierType.URN.value:
            pid_type = IdentifierType.URN
        return pid_type


class RabbitMQPublishRecord():

    """
    Callable object to be passed to CommonService.add_post_request_callable(callable).

    Handles rabbitmq publishing.
    """

    def __init__(self, cr, routing_key):
        assert routing_key in ('create', 'update', 'delete'), 'invalid value for routing_key'
        self.cr = cr
        self.routing_key = routing_key

    def __call__(self):
        """
        The actual code that gets executed during CommonService.run_post_request_callables().
        """
        from metax_api.services import RabbitMQService as rabbitmq

        _logger.info(
            'Publishing CatalogRecord %s to RabbitMQ... routing_key: %s'
            % (self.cr.identifier, self.routing_key)
        )

        if self.routing_key == 'delete':
            cr_json = { 'identifier': self.cr.identifier }
        else:
            cr_json = self._to_json()
            # Send full data_catalog json
            cr_json['data_catalog'] = {'catalog_json': self.cr.data_catalog.catalog_json}

        try:
            rabbitmq.publish(cr_json, routing_key=self.routing_key, exchange='datasets')
        except:
            # note: if we'd like to let the request be a success even if this operation fails,
            # we could simply not raise an exception here.
            _logger.exception('Publishing rabbitmq message failed')
            raise Http503({ 'detail': [
                'failed to publish updates to rabbitmq. request is aborted.'
            ]})

    def _to_json(self):
        from metax_api.api.rest.base.serializers import CatalogRecordSerializer
        return CatalogRecordSerializer(self.cr).data


class REMSUpdate():

    """
    Callable object to be passed to CommonService.add_post_request_callable(callable).

    Handles managing REMS resources when creating, updating and deleting datasets.
    """

    def __init__(self, cr, action):
        assert action in ('create', 'update', 'delete'), 'invalid value for action'
        self.cr = cr
        self.action = action

    def __call__(self):
        """
        The actual code that gets executed during CommonService.run_post_request_callables().
        """
        _logger.info(
            'Publishing CatalogRecord %s update to REMS... action: %s'
            % (self.cr.identifier, self.action)
        )

        try:
            # todo do_stuff()
            pass
        except:
            _logger.exception('REMS interaction failed')
            raise Http503({ 'detail': [
                'failed to publish updates to rems. request is aborted.'
            ]})


class DataciteDOIUpdate():

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
        assert action in ('create', 'update', 'delete'), 'invalid value for action'
        assert is_metax_generated_doi_identifier(doi_identifier)
        self.cr = cr
        self.doi_identifier = doi_identifier
        self.action = action

    def __call__(self):
        """
        The actual code that gets executed during CommonService.run_post_request_callables().
        Do not run for tests or in travis
        """
        from metax_api.services.datacite_service import DataciteService

        if hasattr(settings, 'DATACITE'):
            if not settings.DATACITE.get('ETSIN_URL_TEMPLATE', None):
                raise Exception('Missing configuration from settings for DATACITE: ETSIN_URL_TEMPLATE')
        else:
            raise Exception('Missing configuration from settings: DATACITE')

        doi = extract_doi_from_doi_identifier(self.doi_identifier)
        if doi is None:
            return

        if self.action == 'create':
            _logger.info(
                'Publishing CatalogRecord {0} metadata and url to Datacite API using DOI {1}'.
                format(self.cr.identifier, doi)
            )
        elif self.action == 'update':
            _logger.info(
                'Updating CatalogRecord {0} metadata and url to Datacite API using DOI {1}'.
                format(self.cr.identifier, doi)
            )
        elif self.action == 'delete':
            _logger.info(
                'Deleting CatalogRecord {0} metadata from Datacite API using DOI {1}'.
                format(self.cr.identifier, doi)
            )

        try:
            dcs = DataciteService()
            if self.action == 'create':
                try:
                    self._publish_to_datacite(dcs, doi)
                except Exception as e:
                    # Try to delete DOI in case the DOI got created but stayed in "draft" state
                    dcs.delete_draft_doi(doi)
                    raise(Exception(e))
            elif self.action == 'update':
                self._publish_to_datacite(dcs, doi)
            elif self.action == 'delete':
                # If metadata is in "findable" state, the operation below should transition the DOI to "registered"
                # state
                dcs.delete_doi_metadata(doi)
        except Exception as e:
            _logger.error(e)
            _logger.exception('Datacite API interaction failed')
            raise Http503({ 'detail': [
                'failed to publish updates to Datacite API. request is aborted.'
            ]})

    def _publish_to_datacite(self, dcs, doi):
        cr_json = {'research_dataset': self.cr.research_dataset}
        if self.cr.preservation_identifier:
            cr_json['preservation_identifier'] = self.cr.preservation_identifier

        datacite_xml = dcs.convert_catalog_record_to_datacite_xml(cr_json, True)
        _logger.debug("Datacite XML to be sent to Datacite API: {0}".format(datacite_xml))

        # When the two operations below are successful, it should result in the DOI transitioning to
        # "findable" state
        dcs.create_doi_metadata(datacite_xml)
        dcs.register_doi_url(doi, settings.DATACITE['ETSIN_URL_TEMPLATE'] % self.cr.identifier)
