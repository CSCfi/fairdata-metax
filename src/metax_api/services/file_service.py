import logging
from os import getpid
from os.path import dirname, basename
from time import time
from uuid import uuid3, NAMESPACE_DNS as UUID_NAMESPACE_DNS

from django.conf import settings
from django.db import connection
from django.db.models.fields import FieldDoesNotExist
from django.http import Http404
from rest_framework import status
from rest_framework.response import Response
from rest_framework.serializers import ValidationError

from metax_api.exceptions import Http400
from metax_api.models import CatalogRecord, Directory, File
from metax_api.utils import RabbitMQ
from metax_api.utils.utils import get_tz_aware_now_without_micros
from .common_service import CommonService


DEBUG = settings.DEBUG
_logger = logging.getLogger(__name__)
d = logging.getLogger(__name__).debug


"""
busting circular import issues...
note that the imports in below functions are only executed once, and
the function signatures are replaced with the imported classes. importing
multiple times in itself isnt bad, but using this function-replacement
-mechanic, so that the imports are not littered in several methods in
FileService where they would otherwise be needed.
"""
def CatalogRecordSerializer(*args, **kwargs):
    from metax_api.api.rest.base.serializers import CatalogRecordSerializer as CRS
    CatalogRecordSerializer = CRS
    return CatalogRecordSerializer(*args, **kwargs)

def DirectorySerializer(*args, **kwargs):
    from metax_api.api.rest.base.serializers import DirectorySerializer as DS
    DirectorySerializer = DS
    return DirectorySerializer(*args, **kwargs)

def FileSerializer(*args, **kwargs):
    from metax_api.api.rest.base.serializers import FileSerializer as FS
    FileSerializer = FS
    return FileSerializer(*args, **kwargs)


class MaxRecursionDepthExceeded(Exception):
    pass


class FileService(CommonService):

    @classmethod
    def get_datasets_where_file_belongs_to(cls, file_identifiers):
        """
        Find out which (non-deprecated) datasets a list of files belongs to, and return
        their preferred_identifiers as a list.

        Parameter file_identifiers can be a list of pk's (integers), or file identifiers (strings).
        """
        _logger.info('Retrieving list of datasets where files belong to')

        if not isinstance(file_identifiers, list):
            raise Http400('identifiers must be passed as a list')

        _logger.info('Looking datasets for the following files (printing first 10):\n%s'
                     % '\n'.join(str(id) for id in file_identifiers[:10]))

        file_ids = cls._file_identifiers_to_ids(file_identifiers)

        sql_select_related_records = """
            select research_dataset->>'preferred_identifier' as preferred_identifier
            from metax_api_catalogrecord cr
            inner join metax_api_catalogrecord_files cr_f on catalogrecord_id = cr.id
            where cr_f.file_id in %s and cr.removed = false and cr.active = true
            group by preferred_identifier
            """

        with connection.cursor() as cr:
            cr.execute(sql_select_related_records, [tuple(file_ids)])
            if cr.rowcount == 0:
                preferred_identifiers = []
                _logger.info('No datasets found for files')
            else:
                preferred_identifiers = [ row[0] for row in cr.fetchall() ]
                _logger.info('Found following datasets:\n%s' % '\n'.join(preferred_identifiers))

        return Response(preferred_identifiers, status=status.HTTP_200_OK)

    @classmethod
    def destroy_single(cls, file):
        """
        Mark a single file as removed. Marks related datasets deprecated, and deletes any empty
        directories above the file.
        """
        _logger.info('Begin delete file')

        deleted_files_count, project_identifier = cls._mark_files_as_deleted([file.id])
        cls._delete_empy_dir_chain_above(file.parent_directory)
        cls.calculate_project_directory_byte_sizes_and_file_counts(file.project_identifier)
        cls._mark_datasets_as_deprecated([file.id])

        _logger.info('Marked %d files as deleted from project %s' % (deleted_files_count, project_identifier))
        return Response({ 'deleted_files_count': deleted_files_count }, status=status.HTTP_200_OK)

    @classmethod
    def destroy_bulk(cls, file_identifiers):
        """
        Mark files as deleted en masse. Parameter file_identifiers can be a list of pk's
        (integers), or file identifiers (strings).

        A bulk delete request can concern either an entire directory and its subdirectories,
        or just some files in a directory, or all files in a single directory. Effectively,
        whatever files the request contains, should be deleted, while ensuring that
        the directory hierarchy remains valid for any files that are left after deleting.

        If the result of a bulk delete request is that none of the directories left
        contain any files, the all directories, including the root, will end up deleted.

        The method returns a http response with the number of deleted files in the body.
        """
        _logger.info('Begin bulk delete files')

        file_ids = cls._file_identifiers_to_ids(file_identifiers)

        deleted_files_count, project_identifier = cls._mark_files_as_deleted(file_ids)

        cls._find_and_delete_empty_directories(project_identifier)
        cls.calculate_project_directory_byte_sizes_and_file_counts(project_identifier)
        cls._mark_datasets_as_deprecated(file_ids)

        _logger.info('Marked %d files as deleted from project %s' % (deleted_files_count, project_identifier))
        return Response({ 'deleted_files_count': deleted_files_count }, status=status.HTTP_200_OK)

    @staticmethod
    def _file_identifiers_to_ids(file_identifiers):
        """
        In case file_identifiers is identifiers (strings), which they probably are in real use,
        do a query to get a list of pk's instead, since they will be used quite a few times.
        """
        if not isinstance(file_identifiers, list):
            raise Http400('Received identifiers is not a list')
        elif not file_identifiers:
            _logger.info('Received empty list of identifiers. Aborting')
            raise Http400('Received empty list of identifiers')
        elif isinstance(file_identifiers[0], int):
            return file_identifiers
        else:
            ids = [ id for id in File.objects.filter(identifier__in=file_identifiers).values_list('id', flat=True) ]
            if not ids:
                raise Http404
            return ids

    @staticmethod
    def _mark_files_as_deleted(file_ids):
        """
        Mark files designated by file_ids as deleted.
        """
        _logger.info('Marking files as removed...')

        sql_delete_files = '''
            update metax_api_file
            set removed = true, file_deleted = CURRENT_TIMESTAMP
            where active = true and removed = false
            and id in %s'''

        sql_select_related_projects = 'select distinct(project_identifier) from metax_api_file where id in %s'

        with connection.cursor() as cr:
            cr.execute(sql_select_related_projects, [tuple(file_ids)])
            if cr.rowcount == 0:
                raise Http400({ 'detail': ['no files found for given identifiers'] })
            elif cr.rowcount > 1:
                raise Http400({ 'project_identifier': [
                    'deleting files from more than one project in a single request is not allowed'] })
            project_identifier = cr.fetchone()[0]

            cr.execute(sql_delete_files, [tuple(file_ids)])
            deleted_files_count = cr.rowcount

        return deleted_files_count, project_identifier

    @classmethod
    def _find_and_delete_empty_directories(cls, project_identifier):
        """
        Find and delete, if feasible, all empty directories in a project. The only dir
        found in the project without a parent dir, is considered to be the root.
        """
        _logger.info('Finding and deleting empty directory chains...')

        root_dir = Directory.objects.filter(project_identifier=project_identifier, parent_directory_id=None)

        if root_dir.count() > 1: # pragma: no cover
            raise ValidationError({
                'detail': [
                    'found more than one root dir (directories without parents: %s. unable to proceed' %
                    ', '.join(dr.directory_path for dr in root_dir)
                ]
            })

        cls._delete_empy_directories(root_dir[0])

    @classmethod
    def _delete_empy_dir_chain_above(cls, directory):
        """
        When deleting a single file, find out if directories above are empty, and delete them.
        """
        if not directory:
            return
        elif directory.files.exists():
            return
        elif directory.child_directories.exists():
            return
        parent_directory = directory.parent_directory
        directory.delete()
        if parent_directory:
            cls._delete_empy_dir_chain_above(parent_directory)

    @classmethod
    def _delete_empy_directories(cls, dr):
        """
        Find and delete any empty sub directories, and the directory itself. If any of the
        sub directories contained any files, directory is not deleted. This method is
        intended to be called recursively.

        Returns True if the given directory, and its subdirs were deleted, and False,
        if the directory or any of its sub directories contained files, preventing
        deleting the directory.
        """
        sub_dirs_can_be_deleted = False
        sub_dirs_are_empty = []

        sub_dirs = dr.child_directories.all()

        if sub_dirs.exists():
            # start deleting directories from the bottom to up
            for sub_dr in sub_dirs:
                sub_dirs_are_empty.append(cls._delete_empy_directories(sub_dr))

            if all(sub_dirs_are_empty):
                # if even one sub dir needs to be preserved, the current directory must be preserved.
                sub_dirs_can_be_deleted = True
        else:
            sub_dirs_can_be_deleted = True

        if dr.files.exists():
            return False
        elif sub_dirs_can_be_deleted:
            dr.delete()
            return True
        else:
            return False

    @staticmethod
    def _mark_datasets_as_deprecated(file_ids):
        """
        Get all CatalogRecords which have files set to them from file_ids,
        and set their deprecated flag to True. Then, publish update-messages to rabbitmq.
        """
        _logger.info('Marking related datasets as deprecated...')
        deprecated_records = []

        for cr in CatalogRecord.objects.filter(files__in=file_ids, deprecated=False).distinct('id'):
            cr.deprecated = True
            cr.date_modified = get_tz_aware_now_without_micros()
            cr.save()
            deprecated_records.append(CatalogRecordSerializer(cr).data)

        _logger.info('Publishing deprecated datasets to rabbitmq update queues...')
        rabbitmq = RabbitMQ()
        rabbitmq.publish(deprecated_records, routing_key='update', exchange='datasets')

    @classmethod
    def get_directory_contents(cls, identifier=None, path=None, project_identifier=None,
            recursive=False, max_depth=1, dirs_only=False, include_parent=False, cr_identifier=None, request=None):
        """
        Get files and directories contained by a directory.

        Parameters:

        identifier: may be a pk, or an uuid value. Search using approriate fields.

        cr_identifier: may be used to browse files in the context of the given
        cr_identifier: Only those files and directories are retrieved, which have been
        selected for that CatalogRecord.

        path and project_identifier: may be specified to search directly by
        a known project and path, instead of a directory identifier. If identifier is passed,
        it is used instead of path and project.

        recursive: may be used to either get a flat list of all files below the
        directory, or a hierarchial directory tree with depth of max_depth.

        max_depth: has to be specified to a higher number than 1 for 'recursive' to go
        deeper than one level. 0 Does not return anything.

        dirs_only: may be set to True to retrieve only directories. If combined with
        'recursive', the returned directories will be hierarchial, and not a flat list, unlike
        with files.

        include_parent: may be set to True to include the 'parent' directory of the content
        being retrieved in the results. Example: /directories/3/files?include_parent=true also
        includes the data about directory id 3 in the results. Normally its data would not be
        present, and instead would need to be retrieved by calling /directories/3.

        request: the web request object.
        """
        assert request is not None, 'kw parameter request must be specified'

        if identifier and identifier.isdigit() and not include_parent:
            directory_id = identifier
        else:
            if identifier:
                if identifier.isdigit():
                    # go this path because parent needs to be included. avoid having to retrieve
                    # directory info again later
                    params = { 'id': identifier }
                else:
                    params = { 'identifier': identifier }
            elif path and project_identifier:
                params = { 'directory_path': path, 'project_identifier': project_identifier }
            else: # pragma: no cover
                raise ValidationError({'detail': 'no parameters to query by'})

            try:
                # get entire object in case parent has to be included also (include_parent)
                directory = Directory.objects.get(**params)
            except Directory.DoesNotExist:
                raise Http404
            directory_id = directory.id

        if cr_identifier:
            if cr_identifier.isdigit():
                cr = CatalogRecord.objects.values('id', '_directory_data').get(pk=cr_identifier)
            else:
                try:
                    cr = CatalogRecord.objects.values('id', '_directory_data').get(identifier=cr_identifier)
                except CatalogRecord.DoesNotExist:
                    # raise 400 instead of 404, to distinguish from the error
                    # 'directory not found', which raises a 404
                    raise ValidationError({
                        'detail': [ 'CatalogRecord with identifier %s does not exist' % cr_identifier ]
                    })
            cr_id = cr['id']
            cr_directory_data = cr['_directory_data'] or {}
        else:
            cr_id = None
            cr_directory_data = {}

        # note: by default all fields are retrieved
        directory_fields, file_fields = cls._get_requested_file_browsing_fields(request, cr_id)

        contents = cls._get_directory_contents(
            directory_id,
            recursive=recursive,
            max_depth=max_depth,
            dirs_only=dirs_only,
            cr_id=cr_id,
            directory_fields=directory_fields,
            file_fields=file_fields
        )

        if recursive:
            if dirs_only:
                # taken care of the in the called methods. can return the result as is
                # as a directory tree
                pass
            else:
                # create a flat file list of the contents
                file_list = []
                file_list_append = file_list.append
                cls._form_file_list(contents, file_list_append)
                return file_list

        if include_parent:
            contents.update(DirectorySerializer(directory, only_fields=directory_fields).data)

        if cls._include_total_byte_sizes_and_file_counts(cr_id, directory_fields):
            cls.retrieve_directory_byte_sizes_and_file_counts_for_cr(contents, cr_id,
                directory_fields, cr_directory_data)

        return contents

    @classmethod
    def _get_requested_file_browsing_fields(cls, request, cr_id):
        """
        Find out if only specific fields were requested to be returned, and return those fields
        for directories and files respectively.
        """
        directory_fields = []
        file_fields = []

        if request.query_params.get('directory_fields', False):
            directory_fields = request.query_params['directory_fields'].split(',')

        if request.query_params.get('file_fields', False):
            file_fields = request.query_params['file_fields'].split(',')

        return directory_fields, file_fields

    @staticmethod
    def _include_total_byte_sizes_and_file_counts(cr_id, directory_fields):
        if not cr_id:
            # totals are counted only when browsing files for a specific record.
            # for non-cr file browsing, those numbers have been counted when the
            # files were first created.
            return False
        if not directory_fields:
            # specific fields not specified -> all fields are returned
            return True
        if 'byte_size' in directory_fields or 'file_count' in directory_fields:
            return True
        return False

    @classmethod
    def _form_file_list(cls, contents, file_list_append):
        for f in contents.get('files', []):
            file_list_append(f)
        for d in contents.get('directories', []):
            cls._form_file_list(d, file_list_append)

    @classmethod
    def _get_directory_contents(cls, directory_id, recursive=False, max_depth=1, depth=0, dirs_only=False,
            cr_id=None, directory_fields=[], file_fields=[]):
        """
        Get files and directories contained by a directory.

        If recursively requested, collects all files and dirs below the directory.

        If cr_id is provided, only those files and directories are retrieved, which have been
        selected for that CatalogRecord.

        If directory_fields and/or file_fields are specified, then only specified fields are retrieved
        for directories and files respectively.
        """
        if recursive and max_depth != '*':
            if depth > max_depth:
                raise MaxRecursionDepthExceeded('max depth is %d' % max_depth)
            depth += 1

        if cr_id:
            try:
                dirs, files = cls._get_directory_contents_for_catalog_record(directory_id, cr_id,
                    dirs_only=dirs_only, directory_fields=directory_fields, file_fields=file_fields)
            except Http404:
                if recursive:
                    return { 'directories': [] }
                raise
        else:
            # browsing from ALL files, not cr specific

            dirs = Directory.objects.filter(parent_directory_id=directory_id).only(*directory_fields)

            if dirs_only:
                files = None
            else:
                files = File.objects.filter(parent_directory_id=directory_id).only(*file_fields)

        try:
            contents = { 'directories': [ DirectorySerializer(n, only_fields=directory_fields).data for n in dirs ] }
        except FieldDoesNotExist as e:
            raise Http400({ 'detail': [str(e)]})

        try:
            # note: the below statement already executes the queryset, hence this block inside try-except
            if files or not dirs_only:
                # for normal file browsing (not with 'dirs_only'), the files-key should be present,
                # even if empty.
                contents['files'] = [ FileSerializer(n, only_fields=file_fields).data for n in files ]
        except FieldDoesNotExist as e:
            raise Http400({ 'detail': [str(e)]})

        if recursive:
            for directory in contents['directories']:
                try:
                    sub_dir_contents = cls._get_directory_contents(
                        directory['id'],
                        recursive=recursive,
                        max_depth=max_depth,
                        depth=depth,
                        dirs_only=dirs_only,
                        cr_id=cr_id,
                        directory_fields=directory_fields,
                        file_fields=file_fields
                    )
                except MaxRecursionDepthExceeded:
                    continue

                directory['directories'] = sub_dir_contents['directories']
                if 'files' in sub_dir_contents:
                    directory['files'] = sub_dir_contents['files']

        return contents

    def _get_directory_contents_for_catalog_record(directory_id, cr_id, dirs_only=False,
            directory_fields=[], file_fields=[]):
        """
        Browsing files in the context of a specific CR id.
        """

        # select dirs which are contained by the directory,
        # AND which contain files belonging to the cr <-> files m2m relation table,
        # AND there exists files for CR which beging with the same path as the dir path,
        # to successfully also include files which did not DIRECTLY contain any files, but do
        # contain files further down the tree.
        #
        # dirs which otherwise contained files, but not any files that were selected for
        # the cr, are not returned.
        sql_select_dirs_for_cr = """
            select d.id
            from metax_api_directory d
            where d.parent_directory_id = %s
            and exists(
                select 1
                from metax_api_file f
                inner join metax_api_catalogrecord_files cr_f on cr_f.file_id = f.id
                where f.file_path like (d.directory_path || '/%%')
                and cr_f.catalogrecord_id = %s
                and f.removed = false
                and f.active = true
            )
            """

        # select files which are contained by the directory, and which
        # belong to the cr <-> files m2m relation table
        sql_select_files_for_cr = """
            select f.id
            from metax_api_file f
            inner join metax_api_catalogrecord_files cr_f on cr_f.file_id = f.id
            where f.parent_directory_id = %s
            and cr_f.catalogrecord_id = %s
            and f.removed = false
            and f.active = true
            """

        with connection.cursor() as cr:
            cr.execute(sql_select_dirs_for_cr, [directory_id, cr_id])
            directory_ids = [ row[0] for row in cr.fetchall() ]

            if dirs_only:
                file_ids = []
            else:
                cr.execute(sql_select_files_for_cr, [directory_id, cr_id])
                file_ids = [ row[0] for row in cr.fetchall() ]

        if not directory_ids and not file_ids:
            # for this specific version of the record, the requested directory either
            # didnt exist, or it was not selected
            raise Http404

        dirs = Directory.objects.filter(id__in=directory_ids).only(*directory_fields)
        files = None if dirs_only else File.objects.filter(id__in=file_ids).only(*file_fields)

        return dirs, files

    @classmethod
    def retrieve_directory_byte_sizes_and_file_counts_for_cr(cls, directory, cr_id,
            directory_fields=[], cr_directory_data={}):
        """
        Retrieve total byte size and file counts of a directory, sub-directories included,
        in the context of a specific catalog record.
        Note: Called recursively.
        """
        if not directory_fields:
            BYTE_SIZE = FILE_COUNT = True
        else:
            BYTE_SIZE = 'byte_size' in directory_fields
            FILE_COUNT = 'file_count' in directory_fields

        if len(directory.get('directories', [])):
            for sub_dir in directory.get('directories', []):
                cls.retrieve_directory_byte_sizes_and_file_counts_for_cr(sub_dir, cr_id,
                    directory_fields, cr_directory_data)

            # this block is not executed for the top-level directory, unless query param
            # include_parent is used.
            if 'id' in directory:

                current_dir = cr_directory_data.get(str(directory['id']), [0, 0])

                if BYTE_SIZE:
                    directory['byte_size'] = current_dir[0]

                if FILE_COUNT:
                    directory['file_count'] = current_dir[1]

        elif 'id' in directory:

            # bottom dir - retrieve total byte_size and file_count for this cr
            current_dir = cr_directory_data.get(str(directory['id']), [0, 0])

            if BYTE_SIZE:
                directory['byte_size'] = current_dir[0]

            if FILE_COUNT:
                directory['file_count'] = current_dir[1]

    @classmethod
    def get_project_root_directory(cls, project_identifier):
        """
        Return root directory for a project, with its child directories and files.
        """
        try:
            root_dir = Directory.objects.get(
                project_identifier=project_identifier, parent_directory=None
            )
        except Directory.DoesNotExist:
            raise Http404
        except Directory.MultipleObjectsReturned: # pragma: no cover
            raise Exception(
                'Directory.MultipleObjectsReturned when looking for root directory. This should never happen')

        root_dir_json = DirectorySerializer(root_dir).data
        root_dir_json.update(cls._get_directory_contents(root_dir.id))
        return root_dir_json

    @classmethod
    def _create_single(cls, common_info, initial_data, serializer_class, **kwargs):
        """
        Override the original _create_single from CommonService to also create directories,
        and setting them as parent_directory to approriate dirs and the file, before creating.
        """
        _logger.info('Begin create single file')

        cls._check_errors_before_creating_dirs([initial_data])

        # the same initial data as received, except it has parent_directory set also
        initial_data_with_dirs = cls._create_directories_from_file_list(common_info, [initial_data], **kwargs)

        res = super(FileService, cls)._create_single(
            common_info, initial_data_with_dirs[0], serializer_class, **kwargs)

        cls.calculate_project_directory_byte_sizes_and_file_counts(initial_data['project_identifier'])

        _logger.info('Created 1 new files')
        return res

    @classmethod
    def _create_bulk(cls, common_info, initial_data_list, results, serializer_class, **kwargs):
        """
        Override the original _create_bulk from CommonService to also create directories,
        and setting them as parent_directory to approriate files, before creating the files.
        """
        _logger.info('Begin bulk create files')

        cls._check_errors_before_creating_dirs(initial_data_list)
        file_list_with_dirs = cls._create_directories_from_file_list(common_info, initial_data_list, **kwargs)

        _logger.info('Creating files...')

        cls._create_files(
            common_info, file_list_with_dirs, results, serializer_class, **kwargs)

        cls.calculate_project_directory_byte_sizes_and_file_counts(initial_data_list[0]['project_identifier'])

        _logger.info('Created %d new files' % len(results.get('success', [])))

    @classmethod
    def _create_files(cls, common_info, initial_data_list, results, serializer_class, **kwargs):
        """
        The actual part where the list is iterated and objects validated, and created.
        """

        # pre-fetch all file_paths in the project, to spare an individual db fetch for each file
        # in serializer.save(), where they otherwise would check for path presence.
        project_file_paths = File.objects.filter(
            project_identifier=initial_data_list[0]['project_identifier']).values_list('file_path', flat=True)
        project_file_paths = set(project_file_paths)

        file_storage_id = None
        entries = []

        def to_model_format(entry, common_info):
            """
            Format that is inserted into db.
            """
            del entry['checksum']
            entry['file_storage_id'] = entry['file_storage']
            del entry['file_storage']
            entry['parent_directory_id'] = entry['parent_directory']
            del entry['parent_directory']
            entry.update(**common_info) # add date_created, service_created etc fields

        def to_repr(entry, common_info):
            """
            Format that is returned in the response.
            """
            entry['file_storage'] = { 'id': entry['file_storage_id'] }
            entry['parent_directory'] = { 'id': entry['parent_directory_id'] }
            del entry['file_storage_id']
            del entry['parent_directory_id']
            for field in common_info.keys():
                # cast datetime objects into strings
                entry[field] = str(entry[field])

        if DEBUG:
            start = time()

        for i, row in enumerate(initial_data_list):

            if file_storage_id:
                # saves a fetch to db in serializer.is_valid(), once file_storage_id has been retrieved
                # for one of the files.
                row['file_storage'] = file_storage_id

            serializer = serializer_class(data=row, **kwargs)

            if row['file_path'] not in project_file_paths:
                # saves a fetch to db in serializer.is_valid()
                serializer.file_path_checked = True
            else:
                # looks like path already exists in the project scope... let the serializer
                # confirm it on its own, and raise an error along the standard validations.
                # if the identifier also already exists, the error may be ignored if parameter
                # ignore_already_exists_errors is used.
                pass

            try:
                serializer.is_valid(raise_exception=True)
            except Exception as e:
                if CommonService.get_boolean_query_param(kwargs['context']['request'], 'ignore_already_exists_errors'):
                    if cls._error_is_already_exists(e):
                        # add only a minuscule response informing of the situation...
                        results['success'].append({ 'object': {
                            'identifier': row['identifier'], 'detail': ['already exists'] }
                        })
                        continue
                cls._append_error(results, serializer, e)
            else:
                entry = serializer.initial_data
                to_model_format(entry, common_info)
                entries.append(File(**entry))
                file_storage_id = entry['file_storage_id'] # re-used for following loops

                to_repr(entry, common_info)
                results['success'].append({ 'object': entry })

                if i % 1000 == 0:
                    # pros and cons of Model.objects.bulk_create():
                    # pros:
                    # - less db calls... MUCH faster
                    # cons:
                    # - Model.save() is not called. luckily File does not implement anything custom there
                    # - uses a bit more memory since entries[] is accumulated. control by adjusting batch size
                    # - the values returned to the requestor do not look identical to serializer.data. currently
                    #   low impact though, as no service is inspecting it anyway.
                    File.objects.bulk_create(entries)
                    entries = []

                    # for large amounts of data, the process slows down considerably as the process continues...
                    # closing and re-opening the connection helps the speed stay constant. downside: unable
                    # to create files inside a transaction. for rare cases where retries are made
                    # with the same data where the requestor never got the previousy attempt's result back,
                    # the requestor can use the parameter ignore_already_exists_errors to be spared from
                    # uninteresting errors.
                    if not connection.in_atomic_block:
                        # do not close when in atomic block! should only happen when inside test cases
                        connection.close()
                        connection.connect()

                    if DEBUG:
                        end = time()
                        _logger.debug('processed %d files... (%.3f seconds per batch)' % (i, end - start))
                        start = time()

        if entries:
            _logger.debug('a final dose of %d records still left to bulk_create...' % len(entries))
            File.objects.bulk_create(entries)
            _logger.debug('done!')

        if DEBUG:
            end = time()
            _logger.debug('total time for inserting %d files: %d seconds' % (len(initial_data_list), (end - start)))

    @staticmethod
    def _error_is_already_exists(e):
        """
        Check if the error 'identifier already exists' was raised. There may have been other errors
        included, but they may be a symptom of the record already existing, so we don't care about them.
        """
        if hasattr(e, 'detail'):
            for field_name, errors in e.detail.items():
                if field_name == 'identifier' and 'already exists' in errors[0]:
                    return True
        return False

    @staticmethod
    def _check_errors_before_creating_dirs(initial_data_list):
        """
        Check for errors concerning these fields, since creating directories counts on them.
        FileSerializer is_valid() is called a lot later for more thorough error checking.

        Errors here would realistically be an extremely rare case of error from the requestor's side,
        but doing the minimum necessary here anyway since we can't count on FileSerializer.is_valid()
        """
        for row in initial_data_list:

            if 'file_path' not in row:
                raise Http400({
                    'file_path': ['file_path is a required parameter (file id: %s)' % row['identifier']]
                })

            if 'project_identifier' not in row:
                raise Http400({
                    'project_identifier': [
                        'project_identifier is a required parameter (file id: %s)' % row['identifier']
                    ]
                })

        if len(set(f['project_identifier'] for f in initial_data_list)) > 1:
            raise Http400({
                'project_identifier': ['creating files for multiple projects in one request is not permitted.']
            })

    @classmethod
    def _create_directories_from_file_list(cls, common_info, initial_data_list, **kwargs):
        """
        IDA does not give metax information about directories associated with the files
        as separate entities in the request, so they have to be created based on the
        paths in the list of files.
        """
        _logger.info('Creating and checking file hierarchy...')

        project_identifier = initial_data_list[0]['project_identifier']
        sorted_data = sorted(initial_data_list, key=lambda row: row['file_path'])
        received_dir_paths = sorted(set(dirname(f['file_path']) for f in sorted_data))

        new_dir_paths, existing_dirs = cls._get_new_and_existing_dirs(received_dir_paths, project_identifier)

        if new_dir_paths:
            cls._create_directories(common_info, existing_dirs, new_dir_paths, project_identifier, **kwargs)

        cls._assign_parents_to_files(existing_dirs, sorted_data)

        _logger.info('Directory hierarchy in place')
        return sorted_data

    @classmethod
    def _get_new_and_existing_dirs(cls, received_dir_paths, project_identifier):
        """
        From the received file paths list, find out which directories already exist in the db,
        and which need to be created.
        """
        received_dir_paths = cls._get_unique_dir_paths(received_dir_paths)

        # get existing dirs as a dict of { 'directory_path': 'id' }
        existing_dirs = dict(
            (dr.directory_path, dr.id) for dr in
            Directory.objects.filter(
                directory_path__in=received_dir_paths,
                project_identifier=project_identifier
            ).order_by('directory_path')
        )

        new_dir_paths = [ path for path in received_dir_paths if path not in existing_dirs ]
        _logger.info('Found %d existing, %d new directory paths' % (len(existing_dirs), len(new_dir_paths)))
        return new_dir_paths, existing_dirs

    def _get_unique_dir_paths(received_dir_paths):
        """
        Those dirs that are only used to organize other dirs, i.e. does not contain files actually,
        need to be weeded out from the list of received dir paths, to get all required directories
        that need to be created.
        """
        all_paths = { '/': True }

        for path in received_dir_paths:
            parent_path = dirname(path)
            while parent_path != '/':
                if parent_path not in all_paths:
                    all_paths[parent_path] = True
                parent_path = dirname(parent_path)
            all_paths[path] = True

        return sorted(all_paths.keys())

    @classmethod
    def _create_directories(cls, common_info, existing_dirs, new_dir_paths, project_identifier, **kwargs):
        """
        Create to db directory hierarchy from directories extracted from the received file list.

        Save created paths/id's to existing_dirs dict, so the results can be efficiently re-used
        by other dirs being created, and later by the files that are created.

        It is possible, that no new directories are created at all, when appending files to an
        existing dir.
        """
        _logger.info('Creating directories...')
        python_process_pid = str(getpid())

        for i, path in enumerate(new_dir_paths):

            directory = {
                'directory_path': path,
                'directory_name': basename(path) if path != '/' else '/',
                # identifier: uuid3 as hex, using as salt time in ms, idx of loop, and python process id
                'identifier': uuid3(UUID_NAMESPACE_DNS, '%d%d%s'
                                    % (int(round(time() * 1000)), i, python_process_pid)).hex,
                'project_identifier': project_identifier
            }

            directory.update(common_info)

            if path != '/':
                cls._find_parent_dir_from_previously_created_dirs(directory, existing_dirs)

            serializer = DirectorySerializer(data=directory, **kwargs)
            serializer.is_valid(raise_exception=True)
            serializer.save()
            existing_dirs[serializer.data['directory_path']] = serializer.data['id']

    @classmethod
    def _assign_parents_to_files(cls, existing_dirs, sorted_data):
        """
        Using the previously created dirs, assign parent_directory to
        each file in the received file list.

        Assigning parent_directory is not allowed for requestors, so all existing
        parent_directories in the received files are purged.
        """
        _logger.info('Assigning parent directories to files...')

        for row in sorted_data:
            row.pop('parent_directory', None)

        for row in sorted_data:
            cls._find_parent_dir_from_previously_created_dirs(row, existing_dirs)

        return sorted_data

    @staticmethod
    def _find_parent_dir_from_previously_created_dirs(node, existing_dirs):
        """
        Parameter existing_dirs contains key-values as (directory_path, id).

        Find the approriate directory id to use as parent_directory, using the node's
        dirname(file_path) or dirname(directory_path) as key.
        """
        node_path = node.get('file_path', node.get('directory_path', None))

        if node_path == '/':
            return

        expected_parent_dir_path = dirname(node_path)

        try:
            node['parent_directory'] = existing_dirs[expected_parent_dir_path]
        except KeyError: # pragma: no cover
            raise Exception(
                'No parent found for path %s, even though existing_dirs had stuff '
                'in it. This should never happen' % node.get('file_path', node.get('directory_path', None))
            )

    @staticmethod
    def calculate_project_directory_byte_sizes_and_file_counts(project_identifier):
        """
        (Re-)calculate directory byte sizes and file counts in this project.
        """
        try:
            project_root_dir = Directory.objects.get(project_identifier=project_identifier, parent_directory_id=None)
        except Directory.DoesNotExist:
            # root directory does not exist - all project files have been deleted
            pass
        else:
            project_root_dir.calculate_byte_size_and_file_count()
