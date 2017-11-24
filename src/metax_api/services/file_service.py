import logging
from os import getpid
from os.path import dirname, basename
from time import time
from uuid import uuid3, NAMESPACE_DNS as UUID_NAMESPACE_DNS

from django.db import connection
from django.http import Http404
from rest_framework import status
from rest_framework.response import Response
from rest_framework.serializers import ValidationError

from metax_api.exceptions import Http400
from metax_api.models import CatalogRecord, Directory, File
from metax_api.utils import RabbitMQ
from .common_service import CommonService

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
    from metax_api.api.base.serializers import CatalogRecordSerializer as CRS
    CatalogRecordSerializer = CRS
    return CatalogRecordSerializer(*args, **kwargs)

def DirectorySerializer(*args, **kwargs):
    from metax_api.api.base.serializers import DirectorySerializer as DS
    DirectorySerializer = DS
    return DirectorySerializer(*args, **kwargs)

def FileSerializer(*args, **kwargs):
    from metax_api.api.base.serializers import FileSerializer as FS
    FileSerializer = FS
    return FileSerializer(*args, **kwargs)


class FileService(CommonService):

    @classmethod
    def get_datasets_where_file_belongs_to(cls, file_identifiers):
        """
        Find out which (non-deprecated) datasets a list of files belongs to, and return
        their urn_identifiers as a list. Includes only latest versions of datasets.

        Parameter file_identifiers can be a list of pk's (integers), or file identifiers (strings).
        """
        _logger.info('Retrieving list of datasets where files belong to')

        if not isinstance(file_identifiers, list):
            raise Http400('identifiers must be passed as a list')

        _logger.info('Looking datasets for the following files (printing first 10):\n%s' % '\n'.join(str(id) for id in file_identifiers[:10]))

        file_ids = cls._file_identifiers_to_ids(file_identifiers)

        if not file_ids:
            raise Http404

        sql_select_related_records = """
            select research_dataset->>'urn_identifier' as urn_identifier
            from metax_api_catalogrecord cr
            inner join metax_api_catalogrecord_files cr_f on catalogrecord_id = cr.id
            where cr_f.file_id in %s and cr.removed = false and cr.active = true
            group by urn_identifier
            """

        with connection.cursor() as cr:
            cr.execute(sql_select_related_records, [tuple(file_ids)])
            if cr.rowcount == 0:
                urn_identifiers = []
                _logger.info('No datasets found for files')
            else:
                urn_identifiers = [ row[0] for row in cr.fetchall() ]
                _logger.info('Found following datasets:\n%s' % '\n'.join(urn_identifiers))

        return Response(urn_identifiers, status=status.HTTP_200_OK)

    @classmethod
    def destroy_bulk(cls, file_identifiers):
        """
        Mark files as deleted en masse. Parameter file_identifiers can be a list of pk's
        (integers), or file identifiers (strings).

        Currently the assumption is that a bulk delete request will always contain ALL the files
        in a directory, and its sub-directories, so the end result after all the operations should
        be that the top-dir found in a list of files should effectively be completely cut out.

        By far the easiest solution would have been to just find the top file_path, and delete all
        files and dirs within the project scope that begin with said path, but it was recommended
        to operate explicitly on the given file identifiers to minimize the opportunity for any
        unforeseen accidents.

        Since metax has to make due with working only with lists of the files themselves and
        their file paths instead of being given the related directories with their unique
        identifiers as well, this method goes a decent length to protect the file hierarchy from
        ending up in any kind of inconsistent state, mostly culminating in making sure that
        - there will not be any directories or files without parent directories
          (a project can only have one directory without a parent, which is the root
          /project_x_FROZEN directory. This method will delete the root as well, though,
          when approriate).
        - any directory being deleted does not have files in it (directories are deleted
          after deleting the specified files, so any files still found in the directories
          implies there is a de-sync between IDA and metax)
        - there will not be any "orphaned" directory chains (with no files in them) between
          the top and the bottom of the file hierarchy

        Any evidence of the previous scenarios will raise an error.

        The method returns a http response with the number of deleted files in the body.
        """
        _logger.info('Begin bulk delete files')

        file_ids = cls._file_identifiers_to_ids(file_identifiers)

        deleted_files_count, project_identifier = cls._mark_files_as_deleted(file_ids)

        parent_of_top_dir = cls._delete_dirs_of_deleted_files(file_ids)

        cls._find_and_delete_empy_dir_chains(project_identifier, parent_of_top_dir)

        cls._mark_datasets_as_deprecated(file_ids)

        _logger.info('Marked %d files as deleted from project %s' % (deleted_files_count, project_identifier))
        return Response({ 'deleted_files_count': deleted_files_count }, status=status.HTTP_200_OK)

    @staticmethod
    def _file_identifiers_to_ids(file_identifiers):
        """
        In case file_identifiers is identifiers (strings), which they probably are in real use,
        do a query to get a list of pk's instead, since they will be used quite a few times.
        """
        if not file_identifiers:
            _logger.info('Received empty list of identifiers. Aborting')
            raise Http400('Received empty list of identifiers')
        elif isinstance(file_identifiers[0], int):
            file_ids = file_identifiers
        else:
            file_ids = File.objects.filter(identifier__in=file_identifiers).values_list('id', flat=True)
        return file_ids

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
    def _delete_dirs_of_deleted_files(cls, file_ids):
        """
        Gather ids of all directories that were parents of the files marked as removed,
        and permanently delete them.
        """
        _logger.info('Deleting directories of deleted files...')

        # get all parent directories of deleted files, and check that they are all really empty
        dirs_of_deleted_files = Directory.objects.filter(files__in=file_ids)

        try:
            # find the top-most directory, so that any possible empty directory chains
            # above it can be found and deleted later
            parent_of_top_dir = dirs_of_deleted_files[0].parent_directory
        except: # pragma: no cover
            raise ValidationError({
                'detail': ['Could not find any directories associated with the deleted files... This should not happen']
            })

        for dr in dirs_of_deleted_files:
            if dr.files.exists():
                cls._raise_incomplete_bulk_delete_file_list_error(dr)
            else:
                dr.delete()

        return parent_of_top_dir

    @classmethod
    def _find_and_delete_empy_dir_chains(cls, project_identifier, parent_of_top_dir):
        """
        Make sure there will not be any "orphaned" directory chains
        (directories which only contained other directories for organizing data), and other
        empty directories "higher up" than what the first received file points to, to not
        leave any trash laying around that could cause trouble later.
        """
        _logger.info('Finding and deleting empty directory chains...')

        for dr in Directory.objects.filter(project_identifier=project_identifier, parent_directory_id=None):
            if len(dirname(dr.directory_path)) <= 1:
                # dont delete a root directory such as /project_x_FROZEN, since that
                # is also included by the query.
                continue
            cls._delete_empy_dir_chain_below(dr)

        # finally, take the root directory of the original file list, and delete
        # any empty directory chains above
        cls._delete_empy_dir_chain_above(parent_of_top_dir)

    @classmethod
    def _delete_empy_dir_chain_above(cls, directory):
        """
        If the highest path from the files being deleted was e.g. /some/path/here/file.png,
        then /some/path/here would be the closest dir that has now been deleted, since it was
        the parent of file.png. It is possible that other directories above it are empty, for
        example in the case that the dir that was chosen for unfreezing/deletion, was some dir
        higher up, but only contained directories leading up to the first file.

        Delete all empty directories above the first parent directory to not leave behind
        any clutter. It is possible that all directories including the root will get deleted.
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
    def _delete_empy_dir_chain_below(cls, dr):
        """
        Delete possibly empty directory chains that might have been left over after
        deleting directories directly associated with files (i.e., delete directory
        hierarchies that existed solely for organizing other dirs, but did not contain
        files themselves).
        """
        if dr.files.exists():
            cls._raise_incomplete_bulk_delete_file_list_error(dr)
        else:
            # start deleting directories from the bottom to up
            for sub_dr in dr.child_directories.all():
                cls._delete_empy_dir_chain_below(sub_dr)
            dr.delete()

    @staticmethod
    def _mark_datasets_as_deprecated(file_ids):
        """
        Get all CatalogRecords which have files set to them from file_ids,
        and set their deprecated flag to True. Then, publish update-messages to rabbitmq.
        """
        _logger.info('Marking related datasets as deprecated...')
        deprecated_records = []

        for cr in CatalogRecord.objects.filter(files__in=file_ids, deprecated=False):
            cr.deprecated = True
            cr.save()
            deprecated_records.append(CatalogRecordSerializer(cr).data)

        _logger.info('Publishing deprecated datasets to rabbitmq update queues...')
        rabbitmq = RabbitMQ()
        rabbitmq.publish(deprecated_records, routing_key='update', exchange='datasets')

    @staticmethod
    def _raise_incomplete_bulk_delete_file_list_error(directory):
        """
        Log and raise an error with details about what files what were left over in the directories,
        after attempting to delete files according to received list of file ids.
        """
        error_msg = 'The received file list is incomplete. Not all files were deleted from ' \
            'a directory. When bulk deleting files, make sure to include all files in sub-directories ' \
            'as well. See file_list for list of files left over.'

        file_list = [ f.file_path for f in directory.files.all() ]

        _logger.error('%s file_list:\n%s' % (error_msg, '\n'.join(file_list)))

        raise Http400({
            'detail': [error_msg],
            'file_list': file_list
        })

    @classmethod
    def get_directory_contents(cls, identifier, recursive=False):
        """
        Get files and directories contained by a directory. Parameter 'identifier'
        may be a pk, or an uuid value. Search using approriate fields.

        Parameter 'recursive' may be used to get a flat list of all files below the
        directory. Sorted by file_path for convenience
        """
        if identifier.isdigit():
            directory_id = identifier
        else:
            directory = Directory.objects.filter(identifier=identifier).values('id').first()
            if not directory:
                raise Http404
            directory_id = directory['id']

        contents = cls._get_directory_contents(directory_id, recursive=recursive)

        if recursive:
            file_list = []
            file_list_append = file_list.append
            cls._form_file_list(contents, file_list_append)
            return file_list
        else:
            return contents

    @classmethod
    def _form_file_list(cls, contents, file_list_append):
        for f in contents['files']:
            file_list_append(f)
        for d in contents['directories']:
            cls._form_file_list(d, file_list_append)

    @classmethod
    def _get_directory_contents(cls, directory_id, recursive=False):
        """
        Get files and directories contained by a directory. If recursively requested,
        returns a flat list of all files below the directory.
        """
        dirs = Directory.objects.filter(parent_directory_id=directory_id)
        files = File.objects.filter(parent_directory_id=directory_id)

        contents = {
            'directories': [ DirectorySerializer(n).data for n in dirs ],
            'files': [ FileSerializer(n).data for n in files ]
        }

        if recursive:
            for directory in contents['directories']:
                sub_dir_contents = cls._get_directory_contents(directory['id'], recursive=recursive)
                directory['directories'] = sub_dir_contents['directories']
                directory['files'] = sub_dir_contents['files']

        return contents

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
    def _create_bulk(cls, common_info, initial_data_list, results, serializer_class, **kwargs):
        """
        Override the original _create_bulk from CommonService to also create directories,
        and setting them as parent_directory to approriate files, before creating the files.
        """
        file_list_with_dirs = cls._create_directories_from_file_list(common_info, initial_data_list, **kwargs)
        return super(FileService, cls)._create_bulk(
            common_info, file_list_with_dirs, results, serializer_class, **kwargs)

    @classmethod
    def _create_directories_from_file_list(cls, common_info, initial_data_list, **kwargs):
        """
        IDA does not give metax information about directories associated with the files
        as separate entities in the request, so they have to be created based on the
        paths in the list of files.
        """
        cls._check_errors_before_creating_dirs(initial_data_list)

        # all required dirs that are related to anything (files and other directories) during
        # this create-request, will be placed in the below dict for quick access.
        created_dirs = {}

        project_identifier = initial_data_list[0]['project_identifier']
        sorted_data = sorted(initial_data_list, key=lambda row: row['file_path'])
        unique_dir_paths = sorted(set(dirname(f['file_path']) for f in sorted_data))

        root_dir = cls._check_if_parent_already_exists(unique_dir_paths[0], project_identifier)

        if root_dir:
            # place the previously created (in some previous request) root_dir in created_dirs,
            # to make it available to other dirs and files as part of the normal process
            created_dirs[root_dir['directory_path']] = root_dir['id']
        else:
            cls._create_leading_dirs(created_dirs, unique_dir_paths, project_identifier)

        cls._create_directories(common_info, created_dirs, unique_dir_paths, project_identifier, **kwargs)

        cls._assign_parents_to_files(created_dirs, sorted_data)

        return sorted_data

    @staticmethod
    def _check_errors_before_creating_dirs(initial_data_list):
        """
        Check for errors concerning these fields, since creating directories counts on them.
        FileSerializer is_valid() is called a lot later for more thorough error checking.

        Errors here would realistically be an extremely rare case of error from the requestor's side,
        but doing the minimum necessary here anyway since we can't count on FileSerializer.is_valid()
        """
        errors = {}
        for row in initial_data_list:
            if 'file_path' not in row:
                errors['file_path'] = ['file_path is a required parameter (file id: %s)' % row['identifier']]
            if 'project_identifier' not in row:
                errors['project_identifier'] = ['project_identifier is a required parameter (file id: %s)'
                                                % row['identifier']]

            if errors:
                raise Http400(errors)

    @classmethod
    def _create_leading_dirs(cls, created_dirs, unique_dir_paths, project_identifier):
        """
        In case there was no previously existing root the new directories
        could be attached to...

        The first frozen file could have been something like /some/path/here/file.png,
        so the top dir in unique_dir_paths gathered would be /some/path/here.
        For a proper file hierarchy, directories /some/path and /some has to
        be additionally created.

        It is possible however, that the first file is something like /some/other/path/file.png,
        where /some would already exist. Therefore make sure to check for existing directories
        after each created directory.
        """
        emergency_break = 1000
        upper_dir = dirname(unique_dir_paths[0])
        while len(upper_dir) > 1:
            unique_dir_paths.insert(0, upper_dir)

            # it is possible we only had to create one or some dirs in the middle, to find an
            # existing parent dir higher up. check it
            root_dir = cls._check_if_parent_already_exists(upper_dir, project_identifier)
            if root_dir:
                created_dirs[root_dir['directory_path']] = root_dir['id']
                return

            if emergency_break < 0: # pragma: no cover
                raise Exception('emergency_break reached while creating leading dirs, should (probably) never happen..')

            emergency_break -= 1

            upper_dir = dirname(upper_dir)

    @classmethod
    def _create_directories(cls, common_info, created_dirs, unique_dir_paths, project_identifier, **kwargs):
        """
        Create to db directory hierarchy from directories extracted from the received file list.

        Save created paths/id's to created_dirs dict, so the results can be efficiently re-used
        by other dirs being created, and later by the files that are created.
        """
        python_process_pid = str(getpid())

        for i, path in enumerate(unique_dir_paths):

            directory = {
                'directory_path': path,
                'directory_name': basename(path),
                # identifier: uuid3 as hex, using as salt time in ms, idx of loop, and python process id
                'identifier': uuid3(UUID_NAMESPACE_DNS, '%d%d%s'
                                    % (int(round(time() * 1000)), i, python_process_pid)).hex,
                'project_identifier': project_identifier
            }

            directory.update(common_info)

            cls._find_parent_dir_from_previously_created_dirs(directory, created_dirs)

            serializer = DirectorySerializer(data=directory, **kwargs)
            serializer.is_valid()

            if serializer.errors:
                raise ValidationError(serializer.errors)

            serializer.save()
            created_dirs[serializer.data['directory_path']] = serializer.data['id']

    @classmethod
    def _assign_parents_to_files(cls, created_dirs, sorted_data):
        """
        Using the previously created dirs and files, assign parent_directory to
        each file in the received file list.

        Since the files are sorted by file_path, there is a very good chance that files
        can use the previous file's parent_directory directly, instead of having to
        look it up from created_dirs.
        """

        # assigning parent_directory is not allowed for user. ensure all parent_directories
        # are purged before proceeding. in reality popping the key from sorted_data[-1] would
        # be enough for the first loop, but lets not invite disaster.
        for row in sorted_data:
            row.pop('parent_directory', None)

        for i, row in enumerate(sorted_data):

            previous_file = sorted_data[i - 1]

            if 'parent_directory' in previous_file and dirname(row['file_path']) == dirname(previous_file['file_path']):
                row['parent_directory'] = previous_file['parent_directory']
            else:
                cls._find_parent_dir_from_previously_created_dirs(row, created_dirs)

        return sorted_data

    @staticmethod
    def _find_parent_dir_from_previously_created_dirs(node, created_dirs):
        """
        Parameter created_dirs contains key-values as (directory_path, id).

        Find the approriate directory id to use as parent_directory, using the node's
        dirname(file_path) or dirname(directory_path) as key.
        """
        if not created_dirs:
            return

        expected_parent_dir_path = dirname(node.get('file_path', node.get('directory_path', None)))
        node['parent_directory'] = created_dirs.get(expected_parent_dir_path, None)

        if not node['parent_directory']: # pragma: no cover
            raise Exception(
                'No parent found for path %s, even though created_dirs had stuff '
                'in it. This should never happen' % node.get('file_path', node.get('directory_path', None))
            )

    @staticmethod
    def _check_if_parent_already_exists(directory_path, project_identifier):
        """
        Check if there already exists an applicable root directory for the first dir in
        the sorted list of directories. The case is conceivable when someone had previously
        frozen for example path /some/path/here, and then later /some/other/path, so
        /some would already exist.

        This check only needs to be made for the first dir, once the list is sorted. If there
        existed some common parent higher up, it is checked for later.

        Note: This method does not check if the targeted directory ITSELF already exists,
        in the sense that a path /some/path/mydir was frozen twice, and on the second time
        the files would simply be appended to it. That will result in an error 'directory already
        exists in project scope'. Currently that scenario shouldn't be possible, so it isn't
        supported.
        """
        parent_dir_path = dirname(directory_path)

        if len(parent_dir_path) == 1:
            # -> dir is something like /my_project_FROZEN,
            # which cant have a parent
            return None

        try:
            parent_dir = Directory.objects.get(project_identifier=project_identifier, directory_path=parent_dir_path)
        except Directory.DoesNotExist:
            return None
        except Directory.MultipleObjectsReturned:
            raise ValidationError({ 'parent_directory': [
                'multiple directories found when looking for parent (looked for path: %s)' % parent_dir_path]} )

        return { 'directory_path': parent_dir.directory_path, 'id': parent_dir.id }
