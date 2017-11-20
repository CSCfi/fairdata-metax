from os import getpid
from os.path import dirname, basename
from time import time
from uuid import uuid3, NAMESPACE_DNS as UUID_NAMESPACE_DNS

from django.http import Http404
from rest_framework.serializers import ValidationError

from metax_api.exceptions import Http400
from metax_api.models import Directory
from .common_service import CommonService

import logging
_logger = logging.getLogger(__name__)
d = logging.getLogger(__name__).debug


class FileService(CommonService):

    @staticmethod
    def get_directory_contents(pk, recursive=False):
        return []

    @staticmethod
    def get_project_root_directories(project_identifier):
        from metax_api.api.base.serializers import DirectorySerializer
        try:
            root_dir = Directory.objects.get(project_identifier=project_identifier, parent_directory=None)
        except Directory.DoesNotExist:
            raise Http404
        except Directory.MultipleObjectsReturned: # pragma: no cover
            raise Exception('Directory.MultipleObjectsReturned when looking for root directory. This should never happen')
        return DirectorySerializer(root_dir).data

    @classmethod
    def _create_bulk(cls, common_info, initial_data_list, results, serializer_class, **kwargs):
        """
        Override the original _create_bulk from CommonService to also create directories,
        and setting them as parent_directory to approriate files, before creating the files.
        """
        file_list_with_dirs = cls._create_directories_from_file_list(common_info, initial_data_list, **kwargs)
        return super(FileService, cls)._create_bulk(common_info, file_list_with_dirs, results, serializer_class, **kwargs)

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
                errors['project_identifier'] = ['project_identifier is a required parameter (file id: %s)' % row['identifier']]

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
                raise Exception('emergency_break reached while creating leading dirs, should (probably) never happen...')

            emergency_break -= 1

            upper_dir = dirname(upper_dir)

    @classmethod
    def _create_directories(cls, common_info, created_dirs, unique_dir_paths, project_identifier, **kwargs):
        """
        Create to db directory hierarchy from directories extracted from the received file list.

        Save created paths/id's to created_dirs dict, so the results can be efficiently re-used
        by other dirs being created, and later by the files that are created.
        """
        from metax_api.api.base.serializers import DirectorySerializer

        python_process_pid = str(getpid())

        for i, path in enumerate(unique_dir_paths):

            directory = {
                'directory_path': path,
                'directory_name': basename(path),
                # identifier: uuid3 as hex, using as salt time in ms, idx of loop, and python process id
                'identifier': uuid3(UUID_NAMESPACE_DNS, '%d%d%s' % (int(round(time() * 1000)), i, python_process_pid)).hex,
                'project_identifier': project_identifier
            }

            directory.update(common_info)

            cls._find_parent_dir_from_previously_created_dirs(directory, created_dirs)

            serializer = DirectorySerializer(data=directory, **kwargs)
            serializer.is_valid()

            if serializer.errors: # pragma: no cover
                # should never happen. serious error
                raise Exception(str(serializer.errors))

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
            raise ValidationError({ 'parent_directory': ['multiple directories found when looking for parent (looked for path: %s)' % parent_dir_path]} )

        return { 'directory_path': parent_dir.directory_path, 'id': parent_dir.id }
