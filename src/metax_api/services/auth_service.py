# This file is part of the Metax API service
#
# Copyright 2017-2018 Ministry of Education and Culture, Finland
#
# :author: CSC - IT Center for Science Ltd., Espoo Finland <servicedesk@csc.fi>
# :license: MIT

import json
import logging
from django.conf import settings


_logger = logging.getLogger(__name__)

class AuthService():

    @staticmethod
    def get_user_projects(request):
        """
        Fetches users file projects from local file and from
        token. On local file values must be a list of strings.
        """
        user_projects = set()
        username = request.user.token.get('CSCUserName', '')

        try:
            with open(settings.ADDITIONAL_USER_PROJECTS_PATH, 'r') as file:
                file_projects = json.load(file)

            if isinstance(file_projects[username], list) and isinstance(file_projects[username][0], str):
                for project in file_projects[username]:
                    user_projects.add(project)
        except:
            _logger.debug("Unable to read projects from file.")

        finally:
            for project in AuthService.extract_file_projects_from_token(request.user.token):
                user_projects.add(project)

            return user_projects

    @staticmethod
    def extract_file_projects_from_token(token):
        '''
        Extract user's project identifiers from token claims.

        The user's token (request.user.token), when the user has access to some
        ida projects, is expected to contain a claim that looks like the following:

        "group_names": [
            "fairdata:IDA01",
            "fairdata:TAITO01",
            "fairdata:TAITO01:1234567",
            "fairdata:IDA01:2001036"
        ]

        Of these, only the groups that look like the last line are interesting:
        - Consists of three parts, separated by a ':' character
        - Middle part (some sort of namespace) is 'IDA01' (it may be interesting
          to parameterize this part in settings.py in the future)
        - Third part is the actual project identifier, as it appears in file metadata
          field 'project_identifier'.

        As a temporary solution, support also an alternative representation of group names,
        where valid group names look like: IDA01:2001036. This kind of group names are to
        be expected when the key CSCUserName is present in the authentication token.
        Eventually, only this form of group names will be supported (and configurable... proabably).
        '''
        if not token:
            return set()

        user_projects = set()

        project_prefix = 'fairdata:IDA01:' if token.get('sub', '').endswith('@fairdataid') else 'IDA01:'

        user_projects = set(
            group.split(':')[-1]
            for group in token.get('group_names', [])
            if group.startswith(project_prefix)
        )

        return user_projects
