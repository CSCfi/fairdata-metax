# This file is part of the Metax API service
#
# Copyright 2017-2018 Ministry of Education and Culture, Finland
#
# :author: CSC - IT Center for Science Ltd., Espoo Finland <servicedesk@csc.fi>
# :license: MIT

import json
import logging

from django.conf import settings
from django.http import Http404


_logger = logging.getLogger(__name__)

class AuthService():

    @classmethod
    def get_user_projects(cls, request):
        """
        Fetches users file projects from local file and from token.
        Projects are cached to request.user.user_projects to increase performance.
        """
        if request.user.token is None:
            # request is made by an anonymous user. raise 404 instead of permission denied,
            # in order to not leak the user any information.
            raise Http404

        if hasattr(request.user, 'user_projects'):
            return request.user.user_projects

        user_projects = cls.extract_file_projects_from_token(request.user.token)

        username = request.user.token.get('CSCUserName', '')
        additional_projects = cls.get_additional_user_projects_from_file(username)
        user_projects.update(additional_projects)

        request.user.user_projects = user_projects
        return user_projects

    @staticmethod
    def extract_file_projects_from_token(token):
        '''
        Extract user's project identifiers from token claims.

        The user's token (request.user.token), when the user has access to some
        ida projects, is expected to contain a claim that looks like the following:

        "group_names": [
            "CPOUTA01:2000123"
            "TAITO01:2000124",
            "IDA01:2001125",
            "IDA01:2001126",
            "IDA01:2001127"
        ]

        Valid group names for IDA projects look like: IDA01:2001036.
        '''
        if not token:
            return set()

        user_projects = set()

        project_prefix = 'IDA01:'

        user_projects = set(
            group.split(':')[-1]
            for group in token.get('group_names', [])
            if group.startswith(project_prefix)
        )

        return user_projects

    @staticmethod
    def get_additional_user_projects_from_file(username):
        """
        Check if user has additional projects in a specific file on disk and return them.
        On local file values must be a list of strings.
        """
        user_projects = set()
        additional_projects = None

        try:
            with open(settings.ADDITIONAL_USER_PROJECTS_PATH, 'r') as file:
                additional_projects = json.load(file)
        except FileNotFoundError:
            _logger.info("No local file for user projects")
        except Exception as e:
            _logger.error(e)

        if additional_projects:
            if not additional_projects.get(username, False):
                _logger.info("No projects for user '%s' on local file" % username)
            elif not isinstance(additional_projects[username], list) \
                    or not isinstance(additional_projects[username][0], str):
                _logger.error("Projects on file are not list of strings")
            else:
                user_projects.update(p for p in additional_projects[username])

        return user_projects

    @classmethod
    def check_user_groups_against_groups(cls, request, group_list):
        """
        Get all user projects, without looking at service profile, from token and local file,
        and check them against the given list of groups.

        Return True if there is a match, otherwise False.
        """
        assert request.user is not None, 'request.user is None'
        assert request.user.token is not None, 'request.user.token is None'

        user_projects = set(
            group.split(':')[-1]
            for group in request.user.token.get('group_names', [])
        )

        user_projects.update(cls.get_additional_user_projects_from_file(request.user.username))

        for proj in user_projects:
            if proj in group_list:
                return True

        return False

    @classmethod
    def check_services_against_allowed_services(cls, request, service_list):
        """
        Get username of authenticating service
        and check it against the given list of services.

        Return True if there is a match, otherwise False.
        """
        assert request.user.username is not None, 'request.user.username is None'

        authenticated_service = request.user.username

        if authenticated_service in service_list:
            return True

        return False
