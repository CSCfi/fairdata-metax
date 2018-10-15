# This file is part of the Metax API service
#
# Copyright 2017-2018 Ministry of Education and Culture, Finland
#
# :author: CSC - IT Center for Science Ltd., Espoo Finland <servicedesk@csc.fi>
# :license: MIT


class AuthService():

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
        '''
        if not token:
            return set()

        user_projects = set()

        for group in token.get('group_names', []):
            group_name_parts = group.split(':')
            if len(group_name_parts) == 3 and group_name_parts[0] == 'fairdata' and group_name_parts[1] == 'IDA01':
                user_projects.add(group_name_parts[2])

        return user_projects
