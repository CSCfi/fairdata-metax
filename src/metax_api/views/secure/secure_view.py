# This file is part of the Metax API service
#
# Copyright 2017-2018 Ministry of Education and Culture, Finland
#
# :author: CSC - IT Center for Science Ltd., Espoo Finland <servicedesk@csc.fi>
# :license: MIT

from datetime import datetime
import logging
import json

from django.shortcuts import render
from django.views.generic import TemplateView

from metax_api.utils import json_logger


_logger = logging.getLogger(__name__)


class SecureLoginView(TemplateView):

    def get(self, request, **kwargs):
        """
        Reached through a redirect after a successful OIDC authentication.
        Parse the received id_token, and show selected contents of it to the user
        on a web page.
        """
        # from pprint import pprint
        # pprint(request.META)
        _logger.debug('extracting information from token')

        token_payload = json.loads(request.META['HTTP_OIDC_ID_TOKEN_PAYLOAD'])
        _logger.debug(token_payload)
        if token_payload.get('sub', '').endswith('@fairdataid'):
            return self._old_proxy(request, token_payload)
        else:
            return self._new_proxy(request, token_payload)

    def _old_proxy(self, request, token_payload):
        linked_accounts = self._get_linked_accounts(token_payload)

        for acc in linked_accounts:
            if acc.endswith('@cscuserid'):
                csc_account_linked = True
                break
        else:
            csc_account_linked = False

        json_logger.info(
            event='user_login_visit',
            user_id=token_payload['sub'],
            org_id=token_payload['schacHomeOrganization'],
        )

        context = {
            'email': token_payload['email'],
            'linked_accounts': linked_accounts,
            'csc_account_linked': csc_account_linked,
            'token_string': request.META['HTTP_OIDC_ID_TOKEN'],
            'token_valid_until': datetime.fromtimestamp(token_payload['exp']).strftime('%Y-%m-%d %H:%M:%S'),
        }

        # note: django automatically searches templates from root directory templates/
        return render(request, 'secure/auth_success.html', context=context)

    def _new_proxy(self, request, token_payload):
        try:
            json_logger.info(
                event='user_login_visit',
                user_id=token_payload.get('CSCUserName', token_payload['eppn']),
                org_id=token_payload.get('schacHomeOrganization', 'org_missing'),
            )
        except KeyError:
            _logger.error('token_payload has no CSCUserName or eppn')

        idm_account_exists = len(token_payload.get('CSCUserName', '')) > 0

        home_org_exists = len(token_payload.get('schacHomeOrganization', '')) > 0

        context = {
            'email': token_payload['email'],
            'idm_account_exists': idm_account_exists,
            'home_org_exists': home_org_exists,
            'token_string': request.META['HTTP_OIDC_ID_TOKEN'] if idm_account_exists and home_org_exists else '',
            'token_valid_until': datetime.fromtimestamp(token_payload['exp']).strftime('%Y-%m-%d %H:%M:%S'),
        }

        # note: django automatically searches templates from root directory templates/
        return render(request, 'secure/auth_success_new.html', context=context)

    def _get_linked_accounts(self, token_payload):
        return [ acc for acc in token_payload.get('linkedIds', []) if not acc.endswith('@fairdataid') ]
