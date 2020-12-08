# This file is part of the Metax API service
#
# Copyright 2017-2018 Ministry of Education and Culture, Finland
#
# :author: CSC - IT Center for Science Ltd., Espoo Finland <servicedesk@csc.fi>
# :license: MIT

import json
import logging
from datetime import datetime

from django.conf import settings as django_settings
from django.shortcuts import redirect, render
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
            'haka_exists': 'eppn' in token_payload,
            'logout_redirect_domain': django_settings.SERVER_DOMAIN_NAME,
        }

        # note: django automatically searches templates from root directory templates/
        return render(request, 'secure/auth_success.html', context=context)


class SecureLogoutView(TemplateView):

    def get(self, request, **kwargs):
        """
        After local oidc logout, redirect to OP for OP's logout procedures.
        """
        return redirect(django_settings.AUTH_SERVER_LOGOUT_URL)
