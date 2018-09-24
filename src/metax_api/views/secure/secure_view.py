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

        context = {
            'email': token_payload['email'],
            'token_string': request.META['HTTP_OIDC_ID_TOKEN'],
            'token_valid_until': datetime.fromtimestamp(token_payload['exp']).strftime('%Y-%m-%d %H:%M:%S'),
        }

        # note: django automatically searches templates from root directory templates/
        return render(request, 'secure/auth_success.html', context=context)
