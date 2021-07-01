# This file is part of the Metax API service
#
# Copyright 2017-2018 Ministry of Education and Culture, Finland
#
# :author: CSC - IT Center for Science Ltd., Espoo Finland <servicedesk@csc.fi>
# :license: MIT

import logging

from django.db import models
from django.db.models import JSONField

from metax_api.utils import get_tz_aware_now_without_micros

_logger = logging.getLogger(__name__)

class ApiError(models.Model):

    id = models.BigAutoField(primary_key=True, editable=False)
    identifier = models.CharField(max_length=200, unique=True, null=False)
    error = JSONField(null=False)
    date_created = models.DateTimeField(default=get_tz_aware_now_without_micros)