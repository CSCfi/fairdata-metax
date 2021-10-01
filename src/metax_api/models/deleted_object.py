# This file is part of the Metax API service
#
# Copyright 2017-2018 Ministry of Education and Culture, Finland
#
# :author: CSC - IT Center for Science Ltd., Espoo Finland <servicedesk@csc.fi>
# :license: MIT

from django.db import models
from django.db.models import JSONField

from metax_api.utils import get_tz_aware_now_without_micros


class DeletedObject(models.Model):

    model_name = models.CharField(max_length=200)
    object_data = JSONField(null=False)
    date_deleted = models.DateTimeField(default=get_tz_aware_now_without_micros)