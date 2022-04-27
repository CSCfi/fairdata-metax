# This file is part of the Metax API service
#
# Copyright 2017-2018 Ministry of Education and Culture, Finland
#
# :author: CSC - IT Center for Science Ltd., Espoo Finland <servicedesk@csc.fi>
# :license: MIT

import logging

from django.db import models

_logger = logging.getLogger(__name__)

class ProjectStatistics(models.Model):
	project_identifier = models.CharField(primary_key=True, max_length=200)
	count = models.IntegerField()
	byte_size = models.BigIntegerField()
	published_datasets = models.TextField()