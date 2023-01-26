# This file is part of the Metax API service
#
# Copyright 2017-2018 Ministry of Education and Culture, Finland
#
# :author: CSC - IT Center for Science Ltd., Espoo Finland <servicedesk@csc.fi>
# :license: MIT

import logging

from django.db import models

_logger = logging.getLogger(__name__)

class OrganizationStatistics(models.Model):
	organization = models.CharField(primary_key=True, max_length=200)
	count_total = models.IntegerField(default=0)
	count_ida = models.IntegerField(default=0)
	count_pas = models.IntegerField(default=0)
	count_att = models.IntegerField(default=0)
	count_other = models.IntegerField(default=0)
	byte_size_total = models.BigIntegerField(default=0)
	byte_size_ida = models.BigIntegerField(default=0)
	byte_size_pas = models.BigIntegerField(default=0)

