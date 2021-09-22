# This file is part of the Metax API service
#
# Copyright 2017-2018 Ministry of Education and Culture, Finland
#
# :author: CSC - IT Center for Science Ltd., Espoo Finland <servicedesk@csc.fi>
# :license: MIT

from django.contrib.auth.models import AbstractUser

"""
It is recommended to use a custom User model right from the start of a project
even if the default model is sufficient, in case there becomes a need to extend it.

https://docs.djangoproject.com/en/1.11/topics/auth/customizing/#using-a-custom-user-model-when-starting-a-project
"""


class MetaxUser(AbstractUser):
    pass
