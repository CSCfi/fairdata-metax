from __future__ import absolute_import

# This will make sure celery is always imported when
# Django starts so that shared_task will use this app.
from .celery import app as celery_app

__all__ = ['celery_app']

default_app_config = 'metax_api.onappstart.OnAppStart'
