import json
import logging

from django.db.models.signals import post_delete
from django.dispatch import receiver
from django.core.serializers.json import DjangoJSONEncoder
from django.forms.models import model_to_dict
from ..models import DeletedObject, CatalogRecord, CatalogRecordV2

_logger = logging.getLogger(__name__)

@receiver(post_delete)
def deleted_object_receiver(instance, sender, *args, **kwargs):
    if sender in [CatalogRecord, CatalogRecordV2]:
        try:
            model_type = instance._meta.model.__name__
            if hasattr(instance, '_initial_data["date_created"]'):
                instance._initial_data["date_created"] = instance._initial_data["date_created"].strftime("%m/%d/%Y, %H:%M:%S")
            if hasattr(instance, 'date_created'):
                instance.date_created = instance.date_created.strftime("%m/%d/%Y, %H:%M:%S")
            if hasattr(instance, 'date_modified'):
                instance.date_modified = instance.date_modified.strftime("%m/%d/%Y, %H:%M:%S")
            instance = model_to_dict(instance)
            deleted_object_json = json.dumps(instance, cls=DjangoJSONEncoder)
            DeletedObject.objects.create(model_name=model_type, object_data=deleted_object_json)
        except Exception as e:
            _logger.error("cannot save Deleted Object. Discarding..")
            _logger.debug(f"error: {e}")