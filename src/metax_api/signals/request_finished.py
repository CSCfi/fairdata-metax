from django.core.signals import request_finished
from django.dispatch import receiver

from metax_api.services.rabbitmq_service import RabbitMQService


@receiver(request_finished)
def handle_exceptions(sender, **kwargs):
    # Request has already been sent back so stuff done here are not prolonging the request handling
    RabbitMQService.consume_api_errors()
