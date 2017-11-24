import logging
from json import dumps as json_dumps

import pika
from django.conf import settings as django_settings

from .utils import executing_test_case, executing_travis

_logger = logging.getLogger(__name__)
d = logging.getLogger(__name__).debug

def RabbitMQ(*args, **kwargs):
    """
    A factory for the rabbitmq client.

    Returns dummy class when executing inside travis or test cases
    """
    if executing_travis() or executing_test_case() or kwargs.pop('dummy', False):
        return _RabbitMQDummy(*args, **kwargs)
    else:
        return _RabbitMQ(*args, **kwargs)


class _RabbitMQ():

    def __init__(self, settings=django_settings):
        """
        Uses django settings.py by default. Pass a dict as settings to override.
        """
        if not isinstance(settings, dict):
            if not hasattr(settings, 'RABBITMQ'):
                raise Exception('Missing configuration from settings.py: RABBITMQ')
            settings = settings.RABBITMQ

        credentials = pika.PlainCredentials(settings['USER'], settings['PASSWORD'])

        self._connection = pika.BlockingConnection(pika.ConnectionParameters(
            settings['HOSTS'],
            settings['PORT'],
            settings['VHOST'],
            credentials))

        self._channel = self._connection.channel()
        self._settings = settings
        self._init_exchanges()

    def publish(self, body, routing_key='', exchange=None, persistent=True):
        """
        Publish a message to an exchange, which might or might not have queues bound to it.

        body: body of the message. can be a list of messages, in which case each message is published
              individually.
        routing_key: in direct-type exchanges, publish message to a specific route, which
                     clients can filter to in their queues. a required parameter for direct-type
                     exchanges (throws exception if missing).
        exchange: exchange to publish in
        persistent: make message persist in rabbitmq storage over rabbitmq-server restart.
                    otherwise messages not retrieved by clients before restart will be lost.
                    (still is not 100 % guaranteed to persist!)
        """
        self._validate_publish_params(routing_key, exchange)

        additional_args = {}
        if persistent:
            additional_args['properties'] = pika.BasicProperties(delivery_mode=2)

        if isinstance(body, list):
            messages = body
        else:
            messages = [body]

        for message in messages:
            if isinstance(message, dict):
                message = json_dumps(message)

            self._channel.basic_publish(body=message, routing_key=routing_key, exchange=exchange, **additional_args)

    def get_channel(self):
        return self._channel

    def _init_exchanges(self):
        """
        Declare the exchanges specified in settings. Re-declaring existing exchanges does no harm, but
        an error will occur if an exchange existed, and it is being re-declared with different settings.
        In that case the exchange has to be manually removed first, which can result in lost messages.
        """
        for exchange in self._settings['EXCHANGES']:
            self._channel.exchange_declare(
                exchange['NAME'], exchange_type=exchange['TYPE'], durable=exchange.get('DURABLE', True))

    def _validate_publish_params(self, routing_key, exchange_name):
        """
        Ensure:
        - exchange_name is specified
        - routing_key is specified when exchange type is direct
        """
        if not exchange_name:
            raise Exception('Specify exchange to publish message to')

        for exchange in self._settings['EXCHANGES']:
            if exchange_name == exchange['NAME'] and exchange['TYPE'] == 'direct' and routing_key == '':
                raise Exception('Messages without routing_key are discarded when exchange type is \'direct\'')


class _RabbitMQDummy():

    """
    A dummy rabbitmq client that doesn't connect anywhere and doesn't do jack actually.
    """

    def __init__(self, settings=django_settings):
        pass

    def publish(self, body, routing_key='', exchange='datasets', persistent=True):
        pass

    def get_channel(self):
        return None
