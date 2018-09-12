# This file is part of the Metax API service
#
# Copyright 2017-2018 Ministry of Education and Culture, Finland
#
# :author: CSC - IT Center for Science Ltd., Espoo Finland <servicedesk@csc.fi>
# :license: MIT

import logging
from json import dumps as json_dumps
import random
from time import sleep

import pika
from django.conf import settings as django_settings

from metax_api.utils.utils import executing_test_case, executing_travis

_logger = logging.getLogger(__name__)
d = logging.getLogger(__name__).debug


def RabbitMQService(*args, **kwargs):
    """
    A factory for the rabbitmq client.

    Returns dummy class when executing inside travis or test cases
    """
    if executing_travis() or executing_test_case() or kwargs.pop('dummy', False):
        return _RabbitMQServiceDummy(*args, **kwargs)
    else:
        return _RabbitMQService(*args, **kwargs)


class _RabbitMQService():

    def __init__(self, settings=django_settings):
        """
        Uses django settings.py by default. Pass a dict as settings to override.
        """
        if not isinstance(settings, dict):
            if not hasattr(settings, 'RABBITMQ'):
                raise Exception('Missing configuration from settings.py: RABBITMQ')
            settings = settings.RABBITMQ

        credentials = pika.PlainCredentials(settings['USER'], settings['PASSWORD'])
        hosts = settings['HOSTS']

        # Connection retries are needed as long as there is no load balancer in front of rabbitmq-server VMs
        sleep_time = 2
        num_conn_retries = 15

        for x in range(0, num_conn_retries):
            # Choose host randomly so that different hosts are tried out in case of connection problems
            try:
                self._connection = pika.BlockingConnection(pika.ConnectionParameters(
                    random.choice(hosts),
                    settings['PORT'],
                    settings['VHOST'],
                    credentials))

                self._channel = self._connection.channel()
                self._settings = settings
                str_error = None
            except Exception as e:
                _logger.error("Problem connecting to RabbitMQ server, trying to reconnect...")
                str_error = e

            if str_error:
                sleep(sleep_time)  # wait before trying to connect again
            else:
                break

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
        if not self.connection_ok():
            _logger.error("Unable to publish message to RabbitMQ due to connection error")
            return

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

            try:
                self._channel.basic_publish(body=message, routing_key=routing_key, exchange=exchange, **additional_args)
            except Exception as e:
                _logger.error(e)
                _logger.error("Unable to publish message to RabbitMQ")

    def get_channel(self):
        return self._channel

    def connection_ok(self):
        return hasattr(self, "_connection") and self._connection.is_open

    def init_exchanges(self):
        """
        Declare the exchanges specified in settings. Re-declaring existing exchanges does no harm, but
        an error will occur if an exchange existed, and it is being re-declared with different settings.
        In that case the exchange has to be manually removed first, which can result in lost messages.
        """
        if not self.connection_ok():
            _logger.error("Unable to init exchanges in RabbitMQ due to connection error")
            return

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


class _RabbitMQServiceDummy():

    """
    A dummy rabbitmq client that doesn't connect anywhere and doesn't do jack actually.
    """

    def __init__(self, settings=django_settings):
        pass

    def publish(self, body, routing_key='', exchange='datasets', persistent=True):
        pass

    def get_channel(self):
        return None
