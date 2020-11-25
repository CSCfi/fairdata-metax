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
from django.core.serializers.json import DjangoJSONEncoder

from metax_api.utils.utils import executing_test_case, executing_travis

_logger = logging.getLogger(__name__)

class _RabbitMQService():

    def __init__(self):
        if not hasattr(django_settings, 'RABBITMQ'):
            raise Exception('Missing configuration from settings.py: RABBITMQ')
        self._settings = django_settings.RABBITMQ
        self._credentials = pika.PlainCredentials(self._settings['USER'], self._settings['PASSWORD'])
        self._hosts = self._settings['HOSTS']
        self._connection = None

    def _connect(self):
        if self._connection and self._connection.is_open:
            return

        # Connection retries are needed as long as there is no load balancer in front of rabbitmq-server VMs
        sleep_time = 2
        num_conn_retries = 15

        for x in range(0, num_conn_retries):
            # Choose host randomly so that different hosts are tried out in case of connection problems
            host = random.choice(self._hosts)
            try:
                self._connection = pika.BlockingConnection(pika.ConnectionParameters(
                    host,
                    self._settings['PORT'],
                    self._settings['VHOST'],
                    self._credentials))
            except Exception as e:
                _logger.error("Problem connecting to RabbitMQ server (%s), trying to reconnect..." % str(e))
                sleep(sleep_time)
            else:
                self._channel = self._connection.channel()
                _logger.debug('RabbitMQ connected to %s' % host)
                break
        else:
            raise Exception("Unable to connect to RabbitMQ")

    def publish_to_TTV(self, body, routing_key='', exchange=None, persistent=True):
        """
        Publish a message to an exchange, which might or might not have queues bound to it.

        body: body of the message. can be a list of messages, in which case each message is published
              individually.
        exchange: exchange to publish in
        persistent: make message persist in rabbitmq storage over rabbitmq-server restart.
                    otherwise messages not retrieved by clients before restart will be lost.
                    (still is not 100 % guaranteed to persist!)
        """
        # For testing
        # credentials = pika.PlainCredentials(self._settings['USER'], self._settings['PASSWORD'])
        # connection = pika.BlockingConnection(pika.ConnectionParameters(
        #     host = 'localhost',
        #     virtual_host = self._settings['VHOST'],
        #     port = 5672,
        #     credentials = credentials))

        if self._settings['USER'] != 'ttv':
            pass
        else:
            host = random.choice(self._hosts)
            connection = pika.BlockingConnection(pika.ConnectionParameters(
                host,
                self._settings['PORT'],
                self._settings['VHOST_TTV'],
                self._credentials))

            if isinstance(body, list):
                messages = body
            else:
                messages = [body]

            additional_args = {}
            if persistent:
                additional_args['properties'] = pika.BasicProperties(delivery_mode=2)

            channel = connection.channel()

            exchange = 'TTV-datasets'
            queue_4 = 'ttv-create'
            queue_5 = 'ttv-update'
            queue_6 = 'ttv-delete'

            channel.exchange_declare(exchange=exchange, exchange_type='fanout')
            channel.queue_declare(queue_4, durable=True)
            channel.queue_declare(queue_5, durable=True)
            channel.queue_declare(queue_6, durable=True)

            channel.queue_bind(exchange=exchange, queue=queue_4, routing_key='create')
            channel.queue_bind(exchange=exchange, queue=queue_5, routing_key='update')
            channel.queue_bind(exchange=exchange, queue=queue_6, routing_key='delete')

            try:
                for message in messages:
                    if isinstance(message, dict):
                        message = json_dumps(
                            message,
                            cls=DjangoJSONEncoder)
                    channel.basic_publish(body=message, routing_key=routing_key, exchange=exchange, **additional_args)
            except Exception as e:
                _logger.error(e)
                _logger.error("Unable to publish message to RabbitMQ")
                raise
            finally:
                # for testing
                connection.close()

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
        self._connect()
        self._validate_publish_params(routing_key, exchange)

        additional_args = {}
        if persistent:
            additional_args['properties'] = pika.BasicProperties(delivery_mode=2)

        if isinstance(body, list):
            messages = body
        else:
            messages = [body]

        try:
            for message in messages:
                if isinstance(message, dict):
                    message = json_dumps(
                        message,
                        cls=DjangoJSONEncoder)
                self._channel.basic_publish(body=message, routing_key=routing_key, exchange=exchange, **additional_args)
                self.publish_to_TTV(body=message, routing_key=routing_key, exchange=None)
        except Exception as e:
            _logger.error(e)
            _logger.error("Unable to publish message to RabbitMQ")
            raise
        finally:
            self._connection.close()

    def init_exchanges(self):
        """
        Declare the exchanges specified in settings. Re-declaring existing exchanges does no harm, but
        an error will occur if an exchange existed, and it is being re-declared with different settings.
        In that case the exchange has to be manually removed first, which can result in lost messages.
        """
        self._connect()
        try:
            for exchange in self._settings['EXCHANGES']:
                self._channel.exchange_declare(
                    exchange['NAME'], exchange_type=exchange['TYPE'], durable=exchange.get('DURABLE', True))
        except Exception as e:
            _logger.error(e)
            _logger.exception('Failed to initialize RabbitMQ exchanges')
            raise
        finally:
            self._connection.close()

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

    def init_exchanges(self, *args, **kwargs):
        pass


if executing_travis() or executing_test_case():
    RabbitMQService = _RabbitMQServiceDummy()
else:
    RabbitMQService = _RabbitMQService()
