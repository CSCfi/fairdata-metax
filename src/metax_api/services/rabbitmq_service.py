# This file is part of the Metax API service
#
# Copyright 2017-2018 Ministry of Education and Culture, Finland
#
# :author: CSC - IT Center for Science Ltd., Espoo Finland <servicedesk@csc.fi>
# :license: MIT

import logging
import random
from datetime import datetime
from json import dumps as json_dumps, loads
from time import sleep

import pika
from django.conf import settings
from django.core import serializers
from django.core.serializers.json import DjangoJSONEncoder
from django.db import DatabaseError

from metax_api.models import ApiError
from metax_api.utils.utils import (
    datetime_to_str,
    executing_test_case,
    parse_timestamp_string_to_tz_aware_datetime,
)

_logger = logging.getLogger(__name__)


class _RabbitMQService:
    def __init__(self):
        if not hasattr(settings, "RABBITMQ"):
            raise Exception("Missing configuration from settings.py: RABBITMQ")
        self._settings = settings.RABBITMQ
        self._credentials = pika.PlainCredentials(
            self._settings["USER"], self._settings["PASSWORD"]
        )
        self._hosts = self._settings["HOSTS"]

    def _connect(self):
        """
        Creates and returns a new BlockingConnection for the caller. Creating a new connection enables multiple
        threads (i.e. requests) to access RabbitMQ in parallel using synchronous connection. Even though initializing
        a new connection for each request is slow and has some overhead, thread-safety is more important.
        """

        # Connection retries are needed as long as there is no load balancer in front of rabbitmq-server VMs
        sleep_time = 1
        num_conn_retries = 5
        _logger.info(f"connecting to RabbitMQ host: {self._hosts} port: {self._settings['PORT']}")

        for x in range(0, num_conn_retries):
            # Choose host randomly so that different hosts are tried out in case of connection problems
            host = random.choice(self._hosts)
            try:
                kwarg_params = {
                    "port": self._settings["PORT"],
                    "credentials": self._credentials,
                }
                if settings.RABBIT_MQ_USE_VHOST:
                    kwarg_params["virtual_host"] = self._settings["VHOST"]

                conn_params = pika.ConnectionParameters(host, **kwarg_params)
                connection = pika.BlockingConnection(conn_params)

            except Exception as e:
                _logger.error(
                    "Problem connecting to RabbitMQ server (%s), trying to reconnect..." % str(e)
                )
                sleep(sleep_time)
            else:
                _logger.info("RabbitMQ connected to %s" % host)

                return connection
        else:
            raise Exception("Unable to connect to RabbitMQ")

    def publish(self, body, routing_key="", exchange=None, persistent=True):
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
        connection = self._connect()
        channel = connection.channel()
        self._validate_publish_params(routing_key, exchange)

        additional_args = {}
        if persistent:
            additional_args["properties"] = pika.BasicProperties(delivery_mode=2)

        if isinstance(body, list):
            messages = body
        else:
            messages = [body]

        try:
            for message in messages:
                if isinstance(message, dict):
                    message = json_dumps(message, cls=DjangoJSONEncoder)
                channel.basic_publish(
                    body=message,
                    routing_key=routing_key,
                    exchange=exchange,
                    **additional_args,
                )
        except Exception as e:
            _logger.error(e)
            _logger.error("Unable to publish message to RabbitMQ")
            raise
        finally:
            connection.close()

    def consume_api_errors(self):
        connection = self._connect()
        channel = connection.channel()

        try:
            errors = []
            for method, _, body in channel.consume("metax-apierrors", inactivity_timeout=1):
                if method is None and body is None:
                    channel.cancel()
                    break
                try:
                    error_payload = loads(body)
                    error = ApiError(identifier=error_payload["identifier"], error=error_payload)
                    errors.append(error)
                except Exception as e:
                    _logger.error(e)
                finally:
                    channel.basic_ack(method.delivery_tag)
            try:
                # ApiError.objects.bulk_create(errors, batch_size=5000)
                now = datetime.now()
                tz_aware = parse_timestamp_string_to_tz_aware_datetime(datetime_to_str(now))
                with open(f"{settings.ERROR_FILES_PATH}{tz_aware}.log", 'w') as out:
                    serializers.serialize("json", errors, stream=out)
            except DatabaseError as e:
                _logger.error("cannot create API Error. Discarding..")
                _logger.error(f"error: {e}")

        except Exception as e:
            _logger.error(e)
        finally:
            _logger.info("All ApiErrors were handled")
            connection.close()

    def init_exchanges(self):
        """
        Declare the exchanges specified in settings. Re-declaring existing exchanges does no harm, but
        an error will occur if an exchange existed, and it is being re-declared with different settings.
        In that case the exchange has to be manually removed first, which can result in lost messages.
        """
        connection = self._connect()

        if connection is None:
            return

        channel = connection.channel()
        try:
            for exchange in self._settings["EXCHANGES"]:
                channel.exchange_declare(
                    exchange["NAME"],
                    exchange_type=exchange["TYPE"],
                    durable=exchange["DURABLE"],
                )
                for queue in exchange.get("QUEUES", []):
                    # declare queues in settings
                    channel.queue_declare(queue["NAME"], durable=exchange["DURABLE"])
                    channel.queue_bind(
                        queue["NAME"], exchange["NAME"], queue.get("ROUTING_KEY")
                    )
        except Exception as e:
            _logger.error(e)
            _logger.exception("Failed to initialize RabbitMQ exchanges")
            raise
        finally:
            connection.close()

    def _validate_publish_params(self, routing_key, exchange_name):
        """
        Ensure:
        - exchange_name is specified
        - routing_key is specified when exchange type is direct
        """
        if not exchange_name:
            raise Exception("Specify exchange to publish message to")

        for exchange in self._settings["EXCHANGES"]:
            if (
                exchange_name == exchange["NAME"]
                and exchange["TYPE"] == "direct"
                and routing_key == ""
            ):
                raise Exception(
                    "Messages without routing_key are discarded when exchange type is 'direct'"
                )


class _RabbitMQServiceDummy:

    """
    A dummy rabbitmq client that doesn't connect anywhere and doesn't do jack actually.
    """

    def __init__(self, settings=settings):
        self.messages = []

    def publish(self, body, routing_key="", exchange="datasets", persistent=True):
        msg = {"body":body, "routing_key":routing_key, "exchange":exchange, "persistent":persistent}
        self.messages.append(msg)

    def init_exchanges(self, *args, **kwargs):
        pass

    def consume_api_errors(self):
        pass

if executing_test_case():
    RabbitMQService = _RabbitMQServiceDummy()
else:
    RabbitMQService = _RabbitMQService()
