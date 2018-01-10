#!/usr/bin/env python
import pika
import yaml

"""
for testing:

script to listen for messages sent when someone accesses /rest/datasets/pid/rabbitmq
"""

with open('/home/metax-user/app_config') as app_config:
    settings = yaml.load(app_config)['RABBITMQ']


def get_test_user():
    for user in settings['CONSUMERS']:
        if user['is_test_user']:
            return user


test_user = get_test_user()
credentials = pika.PlainCredentials(test_user['name'], test_user['password'])
connection = pika.BlockingConnection(
    pika.ConnectionParameters(
        settings['HOSTS'],
        settings['PORT'],
        test_user['vhost'],
        credentials))

channel = connection.channel()

exchange = 'datasets'
queue_1 = 'testaaja-create'
queue_2 = 'testaaja-update'
queue_3 = 'testaaja-delete'

# note: requires write permission to exchanges
# channel.exchange_declare(exchange=exchange, type='fanout')

channel.queue_declare(queue_1, durable=True)
channel.queue_declare(queue_2, durable=True)
channel.queue_declare(queue_3, durable=True)

channel.queue_bind(exchange=exchange, queue=queue_1, routing_key='create')
channel.queue_bind(exchange=exchange, queue=queue_2, routing_key='update')
channel.queue_bind(exchange=exchange, queue=queue_3, routing_key='delete')


def callback_1(ch, method, properties, body):
    print(" [ create ] %r" % body)


def callback_2(ch, method, properties, body):
    print(" [ update ] %r" % body)


def callback_3(ch, method, properties, body):
    print(" [ delete ] %r" % body)


channel.basic_consume(callback_1, queue=queue_1, no_ack=True)
channel.basic_consume(callback_2, queue=queue_2, no_ack=True)
channel.basic_consume(callback_3, queue=queue_3, no_ack=True)

print('[*] Waiting for logs. To exit press CTRL+C')
channel.start_consuming()
