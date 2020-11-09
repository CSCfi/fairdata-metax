from metax_api.settings import env
RABBITMQ = {
    'HOSTS': [env("RABBIT_MQ_HOSTS")],
    'PORT': env("RABBIT_MQ_PORT"),
    'USER': env("RABBIT_MQ_USER"),
    'VHOST': env("RABBIT_MQ_VHOST"),
    'PASSWORD': env("RABBIT_MQ_PASSWORD"),
    'EXCHANGES': [
        {
            'NAME': 'datasets',
            'TYPE': 'direct',
            # make rabbitmq remember queues after restarts
            'DURABLE': True
        }
    ]
}