from metax_api.settings import env

RABBITMQ = {
    "HOSTS": env("RABBIT_MQ_HOSTS"),
    "PORT": env("RABBIT_MQ_PORT"),
    "USER": env("RABBIT_MQ_USER"),
    "PASSWORD": env("RABBIT_MQ_PASSWORD"),
    "EXCHANGES": [
        {
            "NAME": "datasets",
            "TYPE": "direct",
            # make rabbitmq remember queues after restarts
            "DURABLE": True,
        },
        {
            "NAME": "TTV-datasets",
            "TYPE": "fanout",
            "DURABLE": True,
            "QUEUES": [
                {
                    "NAME": "ttv-operations",
                    #"ROUTING_KEY": "some_key"
                }
            ]
        }
    ],
}
RABBIT_MQ_USE_VHOST = env("RABBIT_MQ_USE_VHOST")

if RABBIT_MQ_USE_VHOST:
    RABBITMQ["VHOST"] = env("RABBIT_MQ_VHOST")
