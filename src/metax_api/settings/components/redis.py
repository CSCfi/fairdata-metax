from metax_api.settings import env

REDIS = {
    'PASSWORD': env("REDIS_PASSWORD"),
    'LOCALHOST_PORT': env("REDIS_PORT"),
    "HOST": env("REDIS_HOST"),
    "PORT": env("REDIS_PORT"),

    # https://github.com/andymccurdy/redis-py/issues/485#issuecomment-44555664
    'SOCKET_TIMEOUT': 0.1,

    # db index reserved for test suites
    'TEST_DB': env("REDIS_TEST_DB"),

    # enables extra logging to console during cache usage
    'DEBUG': False,

    'SENTINEL': {
        'HOSTS': [
            [
                "127.0.0.1",
                16379
            ],
            [
                "127.0.0.1",
                16380
            ],
            [
                "127.0.0.1",
                16381
            ]

        ],
        'SERVICE': env("REDIS_SENTINEL_SERVICE")
    }
}
REDIS_USE_PASSWORD = env("REDIS_USE_PASSWORD")
