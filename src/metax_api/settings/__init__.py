"""
This is a django-split-settings main file.
For more information read this:
https://github.com/sobolevn/django-split-settings
To change settings file:
`DJANGO_ENV=production python manage.py runserver`
"""


from split_settings.tools import include, optional
import environ
# Managing environment via DJANGO_ENV variable:

env = environ.Env(
    # set casting, default value
    DEBUG=(bool, False),
    TRAVIS=(bool, False),
    METAX_ENV=(str, "local_development"),
    DJANGO_ENV=(str, "local"),
    ERROR_FILES_PATH=(str, '/var/log/metax-api/errors'),
    ADDITIONAL_USER_PROJECTS_PATH=(str, '')
)
# reading .env file
environ.Env.read_env()

ENV = env("DJANGO_ENV")

base_settings = [
    "components/common.py",
    "components/logging.py",
    "components/redis.py",
    "components/access_control.py",
    "components/elasticsearch.py",
    "components/rabbitmq.py",
    "components/externals.py",
    "environments/{0}.py".format(ENV),
    # Optionally override some settings:
    # optional('environments/legacy.py'),
]

# Include settings:
include(*base_settings)
