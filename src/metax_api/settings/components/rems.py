import logging

from environ import ImproperlyConfigured

from metax_api.settings import env

logger = logging.getLogger(__name__)

REMS = {
    "ENABLED": env("REMS_ENABLED"),
}

if REMS["ENABLED"]:
    try:
        REMS["API_KEY"] = env("REMS_API_KEY")
        REMS["BASE_URL"] = env("REMS_BASE_URL")
        REMS["ETSIN_URL_TEMPLATE"] = env("REMS_ETSIN_URL_TEMPLATE")
        REMS["METAX_USER"] = env("REMS_METAX_USER")
        REMS["REPORTER_USER"] = env("REMS_REPORTER_USER")
        REMS["AUTO_APPROVER"] = env("REMS_AUTO_APPROVER")
        REMS["FORM_ID"] = int(env("REMS_FORM_ID"))
    except ImproperlyConfigured as e:
        logger.warning(e)
