from metax_api.settings import env

METAX_V3 = {
    "HOST": env("METAX_V3_HOST"),
    "TOKEN": env("METAX_V3_TOKEN"),
    "INTEGRATION_ENABLED": env("METAX_V3_INTEGRATION_ENABLED"),
    "PROTOCOL": env("METAX_V3_PROTOCOL"),
}
