from metax_api.settings import env

PID_MS = {
    "HOST": env("PID_MS_HOST"),
    "TOKEN": env("PID_MS_TOKEN"),
    "PROTOCOL": env("PID_MS_PROTOCOL"),
    "CATALOGS_TO_MIGRATE": env("PID_MS_CATALOGS_TO_MIGRATE")
}
