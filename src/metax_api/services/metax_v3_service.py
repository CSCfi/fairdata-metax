import logging
import requests

from django.conf import settings

_logger = logging.getLogger(__name__)


class MetaxV3Service:
    def __init__(self):
        if not hasattr(settings, "METAX_V3"):
            raise Exception("Missing configuration from settings.py: METAX_V3")
        self.metaxV3Url = f"{settings.METAX_V3['PROTOCOL']}://{settings.METAX_V3['HOST']}"
        self.token = settings.METAX_V3["TOKEN"]


    def handle_error(self, error):
        _logger.error(f"Exception in Metax V3: {error}")
        if (response := getattr(error, 'response', None)) is not None:
            _logger.error(f"{response.text=}")
        raise MetaxV3UnavailableError()

    def create_dataset(self, dataset_json, legacy_file_ids=None):
        payload = {"dataset_json": dataset_json, "legacy_file_ids": legacy_file_ids}
        try:
            res = requests.post(
                f"{self.metaxV3Url}/v3/migrated-datasets",
                json=payload,
                headers={"Authorization": f"Token {self.token}"},
            )
            res.raise_for_status()
        except Exception as e:
            self.handle_error(e)

    def delete_dataset(self, dataset_id):
        try:
            res = requests.delete(
                f"{self.metaxV3Url}/v3/migrated-datasets/{dataset_id}",
                headers={"Authorization": f"Token {self.token}"},
            )
            if res.status_code != 404:
                res.raise_for_status()
        except Exception as e:
            self.handle_error(e)

    def update_dataset(self, dataset_id, dataset_json, legacy_file_ids=None):
        payload = {"dataset_json": dataset_json, "legacy_file_ids": legacy_file_ids}
        try:
            res = requests.put(
                f"{self.metaxV3Url}/v3/migrated-datasets/{dataset_id}",
                json=payload,
                headers={"Authorization": f"Token {self.token}"},
            )
            res.raise_for_status()
        except Exception as e:
            self.handle_error(e)

    def sync_files(self, files_json):
        try:
            res = requests.post(
                f"{self.metaxV3Url}/v3/files/from-legacy",
                json=files_json,
                headers={"Authorization": f"Token {self.token}"},
            )
            res.raise_for_status()
        except Exception as e:
            self.handle_error(e)

    def sync_contracts(self, contracts_json):
        for contract in contracts_json:
            try:
                res = requests.post(
                    f"{self.metaxV3Url}/v3/contracts/from-legacy",
                    json=contract,
                    headers={"Authorization": f"Token {self.token}"},
                )
                res.raise_for_status()
            except Exception as e:
                self.handle_error(e)


    def delete_project(self, project, flush=False):
        q_flush = "flush=true" if flush else "flush=false"
        try:
            res = requests.delete(
                f"{self.metaxV3Url}/v3/files?csc_project={project}&{q_flush}",
                headers={"Authorization": f"Token {self.token}"},
            )
            res.raise_for_status()
        except Exception as e:
            self.handle_error(e)


class MetaxV3UnavailableError(Exception):
    pass
