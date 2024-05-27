import logging
import requests

from watchman.decorators import check

from metax_api.services.redis_cache_service import RedisClient
from metax_api.tasks.refdata.refdata_indexer.service.elasticsearch_service import (
    ElasticSearchService,
)

logger = logging.getLogger(__name__)


@check
def elasticsearch_check():
    try:
        es = ElasticSearchService()
        ref_index = es.index_exists("reference_data")
        org_data = es.index_exists("organization_data")
        return {
            "elasticsearch": [
                {"index: reference_data": {"ok": ref_index}},
                {"index: organization_data": {"ok": org_data}},
            ]
        }
    except Exception as e:
        logger.error(e)
        return {
            "elasticsearch": {
                "ok": False,
                "error": str(e),
                "traceback": str(e.__traceback__),
            }
        }


@check
def redis_check():
    try:
        redis = RedisClient()
        refdata = redis.get("reference_data")
        if len(refdata) > 0:
            return {"redis": [{"key: reference_data": {"ok": True}}]}
    except Exception as e:
        logger.error(e)
        return {"redis": {"ok": False, "error": str(e), "traceback": str(e.__traceback__)}}

@check
def finto_check():
    finto_api_urls = {
        "location": "https://api.finto.fi/download/yso-paikat/yso-paikat-skos.rdf",
        "language": "https://api.finto.fi/download/lexvo/lexvo-skos.rdf",
        "field_of_science": "https://api.finto.fi/download/okm-tieteenala/okm-tieteenala-skos.rdf",
        "keyword": "https://api.finto.fi/download/koko/koko-skos.rdf"
    }
    status_dict = {"finto": []}
    for key, value in finto_api_urls.items():
        try:

            res = requests.head(value, timeout=5)
            status_dict["finto"].append({key: {"ok": res.ok, "status_code": res.status_code}})

        except Exception as e:
            logger.error(e)
            status_dict["finto"].append({key: {"ok": False, "error": str(e), "traceback": str(e.__traceback__)}})
    return status_dict


def check_metax_availability(v2_url, v3_url):
    try:
        requests.head(v2_url, timeout=10)
    except Exception as e:
        logger.error(e)
        return False, "Metax V2 is not available"

    try:
        requests.head(v3_url, timeout=10)
    except Exception as e:
        logger.error(e)
        return False, "Metax V3 is not available"

    return True, "OK"


@check
def v3_sync_check():

    v2_url = f"https://metax.fairdata.fi/rest/v2/datasets"
    v3_url = f"https://metax.fairdata.fi/v3/datasets"
    v2_params = {"limit": "1", "fields": "identifier"}
    v3_params = {"limit": "1"}

    status_dict = {"v3 synchronization": []}

    metaxes_available, response = check_metax_availability(v2_url, v3_url)
    if metaxes_available == False:

        status_dict["v3 synchronization"].append(
            {
                "ok": False,
                "details": response,
            }
        )
        return status_dict

    v3_catalogs_response = requests.get("https://metax.fairdata.fi/v3/data-catalogs", timeout=10)
    v3_catalogs_json = v3_catalogs_response.json()["results"]
    synced_catalogs = [catalog["id"] for catalog in v3_catalogs_json]

    try:
        for dc_id in synced_catalogs:
            params = {**v2_params, "data_catalog": dc_id}
            v2_count = requests.get(v2_url, params, timeout=10).json()["count"]

            params = {**v3_params, "data_catalog__id": dc_id}
            v3_count = requests.get(v3_url, params, timeout=10).json()["count"]

            if v2_count == v3_count:
                status_dict["v3 synchronization"].append(
                    {
                        dc_id: {
                            "ok": True,
                            "V2 Dataset count": v2_count,
                            "V3 Dataset count": v3_count,
                        }
                    }
                )
            else:
                status_dict["v3 synchronization"].append(
                    {
                        dc_id: {
                            "ok": False,
                            "V2 Dataset count": v2_count,
                            "V3 Dataset count": v3_count,
                        }
                    }
                )

            params = {**v2_params, "data_catalog": dc_id, "removed": "true"}
            v2_removed_count = requests.get(v2_url, params, timeout=10).json()["count"]

            params = {**v3_params, "data_catalog__id": dc_id, "include_removed": "true"}
            v3_removed_count = requests.get(v3_url, params, timeout=10).json()["count"] - v3_count

            if v2_removed_count == v3_removed_count:
                status_dict["v3 synchronization"].append(
                    {
                        f"{dc_id} removed": {
                            "ok": True,
                            "V2 Removed Dataset count": v2_removed_count,
                            "V3 Removed Dataset count": v3_removed_count,
                        }
                    }
                )
            else:
                status_dict["v3 synchronization"].append(
                    {
                        f"{dc_id} removed": {
                            "ok": False,
                            "V2 Removed Dataset count": v2_removed_count,
                            "V3 Removed Dataset count": v3_removed_count,
                        }
                    }
                )

        return status_dict

    except Exception as e:
        logger.error(e)
        return {
            "v3 synchronization": {
                "ok": False,
                "error": str(e),
                "traceback": str(e.__traceback__),
            }
        }

