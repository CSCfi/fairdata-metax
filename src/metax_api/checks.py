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
    try:

        for key, value in finto_api_urls.items():
            res = requests.head(value)
            status_dict["finto"].append({key: {"ok": res.ok, "status_code": res.status_code}})
        return status_dict

    except Exception as e:
        logger.error(e)
        return {"finto": {"ok": False, "error": str(e), "traceback": str(e.__traceback__)}}

