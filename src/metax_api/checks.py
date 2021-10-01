import logging

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
