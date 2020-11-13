from watchman.decorators import check
from metax_api.tasks.refdata.refdata_indexer.service.elasticsearch_service import ElasticSearchService
from metax_api.services.redis_cache_service import RedisClient
import logging

logger = logging.getLogger(__name__)


@check
def elasticsearch_check():
    try:
        es = ElasticSearchService()
        ref_index = es.index_exists("reference_data")
        org_data = es.index_exists("organization_data")
        return {"elasticsearch":
            [
                {"index: reference_data": {"ok": ref_index}},
                {"index: organization_data": {"ok": org_data}}
            ]
        }
    except Exception as e:
        logger.error(e)
        return {"elasticsearch": {"ok": False, "error": e, "traceback": e.__traceback__}}

@check
def redis_check():
    try:
        redis = RedisClient()
        refdata = redis.get("reference_data")
        return {"redis": [
            {"key: reference_data":{"ok": refdata}}
        ]}
    except Exception as e:
        logger.error(e)
        return {"redis": {"ok": False, "error": e, "traceback": e.__traceback__}}
