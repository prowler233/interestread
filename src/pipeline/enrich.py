"""补字段。校验入选资讯，生成身份 ID，并检查来源和渠道是否仍与原数据一致。

单条补字段失败时上报错误，其余资讯继续处理。"""
import json
from uuid import NAMESPACE_URL, UUID, uuid5

from src.db.failures import ProcessingError
from src.pipeline.parse import NewsItem


def item_identity(item: NewsItem) -> UUID:
    key = json.dumps([str(item.url), item.title], separators=(",", ":"))
    return uuid5(NAMESPACE_URL, key)


async def enrich_news(collected, outcomes, enrich, report):
    accepted = []
    identities = set()
    enrichment_failures = 0
    kept_by_index = {}
    for outcome in outcomes:
        if outcome.status != "kept":
            continue
        item = collected.items[outcome.index]
        try:
            candidate = enrich(item) if enrich else item
            candidate = type(item).model_validate(candidate.model_dump())
            # 不允许补字段改变资讯所属来源。
            if candidate.source != item.source or candidate.channel != item.channel:
                raise ValueError("补字段不能改变来源")
            outcome.identity = str(item_identity(candidate))
            identities.add(outcome.identity)
            accepted.append(candidate)
            kept_by_index[outcome.index] = candidate
        except Exception as exc:
            enrichment_failures += 1
            outcome.status = "failed"
            outcome.reason = "必要字段补齐失败"
            outcome.tags = []
            outcome.summary = None
            await report(ProcessingError(stage="enrich", source_id=item.source,
                item_positions=[outcome.index], error_type=type(exc).__name__, reason=outcome.reason))
    return accepted, {
        "enrich_failed_count": enrichment_failures,
        "stored_observations": sum(outcome.status != "excluded" for outcome in outcomes),
        "stored_unique_identities": len({
            str(item_identity(kept_by_index.get(outcome.index, collected.items[outcome.index])))
            for outcome in outcomes if outcome.status != "excluded"
        }),
        "kept_observations": len(accepted),
        "kept_unique_identities": len(identities),
    }
