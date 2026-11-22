"""最终入库。原子写入本轮资讯、观察记录、汇总和最终任务状态。

写入前校验 worker 执行权，正常排除的资讯不写明细。错误明细由 db/failures.py 独立保存。
"""
import hashlib
from datetime import datetime
from uuid import UUID

from sqlalchemy import case, func, insert, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from src.db.database import ExecutionLost, collection_observations, guard, news_items, now, processing_runs
from src.pipeline.enrich import item_identity


async def save_collected_news(session: AsyncSession, run_id: UUID, collected_at: datetime,
                              accepted, originals, outcomes) -> None:
    """保存资讯和观察记录；事务由调用方管理。"""
    if sorted(o.index for o in outcomes) != list(range(len(originals))):
        raise ValueError("筛选结果没有完整对应候选资讯")
    if sum(o.status == "kept" for o in outcomes) != len(accepted):
        raise ValueError("保留资讯数量与筛选结果不一致")

    identities = {}
    observations = []
    kept = iter(accepted)
    for outcome in sorted(outcomes, key=lambda o: o.index):
        if outcome.status == "excluded":
            continue
        item = next(kept) if outcome.status == "kept" else originals[outcome.index]
        item_id = item_identity(item)
        values = {
            "id": item_id, **item.model_dump(exclude={"source", "url", "observed_at"}),
            "url": str(item.url), "source_id": item.source,
            "filter_status": outcome.status, "filter_reason": outcome.reason,
            "tags": outcome.tags, "ai_summary": outcome.summary, "last_run_id": run_id,
            "first_seen_at": item.observed_at or collected_at, "last_seen_at": item.observed_at or collected_at,
        }
        previous = identities.get(item_id, {})
        for field in ("content", "source_excerpt", "author", "published_at", "source_updated_at", "version_summary"):
            if values["last_seen_at"] >= previous.get("last_seen_at", values["last_seen_at"]) and not values[field] and previous.get(field):
                values[field] = previous[field]
        if previous:
            first_seen = min(previous["first_seen_at"], values["first_seen_at"])
            if previous["last_seen_at"] > values["last_seen_at"]:
                # 来源返回顺序不等于观察时间顺序，较旧结果不能覆盖较新元数据。
                older = values
                values = dict(previous)
                for field in ("content", "source_excerpt", "author", "published_at", "source_updated_at", "version_summary"):
                    if not values[field] and older[field]:
                        values[field] = older[field]
            values["first_seen_at"] = first_seen
        identities[item_id] = values
        observations.append({
            "run_id": run_id, "position": outcome.index + 1, "news_item_id": item_id,
            "channel": item.channel, "source_id": item.source, "source_item_id": item.source_item_id,
            "rank": item.rank,
            "observed_at": item.observed_at or collected_at,
            "content_hash": hashlib.sha256(" ".join(item.content.split()).encode("utf-8")).hexdigest() if item.content.strip() else None,
            "source_position": item.source_position,
            "filter_status": outcome.status,
            "filter_reason": outcome.reason, "tags": outcome.tags, "ai_summary": outcome.summary,
        })
    if next(kept, None) is not None:
        raise ValueError("保留资讯数量与筛选结果不一致")
    if observations:
        statement = pg_insert(news_items)
        updates = {}
        preserve_nonempty = {"content", "source_excerpt", "author", "published_at", "source_updated_at", "version_summary"}
        for column in news_items.columns:
            name = column.name
            if name in ("id", "first_seen_at", "last_seen_at", "content_length"):
                continue
            incoming = statement.excluded[name]
            if name in preserve_nonempty:
                incoming = func.coalesce(func.nullif(incoming, "") if name in
                    {"content", "source_excerpt", "author", "version_summary"} else incoming, column)
            updates[name] = case((statement.excluded.last_seen_at >= news_items.c.last_seen_at, incoming), else_=column)
        updates.update(
            first_seen_at=func.least(news_items.c.first_seen_at, statement.excluded.first_seen_at),
            last_seen_at=func.greatest(news_items.c.last_seen_at, statement.excluded.last_seen_at))
        upsert = statement.on_conflict_do_update(index_elements=[news_items.c.id], set_=updates)
        await session.execute(upsert, [identities[key] for key in sorted(identities)])
        await session.execute(insert(collection_observations), observations)


async def finish_run(sessions, run_id, version, result, originals):
    async with sessions.begin() as session:
        await guard(session, run_id, version)
        collection = result.collection
        source_ids = {source.source_id for source in collection.source_results}
        if any(item.source not in source_ids for item in originals):
            raise ValueError("候选资讯缺少对应的来源结果")
        collected_at = now()
        claimed = await session.execute(update(processing_runs).where(
            processing_runs.c.id == run_id, processing_runs.c.collected_at.is_(None)).values(
                collected_at=collected_at,
                source_results=[source.model_dump(mode="json") for source in collection.source_results],
                source_failures=[failure.model_dump(mode="json") for failure in collection.failures],
            ).returning(processing_runs.c.id))
        if claimed.scalar_one_or_none() is None:
            raise ExecutionLost("该批次已提交，禁止覆盖原结果")
        await save_collected_news(session, run_id, collected_at, collection.items, originals, result.outcomes)
        await session.execute(update(processing_runs).where(processing_runs.c.id == run_id).values(
            status=result.status, finished_at=now(), summary=result.summary))
