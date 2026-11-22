"""任务和资讯查询服务。负责登记、领取、查询、取消、恢复及任务投递记录。

读取已保存的资讯、观察记录和运行结果。整轮资讯处理由 extraction_service 负责，
最终入库由 db/save_run_results.py 负责。
"""
from datetime import timedelta
from uuid import NAMESPACE_URL, UUID, uuid4, uuid5

from sqlalchemy import func, or_, select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from src.db.database import (
    collection_observations, fetch_page, guard, news_items, now, processing_errors, processing_runs, task_outbox,
)
from src.db.failures import ProcessingError, errors_for, insert_failure, list_failures
from src.pipeline.collect import CollectionFailure, CollectionResult, CollectionSourceResult
from src.pipeline.filter import FilterOutcome
from src.pipeline.parse import NewsItem
from src.schemas import PipelineResult


class RunInProgress(RuntimeError):
    pass


async def load_kept_news(session: AsyncSession, run_id: UUID) -> list[NewsItem]:
    rows = (await session.execute(select(
        collection_observations, news_items.c.title, news_items.c.url,
        news_items.c.content, news_items.c.source_excerpt, news_items.c.author,
        news_items.c.published_at, news_items.c.source_updated_at, news_items.c.version_summary,
        collection_observations.c.source_id.label("source"),
    ).join(news_items).where(
        collection_observations.c.run_id == run_id,
        collection_observations.c.filter_status == "kept",
    ).order_by(collection_observations.c.position))).mappings()
    return [NewsItem.model_validate(row) for row in rows]


async def load_filter_outcomes(session: AsyncSession, run_id: UUID) -> list[FilterOutcome]:
    query = select(collection_observations).where(collection_observations.c.run_id == run_id).order_by(
        collection_observations.c.position,
    )
    rows = (await session.execute(query)).mappings()
    return [FilterOutcome(index=row["position"] - 1, status=row["filter_status"],
        reason=row["filter_reason"], tags=row["tags"], summary=row["ai_summary"],
        identity=str(row["news_item_id"]) if row["filter_status"] == "kept" else None) for row in rows]


async def load_news_item(session: AsyncSession, news_item_id: UUID) -> dict | None:
    """查看及后续分析的完整资讯入口，不需要连接采集记录表。"""
    row = (await session.execute(select(news_items).where(news_items.c.id == news_item_id))).mappings().one_or_none()
    return dict(row) if row is not None else None


async def list_news(sessions, *, status, source_id, q, limit, offset):
    conditions = []
    if status is not None:
        conditions.append(news_items.c.filter_status == status)
    if source_id is not None:
        conditions.append(news_items.c.source_id == source_id)
    if q is not None:
        conditions.append(news_items.c.title.icontains(q, autoescape=True))
    query = select(news_items).where(*conditions).order_by(news_items.c.last_seen_at.desc(), news_items.c.id)
    count = select(func.count()).select_from(news_items).where(*conditions)
    return await fetch_page(sessions, query, count, limit=limit, offset=offset)


async def list_observations(sessions, news_item_id, *, source_id, limit, offset):
    conditions = [collection_observations.c.news_item_id == news_item_id]
    if source_id is not None:
        conditions.append(collection_observations.c.source_id == source_id)
    query = select(collection_observations).where(*conditions).order_by(
        collection_observations.c.observed_at.desc().nulls_last(),
        collection_observations.c.run_id, collection_observations.c.position,
    )
    count = select(func.count()).select_from(collection_observations).where(*conditions)
    return await fetch_page(sessions, query, count, limit=limit, offset=offset)


LEASE_SECONDS = 120


async def begin_run(sessions, run_id, snapshot):
    async with sessions.begin() as session:
        timestamp = now()
        statement = pg_insert(processing_runs).values(id=run_id, status="running",
            execution_version=1, lease_expires_at=timestamp + timedelta(seconds=LEASE_SECONDS),
            started_at=timestamp, config_snapshot=snapshot, summary={}).on_conflict_do_nothing().returning(processing_runs.c.id)
        created = (await session.execute(statement)).scalar_one_or_none() is not None
        row = (await session.execute(select(processing_runs).where(
            processing_runs.c.id == run_id).with_for_update())).mappings().one()
        if row["status"] in ("success", "partial", "cancelled") or (row["status"] == "failed" and row["collected_at"] is not None):
            return None, row["config_snapshot"]
        if created:
            return 1, row["config_snapshot"]
        if row["status"] == "running" and row["lease_expires_at"] > timestamp:
            raise RunInProgress("该批次仍在执行，请稍后查询或使用新批次 ID")
        version = row["execution_version"] + 1
        await session.execute(update(processing_runs).where(processing_runs.c.id == run_id).values(
            status="running", execution_version=version, finished_at=None,
            lease_expires_at=timestamp + timedelta(seconds=LEASE_SECONDS)))
        return version, row["config_snapshot"]


async def heartbeat(sessions, run_id, version):
    async with sessions.begin() as session:
        await guard(session, run_id, version)
        await session.execute(update(processing_runs).where(processing_runs.c.id == run_id).values(
            lease_expires_at=now() + timedelta(seconds=LEASE_SECONDS)))


async def mark_terminated(sessions, run_id, version, status):
    async with sessions.begin() as session:
        await guard(session, run_id, version)
        await session.execute(update(processing_runs).where(processing_runs.c.id == run_id).values(
            status=status, finished_at=now()))

async def cancel_run(sessions, run_id):
    async with sessions.begin() as session:
        row = (await session.execute(select(processing_runs).where(
            processing_runs.c.id == run_id).with_for_update())).mappings().one_or_none()
        if not row or row["status"] not in ("queued", "running"):
            return False
        error = ProcessingError(stage="run", error_type="Cancelled",
                                reason="用户取消本轮处理", key="external_cancel")
        await insert_failure(session, run_id, error, occurred_at=now(),
                             error_id=uuid5(NAMESPACE_URL, f"{run_id}:external_cancel"))
        await session.execute(update(processing_runs).where(processing_runs.c.id == run_id).values(
            status="cancelled", finished_at=now(), execution_version=row["execution_version"] + 1))
        return True


async def load_run(sessions, run_id):
    async with sessions() as session:
        run = (await session.execute(select(processing_runs).where(
            processing_runs.c.id == run_id))).mappings().one_or_none()
        if not run or run["status"] in ("queued", "running"):
            return None
        collection = CollectionResult()
        if run["collected_at"] is not None:
            sources = [CollectionSourceResult.model_validate(source) for source in run["source_results"]]
            collection = CollectionResult(
                items=await load_kept_news(session, run_id),
                failures=[CollectionFailure.model_validate(failure) for failure in run["source_failures"]],
                empty_source_ids=[source.source_id for source in sources if source.status == "empty"],
                source_results=sources,
            )
        outcomes = await load_filter_outcomes(session, run_id)
    return PipelineResult(run_id=run_id, status=run["status"],
        collection=collection, outcomes=outcomes,
        errors=await errors_for(sessions, run_id), summary=run["summary"])


async def submit_run(sessions, snapshot, run_id=None):
    run_id = run_id or uuid4()
    async with sessions.begin() as session:
        timestamp = now()
        created = (await session.execute(pg_insert(processing_runs).values(
            id=run_id, status="queued", execution_version=0,
            started_at=timestamp, lease_expires_at=timestamp,
            config_snapshot=snapshot, summary={}
        ).on_conflict_do_nothing().returning(processing_runs.c.id))).scalar_one_or_none()
        if created is not None:
            await session.execute(pg_insert(task_outbox).values(
                run_id=run_id, available_at=timestamp, dispatch_count=0))
    return run_id

async def resume_run(sessions, run_id):
    async with sessions.begin() as session:
        row = (await session.execute(select(processing_runs).where(
            processing_runs.c.id == run_id).with_for_update())).mappings().one_or_none()
        if row is None:
            return "missing"
        if row["status"] == "queued":
            return "queued"
        if row["collected_at"] is not None or row["status"] == "cancelled":
            return "terminal"
        timestamp = now()
        if row["status"] == "running" and row["lease_expires_at"] > timestamp:
            return "active"
        if row["status"] not in ("failed", "running"):
            return "terminal"
        await session.execute(update(processing_runs).where(processing_runs.c.id == run_id).values(
            status="queued", finished_at=None, lease_expires_at=timestamp,
            execution_version=row["execution_version"] + 1))
        await session.execute(pg_insert(task_outbox).values(
            run_id=run_id, available_at=timestamp, dispatch_count=0
        ).on_conflict_do_update(index_elements=[task_outbox.c.run_id], set_={
            "available_at": timestamp, "last_error": None}))
        return "queued"

async def fail_unclaimed(sessions, run_id, error_type):
    """配置加载等领取前错误也必须可查询，不能无限留在排队中。"""
    async with sessions.begin() as session:
        row = (await session.execute(select(processing_runs).where(
            processing_runs.c.id == run_id).with_for_update())).mappings().one_or_none()
        if not row or not (row["status"] == "queued" or
                row["status"] == "running" and row["lease_expires_at"] <= now()):
            return
        error = ProcessingError(stage="run", error_type=error_type,
            reason="后台执行未能开始，请检查服务配置后恢复任务", key="worker_start")
        await insert_failure(session, run_id, error, occurred_at=now(), error_id=uuid4())
        await session.execute(update(processing_runs).where(processing_runs.c.id == run_id).values(
            status="failed", finished_at=now(), execution_version=row["execution_version"] + 1))


def _run_query():
    count = select(func.count()).select_from(processing_errors).where(
        processing_errors.c.run_id == processing_runs.c.id).correlate(processing_runs).scalar_subquery()
    return select(
        processing_runs, count.label("error_count"),
        func.coalesce(task_outbox.c.dispatch_count, 0).label("dispatch_count"),
        task_outbox.c.last_error.label("dispatch_error")
    ).outerjoin(task_outbox)


async def get_run_record(sessions, run_id):
    async with sessions() as session:
        query = _run_query().where(processing_runs.c.id == run_id)
        row = (await session.execute(query)).mappings().one_or_none()
    return dict(row) if row is not None else None


async def get_run_status(sessions, run_id):
    async with sessions() as session:
        query = select(processing_runs.c.status).where(processing_runs.c.id == run_id)
        return (await session.execute(query)).scalar_one_or_none()


async def get_run_execution(sessions, run_id):
    async with sessions() as session:
        query = select(processing_runs.c.status, processing_runs.c.config_snapshot).where(
            processing_runs.c.id == run_id,
        )
        return (await session.execute(query)).mappings().one_or_none()


async def list_run_records(sessions, *, status, limit, offset):
    conditions = [] if status is None else [processing_runs.c.status == status]
    query = _run_query().where(*conditions).order_by(processing_runs.c.started_at.desc(), processing_runs.c.id)
    count = select(func.count()).select_from(processing_runs).where(*conditions)
    return await fetch_page(sessions, query, count, limit=limit, offset=offset)


async def next_dispatch(session, timestamp):
    """Lock one pending outbox row for the duration of the dispatch attempt."""
    query = select(task_outbox.c.run_id).join(processing_runs).where(
        task_outbox.c.available_at <= timestamp,
        or_(processing_runs.c.status == "queued",
            (processing_runs.c.status == "running") & (processing_runs.c.lease_expires_at <= timestamp)),
    ).order_by(task_outbox.c.available_at, task_outbox.c.run_id)
    query = query.with_for_update(of=task_outbox, skip_locked=True).limit(1)
    return (await session.execute(query)).scalar_one_or_none()


async def record_dispatch(session, run_id, *, timestamp, redispatch_seconds, failure):
    await session.execute(update(task_outbox).where(task_outbox.c.run_id == run_id).values(
        dispatch_count=task_outbox.c.dispatch_count + 1,
        last_dispatched_at=timestamp,
        available_at=timestamp + timedelta(seconds=redispatch_seconds),
        last_error=failure,
    ))


class RunNotFound(LookupError):
    """The requested run does not exist."""


class NewsNotFound(LookupError):
    """The requested news item does not exist."""


class RunResultUnavailable(RuntimeError):
    """The run has not finished or has resumed."""


class RunCannotCancel(RuntimeError):
    """The run has already finished."""


async def require_run(sessions, run_id):
    row = await get_run_record(sessions, run_id)
    if row is None:
        raise RunNotFound(run_id)
    return row


async def get_completed_result(sessions, run_id):
    row = await require_run(sessions, run_id)
    if row["status"] in ("queued", "running"):
        raise RunResultUnavailable("任务尚未结束，请继续查询状态")
    result = await load_run(sessions, run_id)
    if result is None:
        raise RunResultUnavailable("任务已恢复执行，请继续查询状态")
    return result


async def get_run_failures(sessions, run_id, *, limit, offset):
    await require_run(sessions, run_id)
    return await list_failures(sessions, run_id, limit=limit, offset=offset)


async def request_cancellation(sessions, run_id):
    row = await require_run(sessions, run_id)
    if row["status"] != "cancelled" and not await cancel_run(sessions, run_id):
        raise RunCannotCancel(run_id)
    return await require_run(sessions, run_id)


async def require_news_item(sessions, news_item_id):
    async with sessions() as session:
        row = await load_news_item(session, news_item_id)
    if row is None:
        raise NewsNotFound(news_item_id)
    return row


async def get_news_history(sessions, news_item_id, *, source_id, limit, offset):
    await require_news_item(sessions, news_item_id)
    return await list_observations(sessions, news_item_id, source_id=source_id,
                                   limit=limit, offset=offset)
