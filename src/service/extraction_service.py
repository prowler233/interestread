"""资讯提取服务。串起采集、解析、筛选、补字段、汇总和最终提交。

负责领取和续期执行权，遇到中断整轮的异常时标注阶段并上报。"""
import asyncio
import json
import logging
from collections import Counter
from contextlib import AsyncExitStack, asynccontextmanager, suppress
from time import perf_counter
from uuid import uuid4

from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from yaml import YAMLError

from config.connect_settings import DatabaseSettings
from config.crawl_settings import CrawlSettings
from config.filter_settings import FilterSettings
from src.ai_client import AISettings, create_ai_client, load_ai_connection
from src.pipeline.collect import collect_result
from src.db.database import ExecutionLost
from src.pipeline.enrich import enrich_news
from src.db.failures import ProcessingError, RunFailureRecorder
from src.pipeline.filter import batches, filter_items, load_filter_settings, plan_search
from config.snapshot_settings import restore_settings, snapshot_settings
from src.schemas import PipelineResult
from src.db.save_run_results import finish_run
from src.service.run_service import begin_run, get_run_status, heartbeat, load_run, mark_terminated, require_run, submit_run

logger = logging.getLogger(__name__)


class RunStorageError(RuntimeError):
    pass


def prepare_run(body):
    crawl = CrawlSettings.load()
    if body.source_ids is not None:
        selected = set(body.source_ids)
        allowed = {s.id for s in crawl.iter_sources() if s.enabled}
        if selected - allowed:
            raise ValueError("请求包含不存在或未启用的来源")
        def select_sources(sources):
            return [s.model_copy(update={"enabled": s.enabled and s.id in selected}) for s in sources]
        crawl = crawl.model_copy(update={
            "newsnow": crawl.newsnow.model_copy(update={"sources": select_sources(crawl.newsnow.sources)}),
            "RAJ": crawl.RAJ.model_copy(update={"feeds": select_sources(crawl.RAJ.feeds)}),
            "platforms": crawl.platforms.model_copy(update={"sources": select_sources(crawl.platforms.sources)})})
    updates = {}
    if body.filter_mode is not None:
        updates["mode"] = body.filter_mode
    if body.keyword is not None:
        updates["keyword"] = body.keyword
    ai_updates = {
        **({"concurrency": body.ai_concurrency} if body.ai_concurrency is not None else {}),
        **({"batch_size": body.ai_batch_size} if body.ai_batch_size is not None else {})}
    if ai_updates:
        updates["ai"] = ai_updates
    if "keyword" in updates:
        updates["keyword"] = updates["keyword"].model_dump()
    settings = load_filter_settings(overrides=updates)
    snapshot = snapshot_settings(crawl, settings)
    snapshot["execution"] = {"filter_concurrency": settings.ai.concurrency}
    return snapshot


class InvalidRunSettings(ValueError):
    """Run parameters or server configuration could not be validated."""


async def schedule_extraction(sessions, body):
    if body.run_id is not None:
        existing = await get_run_status(sessions, body.run_id)
        if existing is not None:
            return body.run_id, existing
    try:
        snapshot = await asyncio.to_thread(prepare_run, body)
    except (ValueError, OSError, YAMLError):
        raise InvalidRunSettings from None
    run_id = await submit_run(sessions, snapshot, body.run_id)
    row = await require_run(sessions, run_id)
    return run_id, row["status"]


@asynccontextmanager
async def _maintain_run_lease(sessions, run_id, version):
    owner = asyncio.current_task()

    async def check_active():
        await heartbeat(sessions, run_id, version)

    async def pulse():
        while True:
            await asyncio.sleep(30)
            try:
                await check_active()
            except Exception:
                # Stop before the worker can continue after losing the lease.
                owner.cancel()
                return

    task = asyncio.create_task(pulse())
    try:
        yield check_active
    finally:
        task.cancel()
        with suppress(asyncio.CancelledError):
            await task


async def run_pipeline(crawl_settings=None, filter_settings=None, *, run_id=None,
                       session_factory=None, transport=None, ai_client=None, ai_transport=None,
                       enrich=None, filter_concurrency=None, prepared_snapshot=None):
    engine = None
    if session_factory is None:
        engine = create_async_engine(DatabaseSettings().sqlalchemy_url(), hide_parameters=True)
        session_factory = async_sessionmaker(engine)
    run_id = run_id or uuid4()
    version = None
    cleanup = AsyncExitStack()
    if engine is not None:
        cleanup.push_async_callback(engine.dispose)
    failures = None
    stage = "run"

    try:
        crawl = crawl_settings or CrawlSettings.load()
        if prepared_snapshot is None:
            settings = filter_settings or load_filter_settings()
        else:
            # API 已保存业务配置、兴趣及提示词；worker 不再按当前文件生成新快照。
            connection = load_ai_connection() if prepared_snapshot["filter"]["mode"] == "ai" else None
            settings = FilterSettings.model_construct(ai=AISettings(
                url=connection.url if connection else None,
                api_key=connection.api_key if connection else ""))
        if filter_concurrency is None:
            filter_concurrency = settings.ai.concurrency
        if type(filter_concurrency) is not int or not 1 <= filter_concurrency <= 8:
            raise ValueError("filter_concurrency 必须是 1 到 8 的整数")
        snapshot = prepared_snapshot
        if snapshot is None:
            snapshot = snapshot_settings(crawl, settings)
            snapshot["execution"] = {"filter_concurrency": filter_concurrency}
        version, snapshot = await begin_run(session_factory, run_id, snapshot)
        if version is None:
            return await load_run(session_factory, run_id)
        failures = RunFailureRecorder(session_factory, run_id, version)
        crawl, settings = restore_settings(snapshot, crawl, settings)
        filter_concurrency = snapshot.get("execution", {}).get("filter_concurrency", filter_concurrency)
        check_active = await cleanup.enter_async_context(
            _maintain_run_lease(session_factory, run_id, version)
        )
        stage = "filter"
        collected, outcomes, timing = await select_news(
            crawl, settings, snapshot, concurrency=filter_concurrency, transport=transport,
            ai_client=ai_client, ai_transport=ai_transport, on_error=failures.report,
            check_active=check_active,
        )
        stage = "statistics"
        summary = statistics(collected, outcomes)
        summary["execution"] = timing
        logger.info(json.dumps({"event": "statistics", "run_id": str(run_id), **summary}, ensure_ascii=False))
        stage = "enrich"
        accepted, enrichment_summary = await enrich_news(collected, outcomes, enrich, failures.report)
        stage = "statistics"
        errors, error_store_failed = await failures.summarize()
        result = build_run_result(
            run_id, collected, outcomes, accepted, summary, enrichment_summary, errors, error_store_failed,
        )
        stage = "store"
        return await commit_run(session_factory, run_id, version, result, collected.items)
    except asyncio.CancelledError:
        if version is not None:
            try:
                await failures.report(ProcessingError(stage="run", error_type="Cancelled", reason="处理被取消"))
                await mark_terminated(session_factory, run_id, version, "cancelled")
            except Exception:
                logger.warning(json.dumps({"event": "cancel_or_lease_lost", "run_id": str(run_id)}))
        raise
    except ExecutionLost:
        raise
    except Exception as exc:
        # TaskGroup 会包装执行权异常；取消/失权是控制信号，不能误报业务失败。
        if isinstance(exc, ExceptionGroup) and exc.subgroup(ExecutionLost) is not None:
            raise ExecutionLost("批次已取消或执行权已失效") from None
        if version is not None:
            try:
                cause = exc.__cause__ if isinstance(exc, RunStorageError) and exc.__cause__ else exc
                sqlstate = getattr(getattr(cause, "orig", None), "sqlstate", None)
                reason = f"本轮在 {stage} 阶段未完成，错误类型 {type(cause).__name__}"
                if sqlstate:
                    reason += f"，SQLSTATE {sqlstate}"
                await failures.report(ProcessingError(stage=stage, error_type=type(cause).__name__, reason=reason))
                await mark_terminated(session_factory, run_id, version, "failed")
            except Exception:
                logger.error(json.dumps({"event": "run_failure_not_saved", "run_id": str(run_id),
                                         "stage": stage, "error_type": type(exc).__name__}))
        raise
    finally:
        await cleanup.aclose()


async def collect_and_store(settings=None, *, run_id=None, session_factory=None,
                            transport=None, filter_settings=None, ai_client=None, filter_concurrency=None):
    """执行整轮处理，只返回任务 ID 和采集结果。"""
    result = await run_pipeline(settings, filter_settings, run_id=run_id,
        session_factory=session_factory, transport=transport, ai_client=ai_client,
        filter_concurrency=filter_concurrency)
    return result.run_id, result.collection


async def _report_filter_failure(report, error, source_id, indices):
    if report is not None:
        # The final merged order is unknown; use 1-based positions within this source.
        await report(error.model_copy(update={
            "source_id": source_id,
            "item_positions": [indices[i] + 1 for i in error.item_positions],
            "key": "source_filter",
        }))


async def _collect_and_filter(crawl, settings, *, ai=None, plan=None, search_queries=None, interests=None,
                             transport=None, on_error=None, concurrency=2, check_active=None, search_planner=None):
    # 关键词计算很轻，单消费者足够；AI 批次才使用并发请求。
    workers = concurrency if settings.mode == "ai" else 1
    queue = asyncio.Queue(maxsize=workers * 2)
    batch_size = settings.ai.batch_size if settings.mode == "ai" else 64
    by_source = {}
    existing_tags = set()
    started = perf_counter()
    metrics = {"filter_concurrency": workers, "filter_batch_size": batch_size, "filter_batches": 0}
    active = peak = 0
    planning = None

    async def source_ready(source_index, source, items):
        for rows in batches(items, batch_size):
            indices = [row["index"] for row in rows]
            await queue.put((source_index, source.id, items, indices))

    async def produce():
        result = await collect_result(crawl, transport=transport, on_error=on_error,
                                      search_queries=search_queries, on_source=source_ready, search_plan=planning)
        metrics["collection_completed_after_seconds"] = round(perf_counter() - started, 6)
        for _ in range(workers):
            await queue.put(None)
        return result

    async def consume():
        nonlocal active, peak
        while True:
            job = await queue.get()
            try:
                if job is None:
                    return
                source_index, source_id, originals, indices = job
                if check_active:
                    await check_active()
                metrics.setdefault("first_filter_started_after_seconds", round(perf_counter() - started, 6))
                metrics["filter_batches"] += 1
                active += 1
                peak = max(peak, active)

                async def report(error):
                    await _report_filter_failure(on_error, error, source_id, indices)

                try:
                    current_plan = plan
                    if planning is not None and planning.done() and not planning.cancelled():
                        _, current_plan = planning.result()
                    outcomes = await filter_items(
                        [originals[i] for i in indices], settings, ai=ai, plan=current_plan,
                        on_error=report, interests=interests, existing_tags=existing_tags)
                    by_source.setdefault(source_index, []).extend(
                        outcome.model_copy(update={"index": indices[outcome.index]}) for outcome in outcomes)
                finally:
                    active -= 1
            finally:
                queue.task_done()

    async with asyncio.TaskGroup() as group:
        if search_planner is not None:
            planning = group.create_task(search_planner())
        producer = group.create_task(produce())
        for _ in range(workers):
            group.create_task(consume())
        collected = await producer

    # 完成顺序可以不同；数据库编号仍按配置来源顺序及来源内顺序稳定排列。
    outcomes = []
    offset = 0
    for source_index, source in enumerate(collected.source_results):
        source_outcomes = sorted(by_source.get(source_index, []), key=lambda x: x.index)
        if [o.index for o in source_outcomes] != list(range(source.valid_count)):
            raise ValueError("来源筛选结果未完整对应输入")
        outcomes.extend(o.model_copy(update={"index": offset + o.index}) for o in source_outcomes)
        offset += source.valid_count
    metrics["filter_peak_concurrency"] = peak
    metrics["filter_completed_after_seconds"] = round(perf_counter() - started, 6)
    return collected, outcomes, metrics


async def select_news(crawl, settings, snapshot, *, concurrency, transport,
                       ai_client, ai_transport, on_error, check_active):
    ai = ai_client
    if settings.mode == "ai":
        ai = create_ai_client(settings.ai, client=ai_client, transport=ai_transport,
                              prompt=snapshot["prompt"], concurrency=concurrency)

    async def search_planner():
        return await plan_search(crawl, settings, ai=ai, on_error=on_error,
                                 interests=snapshot["interests"])
    return await _collect_and_filter(
        crawl, settings, ai=ai, interests=snapshot["interests"], search_planner=search_planner,
        transport=transport, on_error=on_error, concurrency=concurrency, check_active=check_active)


def statistics(collection, outcomes):
    sources = collection.source_results
    counts = Counter(o.status for o in outcomes)
    return {
        "sources": len(sources),
        "source_statuses": dict(Counter(s.status for s in sources)),
        "raw_count_known": sum(s.raw_count_known for s in sources),
        "raw_count_unknown_sources": [s.source_id for s in sources if s.raw_unknown_count],
        "examined_count": sum(s.examined_count for s in sources),
        "parse_valid_count": len(collection.items),
        "parse_skipped_count": sum(s.skipped_count for s in sources),
        "search_duplicate_count": sum(s.duplicate_count for s in sources),
        "search_limited_count": sum(s.limited_count for s in sources),
        "unexamined_count_known": sum(s.unexamined_count_known for s in sources),
        "filter": {status: counts[status] for status in ("kept", "excluded", "insufficient", "failed")},
        "excluded_by_reason": dict(Counter(
            " ".join(outcome.reason.split()) for outcome in outcomes if outcome.status == "excluded"
        )),
    }


def build_run_result(run_id, collected, outcomes, accepted, summary, enrichment_summary,
                     errors, error_store_failed):
    summary.update(enrichment_summary)
    summary.update({
        "error_records": len(errors), "error_store_failed": error_store_failed,
        "errors_by_stage": dict(Counter(e.stage for e in errors)),
    })
    if not accepted:
        summary["empty_reason"] = (
            "no_matching_items" if any(o.status == "excluded" for o in outcomes) else
            "information_insufficient" if any(o.status == "insufficient" for o in outcomes) else
            "processing_failed" if errors else "sources_empty")
    any_completed = any(o.status != "failed" for o in outcomes) or (
        not collected.items and any(s.status == "empty" for s in collected.source_results))
    status = "partial" if (errors or error_store_failed) and any_completed else (
        "failed" if errors or error_store_failed else "success")
    return PipelineResult(run_id=run_id, status=status,
        collection=collected.model_copy(update={"items": accepted}),
        outcomes=outcomes, errors=errors, summary=summary)


async def commit_run(sessions, run_id, version, result, originals):
    try:
        await finish_run(sessions, run_id, version, result, originals)
    except ExecutionLost:
        raise
    except Exception as exc:
        raise RunStorageError("最终入库失败，正常数据已回滚；查看本轮错误记录或应用日志") from exc
    logger.info(json.dumps({"event": "run_finished", "run_id": str(run_id), "status": result.status}))
    # 返回主表的当前完整资讯，与以后查询同一批次的读取方式一致。
    try:
        saved = await load_run(sessions, run_id)
        if saved is not None:
            return saved.model_copy(update={"errors": result.errors})
    except Exception:
        logger.warning(json.dumps({"event": "committed_result_read_failed", "run_id": str(run_id)}))
    return result.model_copy(update={
        "outcomes": [outcome for outcome in result.outcomes if outcome.status != "excluded"],
    })
