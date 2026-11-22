"""采集。按配置并发读取各来源，将原始响应交给解析器。

一个来源处理完成就交付有效资讯，并分别上报取数失败和解析失败。"""
import asyncio
import json
import logging
from dataclasses import dataclass
from typing import Literal

import httpx
from pydantic import BaseModel, Field

from config.crawl_settings import CrawlSettings, NewsNowSource, RAJSource
from src.db.failures import ProcessingError
from src.pipeline.parse import Channel, NewsItem, ParseStats, SourceError, parse_source


class FetchFailed(Exception):
    def __init__(self, error: Exception, attempts: int):
        super().__init__()
        self.error = error
        self.attempts = attempts


async def _download(client, settings, url, **kwargs):
    for attempt in range(1, settings.retries + 2):
        try:
            async with client.stream("GET", url, **kwargs) as response:
                response.raise_for_status()
                body = bytearray()
                async for chunk in response.aiter_bytes():
                    body.extend(chunk)
                    if len(body) > settings.max_response_bytes:
                        raise SourceError("响应体超过大小限制")
                return bytes(body), str(response.url), attempt
        except (httpx.HTTPError, OSError, ValueError) as exc:
            retryable = isinstance(exc, httpx.TransportError) or (
                isinstance(exc, httpx.HTTPStatusError)
                and (exc.response.status_code == 429 or exc.response.status_code >= 500)
            )
            if not retryable or attempt > settings.retries:
                raise FetchFailed(exc, attempt) from exc
            await asyncio.sleep(min(2 ** (attempt - 1), 10))


async def _opencli(settings, source):
    query = (source.query or "").strip()
    if source.needs_query and not query:
        raise SourceError("OpenCLI 搜索来源缺少 query 配置")
    args = [query if arg == "{query}" else arg for arg in source.args]
    if source.limit_flag is not None:
        args.extend((source.limit_flag, str(source.limit)))
    process = await asyncio.create_subprocess_exec(
        "opencli", source.site, source.command, *args, "-f", "json",
        stdin=asyncio.subprocess.DEVNULL, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL,
    )
    try:
        async with asyncio.timeout(settings.platforms.timeout):
            output = bytearray()
            while chunk := await process.stdout.read(65536):
                output.extend(chunk)
                if len(output) > settings.max_response_bytes:
                    raise SourceError("OpenCLI 输出超过大小限制")
            exit_code = await process.wait()
            if exit_code != 0:
                raise SourceError(f"OpenCLI 退出码 {exit_code}")
            return bytes(output)
    finally:
        if process.returncode is None:
            try:
                process.kill()
            except ProcessLookupError:
                pass
            await process.communicate()


async def fetch_source(client, settings, source, opencli_lock) -> tuple[bytes, str | None, int]:
    """Return the source body, effective URL if available, and attempt count."""
    if isinstance(source, NewsNowSource):
        return await _download(client, settings, str(settings.newsnow.api_url),
                               params={"id": source.id}, headers=settings.newsnow.headers)
    if isinstance(source, RAJSource):
        return await _download(client, settings, str(source.url))

    # OpenCLI sources can share a browser session.
    async with opencli_lock:
        for attempt in range(1, settings.retries + 2):
            try:
                return await _opencli(settings, source), None, attempt
            except Exception as exc:
                if not isinstance(exc, TimeoutError) or attempt > settings.retries:
                    raise FetchFailed(exc, attempt) from exc
                await asyncio.sleep(min(2 ** (attempt - 1), 10))


logger = logging.getLogger(__name__)


class CollectionFailure(BaseModel):
    source_id: str
    channel: Channel
    stage: Literal["fetch", "parse"]
    error_type: str
    reason: str
    attempts: int
    http_status: int | None = None


class CollectionSourceResult(BaseModel):
    source_id: str
    channel: Channel
    status: Literal["success", "empty", "failed"]
    raw_count: int | None
    valid_count: int
    skipped_count: int
    skip_reasons: dict[str, int]
    examined_count: int = 0
    duplicate_count: int = 0
    limited_count: int = 0
    raw_count_known: int = 0
    raw_unknown_count: int = 0
    unexamined_count_known: int = 0


class CollectionResult(BaseModel):
    items: list[NewsItem] = Field(default_factory=list)
    failures: list[CollectionFailure] = Field(default_factory=list)
    empty_source_ids: list[str] = Field(default_factory=list)
    source_results: list[CollectionSourceResult] = Field(default_factory=list)


@dataclass
class _CollectedSource:
    items: list[NewsItem]
    summary: CollectionSourceResult
    failure: CollectionFailure | None


def _failure_reason(exc):
    if isinstance(exc, SourceError):
        return str(exc)
    if isinstance(exc, httpx.HTTPStatusError):
        return f"HTTP {exc.response.status_code}"
    if isinstance(exc, (httpx.TimeoutException, TimeoutError)):
        return "请求或命令超时"
    if isinstance(exc, (json.JSONDecodeError, UnicodeDecodeError)):
        return "返回内容无法解析为 JSON"
    if isinstance(exc, FileNotFoundError):
        return "找不到 OpenCLI 命令"
    return type(exc).__name__


async def _collect_source(client, settings, source, opencli_lock, on_error=None) -> _CollectedSource:
    attempts = 1
    stage = "fetch"
    channel = {"newsnow": "newsnow", "RAJ": "feed", "platforms": "opencli"}[source.adapter]
    stats = ParseStats()
    items = []
    failure = None
    try:
        body, base_url, attempts = await fetch_source(client, settings, source, opencli_lock)
        stage = "parse"
        items = parse_source(source, body, base_url, stats=stats)
    except Exception as exc:
        if isinstance(exc, FetchFailed):
            attempts = exc.attempts
            error = exc.error
        else:
            error = exc
        reason = _failure_reason(error)
        logger.warning("来源 %s 采集失败（%s）", source.id, reason)
        failure = CollectionFailure(
            source_id=source.id, channel=channel, stage=stage, attempts=attempts,
            error_type=type(error).__name__, reason=reason,
            http_status=error.response.status_code if isinstance(error, httpx.HTTPStatusError) else None,
        )

    if failure is not None:
        status = "failed"
    elif not items:
        status = "empty"
        logger.warning("来源 %s 没有可用条目", source.id)
    else:
        status = "success"

    summary = CollectionSourceResult(
        source_id=source.id, channel=channel, status=status, raw_count=stats.raw_count,
        valid_count=len(items), skipped_count=stats.skipped_count, skip_reasons=dict(stats.skip_reasons),
        examined_count=stats.examined_count,
        raw_count_known=stats.raw_count or 0,
        raw_unknown_count=int(stats.raw_count is None),
        unexamined_count_known=max(0, (stats.raw_count or 0) - stats.examined_count),
    )
    if on_error:
        for position, reasons in stats.issues:
            await on_error(ProcessingError(stage="parse", source_id=source.id,
                item_positions=[position], error_type="InvalidItem",
                reason="、".join(reasons), key=f"item:{position}"))
        if failure:
            await on_error(ProcessingError(stage=failure.stage, source_id=source.id,
                error_type=failure.error_type, reason=failure.reason,
                attempts=failure.attempts, http_status=failure.http_status, key="source"))
    return _CollectedSource(items=items, summary=summary, failure=failure)


async def _collect_search_source(client, settings, source, lock, queries, on_error):
    if not queries:
        summary = CollectionSourceResult(source_id=source.id, channel="opencli", status="failed",
            raw_count=None, valid_count=0, skipped_count=0, skip_reasons={}, raw_unknown_count=1)
        return _CollectedSource(items=[], summary=summary, failure=CollectionFailure(
            source_id=source.id, channel="opencli", stage="fetch",
            error_type="SearchPlanFailed", reason="搜索条件生成失败", attempts=0))
    results = []
    items = []
    seen = set()
    duplicates = limited = 0
    for number, query in enumerate(queries):
        async def report(error):
            if on_error:
                await on_error(error.model_copy(update={"key": f"query:{number}:{error.key}"}))
        result = await _collect_source(client, settings,
            source.model_copy(update={"query": query}), lock, report)
        results.append(result)
        for item in result.items:
            key = (str(item.url), item.title)
            if key in seen:
                duplicates += 1
            else:
                seen.add(key)
                if len(items) < source.limit:
                    items.append(item)
                else:
                    limited += 1
        if len(items) >= source.limit:
            break
    from collections import Counter
    reasons = Counter()
    for result in results:
        reasons.update(result.summary.skip_reasons)
    failures = [r.failure for r in results if r.failure]
    raw = [r.summary.raw_count for r in results]
    summary = CollectionSourceResult(source_id=source.id, channel="opencli",
        status="success" if items else "failed" if failures else "empty",
        raw_count=None if any(n is None for n in raw) else sum(raw),
        valid_count=len(items), skipped_count=sum(r.summary.skipped_count for r in results),
        examined_count=sum(r.summary.examined_count for r in results),
        duplicate_count=duplicates, limited_count=limited, skip_reasons=dict(reasons),
        raw_count_known=sum(r.summary.raw_count_known for r in results),
        raw_unknown_count=sum(r.summary.raw_unknown_count for r in results),
        unexamined_count_known=sum(r.summary.unexamined_count_known for r in results))
    return _CollectedSource(items=items, summary=summary, failure=failures[0] if failures else None)


async def collect_result(settings: CrawlSettings | None = None, *, transport=None, on_error=None, search_queries=None, on_source=None, search_plan=None) -> CollectionResult:
    """返回本轮可用资讯、最终失败及无可用条目的来源。"""
    settings = settings or CrawlSettings.load()
    sources = [source for source in settings.iter_sources() if source.enabled]
    feeds = []
    other_sources = []
    for index, source in enumerate(sources):
        group = feeds if isinstance(source, RAJSource) else other_sources
        group.append((index, source))
    # 固定来源优先领取，避免等待搜索词的来源占满共享采集任务。
    def waits_for_plan(source):
        return (search_plan is not None and source.adapter == "platforms"
                and source.needs_query and not (source.query or "").strip())
    other_sources.sort(key=lambda entry: waits_for_plan(entry[1]))
    results: dict[int, _CollectedSource] = {}
    opencli_lock = asyncio.Lock()
    async with httpx.AsyncClient(
        timeout=settings.timeout, headers={"User-Agent": settings.user_agent},
        follow_redirects=True, transport=transport,
    ) as client:
        async def collect_sources(pending):
            # 每次领取一个来源。领取时没有 await，多个协程不会取到同一条。
            for position, (index, source) in enumerate(pending):
                if position > 0 and settings.interval:
                    await asyncio.sleep(settings.interval)
                queries = search_queries
                if waits_for_plan(source):
                    queries, _ = await search_plan
                if queries is not None and source.id in queries:
                    results[index] = await _collect_search_source(
                        client, settings, source, opencli_lock, queries[source.id], on_error)
                else:
                    results[index] = await _collect_source(client, settings, source, opencli_lock, on_error)
                if on_source is not None:
                    await on_source(index, source, results[index].items)

        async with asyncio.TaskGroup() as tasks:
            for group, limit in (
                (feeds, settings.RAJ.concurrency),
                (other_sources, settings.concurrency),
            ):
                pending = iter(group)
                for _ in range(min(limit or len(group), len(group))):
                    tasks.create_task(collect_sources(pending))

    batch = CollectionResult()
    for index in range(len(sources)):
        result = results[index]
        batch.items.extend(result.items)
        batch.source_results.append(result.summary)
        if result.failure is not None:
            batch.failures.append(result.failure)
        elif result.summary.status == "empty":
            batch.empty_source_ids.append(result.summary.source_id)
    return batch


async def collect(settings: CrawlSettings | None = None, *, transport=None) -> list[NewsItem]:
    return (await collect_result(settings, transport=transport)).items


async def collect_json(settings: CrawlSettings | None = None, *, transport=None) -> str:
    items = await collect(settings, transport=transport)
    return json.dumps([item.model_dump(mode="json") for item in items], ensure_ascii=False, indent=2) + "\n"
