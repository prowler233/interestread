"""生成和恢复单轮任务的配置快照。快照由任务服务写入 processing_runs，密钥由执行时配置提供。
生成任务的时候生成快照，提交任务的时候把快照提交到表中"""
from pydantic import HttpUrl, TypeAdapter

from config.crawl_settings import CrawlSettings
from src.ai_client import AISettings


def snapshot_settings(crawl, settings):
    crawl = crawl.model_copy(update={"newsnow": crawl.newsnow.model_copy(update={
        "api_url": TypeAdapter(HttpUrl).validate_python(crawl.newsnow.api_url)})})
    snapshot = {
        "crawl": crawl.model_dump(mode="json", exclude={"newsnow": {"headers"}}),
        "filter": settings.model_dump(mode="json", exclude={"ai": {"api_key", "url"}}),
        "interests": settings.ai.read_interests() if settings.mode == "ai" else None,
        "prompt": settings.ai.read_prompt() if settings.mode == "ai" else None,
    }
    return snapshot

def restore_settings(snapshot, crawl, settings):
    restored_crawl = crawl.model_copy(update={key: TypeAdapter(CrawlSettings.model_fields[key].annotation).validate_python(value)
        for key, value in snapshot["crawl"].items()})
    restored_crawl.newsnow.headers = crawl.newsnow.headers
    ai_data = {**snapshot["filter"]["ai"], "api_key": settings.ai.api_key, "url": settings.ai.url}
    restored = settings.model_copy(update={
        **{k: v for k, v in snapshot["filter"].items() if k not in ("ai", "keyword")},
        "ai": AISettings.model_validate(ai_data),
        "keyword": type(settings.keyword).model_validate(snapshot["filter"]["keyword"]),
    })
    return restored_crawl, restored
