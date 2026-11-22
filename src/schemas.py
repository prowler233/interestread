"""API 请求和响应模型。集中定义 body 和 response_model 的字段与校验规则。"""
from datetime import datetime
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field

from config.filter_settings import KeywordSettings
from src.db.failures import ProcessingError
from src.pipeline.collect import CollectionResult
from src.pipeline.filter import FilterOutcome


RunStatus = Literal["queued", "running", "success", "partial", "failed", "cancelled"]
FilterStatus = Literal["kept", "excluded", "insufficient", "failed"]

class RunCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")
    run_id: UUID | None = Field(default=None, description="可选幂等 ID；同一 ID 重复提交只返回已有任务")
    source_ids: list[str] | None = Field(default=None, min_length=1, max_length=200,
                                        description="从服务器已启用来源中选择；省略使用全部已启用来源")
    filter_mode: Literal["keyword", "ai"] | None = None
    keyword: KeywordSettings | None = None
    ai_concurrency: int | None = Field(default=None, ge=1, le=8)
    ai_batch_size: int | None = Field(default=None, ge=1, le=200)

class RunAccepted(BaseModel):
    run_id: UUID
    status: RunStatus
    status_url: str
    result_url: str

class RunRead(BaseModel):
    run_id: UUID
    status: RunStatus
    started_at: datetime
    finished_at: datetime | None
    collected_at: datetime | None
    lease_expires_at: datetime
    summary: dict
    source_results: list[dict]
    source_failures: list[dict]
    error_count: int
    dispatch_count: int
    dispatch_error: str | None

class NewsRead(BaseModel):
    id: UUID
    title: str
    url: str
    channel: str
    source_id: str
    source_item_id: str | None
    source_excerpt: str
    content: str
    content_length: int
    author: str | None
    published_at: datetime | None
    rank: int | None
    source_position: int | None
    source_updated_at: datetime | None
    version_summary: str
    filter_status: FilterStatus
    filter_reason: str
    tags: list[str]
    ai_summary: str | None
    last_run_id: UUID
    first_seen_at: datetime
    last_seen_at: datetime

class ObservationRead(BaseModel):
    run_id: UUID
    position: int
    observed_at: datetime | None
    content_hash: str | None
    news_item_id: UUID
    channel: str
    source_id: str
    source_item_id: str | None
    rank: int | None
    source_position: int | None
    filter_status: FilterStatus
    filter_reason: str
    tags: list[str]
    ai_summary: str | None

class Page[T](BaseModel):
    items: list[T]
    total: int
    limit: int
    offset: int

class ErrorRead(ProcessingError):
    id: UUID
    run_id: UUID
    occurred_at: datetime


class PipelineResult(BaseModel):
    run_id: UUID
    status: Literal["success", "partial", "failed", "cancelled"]
    collection: CollectionResult
    outcomes: list[FilterOutcome] = Field(default_factory=list)
    errors: list[ProcessingError] = Field(default_factory=list)
    summary: dict = Field(default_factory=dict)
