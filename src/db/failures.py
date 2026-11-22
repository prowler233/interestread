"""接收各阶段上报的错误，将错误明细独立写入 processing_errors。

记录器负责去重、暂存写入失败的记录，并在最终提交前尝试补存。"""
import json
import logging
from typing import Literal
from uuid import NAMESPACE_URL, uuid5

from pydantic import BaseModel, Field
from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert

from src.db.database import ExecutionLost, fetch_page, guard, now, processing_errors

logger = logging.getLogger(__name__)


class ProcessingError(BaseModel):
    stage: Literal["fetch", "parse", "search", "filter", "enrich", "statistics", "store", "run"]
    source_id: str | None = None
    item_positions: list[int] = Field(default_factory=list)
    error_type: str
    reason: str
    attempts: int = 1
    http_status: int | None = None
    key: str = ""


async def insert_failure(session, run_id, error, *, occurred_at, error_id=None):
    """Write a record using the caller's transaction and execution checks."""
    payload = error.model_dump(mode="json")
    if error_id is None:
        key = json.dumps(payload, sort_keys=True, ensure_ascii=False)
        error_id = uuid5(NAMESPACE_URL, f"{run_id}:{key}")
    await session.execute(insert(processing_errors).values(
        id=error_id, run_id=run_id, occurred_at=occurred_at, **payload,
    ).on_conflict_do_nothing())


async def errors_for(sessions, run_id):
    async with sessions() as session:
        rows = (await session.execute(select(processing_errors).where(
            processing_errors.c.run_id == run_id).order_by(
                processing_errors.c.occurred_at, processing_errors.c.id))).mappings()
        return [ProcessingError.model_validate(row) for row in rows]


async def list_failures(sessions, run_id, *, limit, offset):
    condition = processing_errors.c.run_id == run_id
    query = select(processing_errors).where(condition).order_by(
        processing_errors.c.occurred_at, processing_errors.c.id,
    )
    count = select(func.count()).select_from(processing_errors).where(condition)
    return await fetch_page(sessions, query, count, limit=limit, offset=offset)


async def save_run_failure(sessions, run_id, version, error):
    async with sessions.begin() as session:
        await guard(session, run_id, version)
        await insert_failure(session, run_id, error, occurred_at=now())


class RunFailureRecorder:
    def __init__(self, sessions, run_id, version):
        self.sessions = sessions
        self.run_id = run_id
        self.version = version
        self.errors = []
        self.pending = []

    async def report(self, error):
        self.errors.append(error)
        try:
            await save_run_failure(self.sessions, self.run_id, self.version, error)
        except ExecutionLost:
            raise
        except Exception:
            self.pending.append(error)
            logger.error(json.dumps({"event": "error_record_not_saved", "run_id": str(self.run_id),
                                     **error.model_dump(mode="json")}, ensure_ascii=False))

    async def summarize(self):
        # Retry independent error writes before committing the business data.
        for error in list(self.pending):
            try:
                await save_run_failure(self.sessions, self.run_id, self.version, error)
                self.pending.remove(error)
            except ExecutionLost:
                raise
            except Exception:
                pass
        error_store_failed = bool(self.pending)
        try:
            persisted = await errors_for(self.sessions, self.run_id)
        except Exception:
            persisted = []
            error_store_failed = True
        unique = {error.model_dump_json(): error for error in [*persisted, *self.errors]}
        return list(unique.values()), error_store_failed
