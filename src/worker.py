"""后台入口。投递进程把任务发给 Celery，worker 收到任务后调用 extraction_service 执行。"""
import asyncio
import json
import logging
from uuid import UUID

from celery import Celery
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from config.api_settings import QueueSettings
from config.connect_settings import DatabaseSettings
from src.db.database import ExecutionLost, now
from src.service.extraction_service import run_pipeline
from src.service.run_service import RunInProgress, fail_unclaimed, get_run_execution, next_dispatch, record_dispatch


logger = logging.getLogger(__name__)
queue_settings = QueueSettings()
celery_app = Celery("interetread", broker=queue_settings.redis_url.get_secret_value())
celery_app.conf.update(
    task_ignore_result=True, task_acks_late=True, task_reject_on_worker_lost=True,
    worker_concurrency=queue_settings.worker_concurrency,
    worker_prefetch_multiplier=1, worker_cancel_long_running_tasks_on_connection_loss=True,
    task_serializer="json", accept_content=["json"], task_default_queue=queue_settings.queue_name,
    broker_connection_timeout=2, broker_transport_options={"socket_timeout": 2, "socket_connect_timeout": 2,
                              "global_keyprefix": queue_settings.queue_name + ":"},
    task_publish_retry=False, broker_connection_retry_on_startup=True,
)

async def execute_run(run_id, *, session_factory=None, transport=None, ai_client=None):
    engine = None
    if session_factory is None:
        engine = create_async_engine(DatabaseSettings().sqlalchemy_url(), hide_parameters=True)
        session_factory = async_sessionmaker(engine)
    try:
        row = await get_run_execution(session_factory, run_id)
        if row is None or row["status"] not in ("queued", "running"):
            return
        try:
            await run_pipeline(run_id=run_id, session_factory=session_factory,
                transport=transport, ai_client=ai_client, prepared_snapshot=row["config_snapshot"])
        except (RunInProgress, ExecutionLost):
            # 重复投递或任务已取消，由数据库状态决定结果。
            return
        except Exception as exc:
            await fail_unclaimed(session_factory, run_id, type(exc).__name__)
            logger.error(json.dumps({"event": "worker_run_failed", "run_id": str(run_id),
                                     "error_type": type(exc).__name__}))
    finally:
        if engine is not None:
            await engine.dispose()

@celery_app.task(name="interetread.execute_run")
def process_run(run_id):
    asyncio.run(execute_run(UUID(run_id)))


async def dispatch_one(sessions, publish, redispatch_seconds=30):
    """Keep the outbox row locked until the publish attempt is recorded."""
    async with sessions.begin() as session:
        timestamp = now()
        run_id = await next_dispatch(session, timestamp)
        if run_id is None:
            return False
        failure = None
        try:
            await publish(run_id)
        except Exception as exc:
            # Only retain the type, as the message may contain credentials.
            failure = type(exc).__name__
        await record_dispatch(session, run_id, timestamp=timestamp,
                              redispatch_seconds=redispatch_seconds, failure=failure)
        return True


async def serve():
    settings = QueueSettings()
    engine = create_async_engine(DatabaseSettings().sqlalchemy_url(), hide_parameters=True)
    sessions = async_sessionmaker(engine)
    async def publish(run_id):
        await asyncio.to_thread(celery_app.send_task, "interetread.execute_run",
                                args=[str(run_id)], task_id=str(run_id))
    try:
        while True:
            try:
                if await dispatch_one(sessions, publish, settings.redispatch_seconds):
                    continue
            except Exception as exc:
                logger.error(json.dumps({"event": "task_dispatch_failed", "error_type": type(exc).__name__}))
            await asyncio.sleep(settings.poll_interval)
    finally:
        await engine.dispose()

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    try:
        asyncio.run(serve())
    except KeyboardInterrupt:
        pass
