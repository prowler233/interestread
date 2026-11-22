"""定义 PostgreSQL 表，供 FastAPI 启动时建库和应用迁移。

提供执行权校验和分页查询等通用能力。"""
from datetime import UTC, datetime
from pathlib import Path

from alembic import command
from alembic.config import Config
from sqlalchemy import (
    CheckConstraint, Column, Computed, DateTime, ForeignKey, Index, Integer, MetaData,
    Table, Text, Uuid, create_engine, pool, select, text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.exc import OperationalError


metadata = MetaData()

news_items = Table("news_items", metadata,
    Column("id", Uuid, primary_key=True),
    Column("title", Text, nullable=False),
    Column("url", Text, nullable=False),
    Column("channel", Text, nullable=False),
    Column("source_id", Text, nullable=False),
    Column("source_item_id", Text),
    Column("source_excerpt", Text, nullable=False),
    Column("content", Text, nullable=False),
    Column("content_length", Integer, Computed("char_length(content)", persisted=True), nullable=False),
    Column("author", Text),
    Column("published_at", DateTime(timezone=True)),
    Column("rank", Integer),
    Column("source_position", Integer),
    Column("source_updated_at", DateTime(timezone=True)),
    Column("version_summary", Text, nullable=False, server_default=""),
    Column("filter_status", Text, nullable=False),
    Column("filter_reason", Text, nullable=False),
    Column("tags", JSONB, nullable=False),
    Column("ai_summary", Text),
    Column("last_run_id", Uuid, ForeignKey("processing_runs.id"), nullable=False),
    Column("first_seen_at", DateTime(timezone=True), nullable=False),
    Column("last_seen_at", DateTime(timezone=True), nullable=False),
    CheckConstraint("filter_status IN (\'kept\',\'excluded\',\'insufficient\',\'failed\')", name="ck_news_filter_status"),
    Index("ix_news_items_last_seen_at", "last_seen_at"),
    Index("ix_news_items_filter_published", "filter_status", "published_at"),
)

collection_observations = Table("collection_observations", metadata,
    Column("run_id", Uuid, ForeignKey("processing_runs.id"), primary_key=True),
    Column("position", Integer, primary_key=True, autoincrement=False),
    Column("observed_at", DateTime(timezone=True)),
    Column("content_hash", Text),
    Column("news_item_id", Uuid, ForeignKey("news_items.id"), nullable=False),
    Column("channel", Text, nullable=False),
    Column("source_id", Text, nullable=False),
    Column("source_item_id", Text),
    Column("rank", Integer),
    Column("source_position", Integer),
    Column("filter_status", Text, nullable=False),
    Column("filter_reason", Text, nullable=False),
    Column("tags", JSONB, nullable=False),
    Column("ai_summary", Text),
    CheckConstraint("filter_status IN ('kept','excluded','insufficient','failed')", name="ck_observation_filter_status"),
    Index("ix_collection_observations_news_item_id", "news_item_id"),
    Index("ix_collection_observations_source_id", "source_id"),
    Index("ix_observations_item_source_time", "news_item_id", "source_id", "observed_at"),
)

# 字段说明同时写入 PostgreSQL，数据库查看工具可直接显示。
FIELD_COMMENTS = {'news_items': {'id': '资讯身份：按 URL 与标题生成 UUID5，不含来源；正文变化不改变身份。',
                'title': '来源提供的标题。',
                'url': '资讯链接；与标题共同确定身份。',
                'channel': '最近一次观察的采集渠道：newsnow、feed 或 opencli。',
                'source_id': '最近一次观察的配置来源 ID；不是资讯唯一身份。',
                'source_item_id': '最近一次观察时来源提供的原始 ID，没有则为空。',
                'source_excerpt': '来源提供的最新非空摘要或描述；不是 AI 摘要。',
                'content': '来源提供的最新非空正文；不保证全文，不保存逐轮旧正文。',
                'content_length': '当前正文字符数，由数据库自动计算；0 表示未取得正文，不代表采集失败。',
                'author': '最新非空作者信息。',
                'published_at': '来源提供的最新非空发布时间；未知为空，不使用采集时间代填。',
                'rank': '最近一次观察的来源内部排名；不能直接当作全网热度。',
                'source_position': '最近一次观察在来源原始列表中的位置，从 1 开始；不是热度排名。',
                'source_updated_at': '来源声明的最新非空更新时间；未知为空，不等于本站采集时间。',
                'version_summary': '来源提供的最新非空版本或更新说明，非本站推断。',
                'filter_status': '最近一次观察的筛选结果：kept 保留、excluded 排除、insufficient 信息不足、failed 筛选失败。',
                'filter_reason': '最近一次筛选判断的原因；执行错误的完整原因见 processing_errors。',
                'tags': '最近一次筛选返回的标签；关键词筛选通常为空。',
                'ai_summary': '最近一次筛选返回的 AI 摘要；未使用 AI 或未保留时可为空。',
                'last_run_id': '提供最近一次观察的运行 ID；正文等非空信息可能沿用更早来源。',
                'first_seen_at': '本站最早成功解析到该资讯的时间；既有无观察时间的数据沿用原记录时间。',
                'last_seen_at': '本站最近成功解析到该资讯的时间，不是最终入库时间。'},
 'collection_observations': {'run_id': '所属采集处理运行 ID。',
                             'position': '本轮统一候选列表的编号，从 1 开始。',
                             'observed_at': '本次来源数据开始统一解析的 UTC 时间；旧记录无法还原时为空。',
                             'content_hash': '本次来源正文归一空白后的 SHA256；无正文为空。只宜与同一资讯同一来源的非空值比较，不保存旧正文。',
                             'news_item_id': '关联完整资讯主表的身份 ID。',
                             'channel': '本次观察的采集渠道。',
                             'source_id': '本次观察的配置来源 ID。',
                             'source_item_id': '本次来源提供的原始条目 ID，没有则为空。',
                             'rank': '本次来源内部排名，未知为空；跨来源排名不直接比较。',
                             'source_position': '本次在来源原始列表中的位置，从 1 开始；不是排名。',
                             'filter_status': '本次筛选结果，和主表的当前结果可以不同。',
                             'filter_reason': '本次筛选判断的原因。',
                             'tags': '本次筛选返回的标签。',
                             'ai_summary': '本次筛选返回的 AI 摘要，不是历史正文。'}}
for table in (news_items, collection_observations):
    for name, description in FIELD_COMMENTS[table.name].items():
        table.c[name].comment = description


processing_runs = Table("processing_runs", metadata,
    Column("id", Uuid, primary_key=True),
    Column("status", Text, nullable=False),
    Column("execution_version", Integer, nullable=False),
    Column("lease_expires_at", DateTime(timezone=True), nullable=False),
    Column("started_at", DateTime(timezone=True), nullable=False),
    Column("finished_at", DateTime(timezone=True)),
    Column("collected_at", DateTime(timezone=True)),
    Column("config_snapshot", JSONB, nullable=False),
    Column("summary", JSONB, nullable=False),
    Column("source_results", JSONB, nullable=False, server_default="[]"),
    Column("source_failures", JSONB, nullable=False, server_default="[]"),
    CheckConstraint("status IN ('queued','running','success','partial','failed','cancelled')", name="ck_processing_run_status"),
)
processing_errors = Table("processing_errors", metadata,
    Column("id", Uuid, primary_key=True),
    Column("run_id", Uuid, ForeignKey("processing_runs.id"), nullable=False, index=True),
    Column("occurred_at", DateTime(timezone=True), nullable=False),
    Column("stage", Text, nullable=False),
    Column("source_id", Text),
    Column("item_positions", JSONB, nullable=False),
    Column("error_type", Text, nullable=False),
    Column("reason", Text, nullable=False),
    Column("attempts", Integer, nullable=False),
    Column("http_status", Integer),
    Column("key", Text, nullable=False),
)

task_outbox = Table("task_outbox", metadata,
    Column("run_id", Uuid, ForeignKey("processing_runs.id"), primary_key=True,
           comment="待投递运行的身份，与任务记录同一事务创建。"),
    Column("available_at", DateTime(timezone=True), nullable=False, comment="允许再次投递的时间。"),
    Column("dispatch_count", Integer, nullable=False, comment="投递尝试次数，包括发送失败。"),
    Column("last_dispatched_at", DateTime(timezone=True), comment="最近一次尝试发送到 Redis 的时间。"),
    Column("last_error", Text, comment="最近投递异常类型；不保存连接地址或密码。"),
    Index("ix_task_outbox_available_at", "available_at"),
)


class ExecutionLost(RuntimeError):
    pass


def now():
    return datetime.now(UTC)


async def guard(session, run_id, version):
    row = (await session.execute(select(processing_runs).where(
        processing_runs.c.id == run_id).with_for_update())).mappings().one()
    if (row["status"] != "running" or row["execution_version"] != version
            or row["lease_expires_at"] <= now()):
        raise ExecutionLost("批次已取消或执行权已失效")
    return row


async def fetch_page(sessions, query, count_query, *, limit, offset):
    async with sessions() as session:
        total = (await session.execute(count_query)).scalar_one()
        rows = (await session.execute(query.limit(limit).offset(offset))).mappings().all()
        return {"items": [dict(row) for row in rows], "total": total, "limit": limit, "offset": offset}


_INITIALIZE_LOCK = 784257110358301


def initialize_database(url):
    """Create the configured database if missing and migrate its schema."""
    engine = create_engine(url, poolclass=pool.NullPool, hide_parameters=True)
    try:
        try:
            connection = engine.connect()
        except OperationalError as exc:
            # libpq may omit SQLSTATE for errors during connection startup.
            if not url.database or getattr(exc.orig, "sqlstate", None) not in (None, "3D000"):
                raise
            _create_database(url)
            connection = engine.connect()
        with connection:
            connection.execute(text("SELECT pg_advisory_lock(:key)"), {"key": _INITIALIZE_LOCK})
            connection.commit()
            root = Path(__file__).resolve().parents[2]
            config = Config(str(root / "alembic.ini"))
            config.attributes["connection"] = connection
            command.upgrade(config, "head")
    finally:
        engine.dispose()


def _create_database(url):
    engine = create_engine(url.set(database="postgres"), poolclass=pool.NullPool,
                           isolation_level="AUTOCOMMIT", hide_parameters=True)
    try:
        with engine.connect() as connection:
            connection.execute(text("SELECT pg_advisory_lock(:key)"), {"key": _INITIALIZE_LOCK})
            exists = connection.scalar(
                text("SELECT 1 FROM pg_database WHERE datname = :name"), {"name": url.database},
            )
            if not exists:
                name = engine.dialect.identifier_preparer.quote_identifier(url.database)
                connection.exec_driver_sql(f"CREATE DATABASE {name}")
    finally:
        engine.dispose()
