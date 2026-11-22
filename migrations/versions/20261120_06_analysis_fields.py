"""Add accurate observation times, body fingerprints and analysis metadata."""
from alembic import op
import sqlalchemy as sa

revision = "20261120_06"
down_revision = "20261120_05"
branch_labels = None
depends_on = None

COMMENTS = {'news_items': {'id': '资讯身份：按 URL 与标题生成 UUID5，不含来源；正文变化不改变身份。',
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

def upgrade():
    op.add_column("news_items", sa.Column("content_length", sa.Integer(), sa.Computed("char_length(content)", persisted=True), nullable=False))
    op.add_column("collection_observations", sa.Column("observed_at", sa.DateTime(timezone=True), nullable=True))
    op.add_column("collection_observations", sa.Column("content_hash", sa.Text(), nullable=True))
    op.create_check_constraint("ck_news_filter_status", "news_items", "filter_status IN ('kept','excluded','insufficient','failed')")
    op.create_index("ix_news_items_last_seen_at", "news_items", ["last_seen_at"])
    op.create_index("ix_news_items_filter_published", "news_items", ["filter_status", "published_at"])
    op.create_index("ix_observations_item_source_time", "collection_observations", ["news_item_id", "source_id", "observed_at"])
    for table, columns in COMMENTS.items():
        for name, description in columns.items():
            op.alter_column(table, name, comment=description)

def downgrade():
    for table, columns in COMMENTS.items():
        for name in columns:
            op.alter_column(table, name, comment=None)
    op.drop_index("ix_observations_item_source_time", table_name="collection_observations")
    op.drop_index("ix_news_items_filter_published", table_name="news_items")
    op.drop_index("ix_news_items_last_seen_at", table_name="news_items")
    op.drop_constraint("ck_news_filter_status", "news_items", type_="check")
    op.drop_column("collection_observations", "content_hash")
    op.drop_column("collection_observations", "observed_at")
    op.drop_column("news_items", "content_length")
