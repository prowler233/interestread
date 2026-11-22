"""Durable API submissions and task delivery."""
from alembic import op
import sqlalchemy as sa

revision = "20261120_07"
down_revision = "20261120_06"
branch_labels = None
depends_on = None

def upgrade():
    op.drop_constraint("ck_processing_run_status", "processing_runs", type_="check")
    op.create_check_constraint("ck_processing_run_status", "processing_runs",
                              "status IN ('queued','running','success','partial','failed','cancelled')")
    op.create_table("task_outbox",
        sa.Column("run_id", sa.Uuid(), sa.ForeignKey("processing_runs.id"), primary_key=True,
                  comment="待投递运行的身份，与任务记录同一事务创建。"),
        sa.Column("available_at", sa.DateTime(timezone=True), nullable=False, comment="允许再次投递的时间。"),
        sa.Column("dispatch_count", sa.Integer(), nullable=False, comment="投递尝试次数，包括发送失败。"),
        sa.Column("last_dispatched_at", sa.DateTime(timezone=True), comment="最近一次尝试发送到 Redis 的时间。"),
        sa.Column("last_error", sa.Text(), comment="最近投递异常类型；不保存连接地址或密码。"))
    op.create_index("ix_task_outbox_available_at", "task_outbox", ["available_at"])

def downgrade():
    # 仍有排队任务时拒绝降级，避免静默丢弃任务。
    if op.get_bind().execute(sa.text("SELECT count(*) FROM processing_runs WHERE status='queued'")).scalar_one():
        raise RuntimeError("仍有排队任务，请先处理或取消，再降级")
    op.drop_table("task_outbox")
    op.drop_constraint("ck_processing_run_status", "processing_runs", type_="check")
    op.create_check_constraint("ck_processing_run_status", "processing_runs",
                              "status IN ('running','success','partial','failed','cancelled')")
