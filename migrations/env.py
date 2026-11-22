from alembic import context
from sqlalchemy import create_engine, pool

from config.connect_settings import DatabaseSettings
from src.db.database import metadata


def migrate(connection):
    context.configure(connection=connection, target_metadata=metadata)
    with context.begin_transaction():
        context.run_migrations()


if context.is_offline_mode():
    context.configure(
        dialect_name="postgresql", target_metadata=metadata, literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )
    with context.begin_transaction():
        context.run_migrations()
else:
    connection = context.config.attributes.get("connection")
    if connection is not None:
        migrate(connection)
    else:
        engine = create_engine(DatabaseSettings().sqlalchemy_url(), poolclass=pool.NullPool, hide_parameters=True)
        try:
            with engine.connect() as connection:
                migrate(connection)
        finally:
            engine.dispose()
