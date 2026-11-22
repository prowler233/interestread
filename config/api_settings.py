"""HTTP 访问控制与后台投递设置；不改写现有 YAML。"""
from pathlib import Path
from pydantic import Field, RedisDsn, SecretStr, TypeAdapter, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

ENV_FILE = Path(__file__).resolve().parents[1] / ".env"

class APISettings(BaseSettings):
    model_config = SettingsConfigDict(env_file=ENV_FILE, env_prefix="API_", extra="ignore", hide_input_in_errors=True)
    auth_enabled: bool = False
    key: SecretStr = SecretStr("")

    @model_validator(mode="after")
    def require_key(self):
        if self.auth_enabled and not self.key.get_secret_value().strip():
            raise ValueError("启用 API 认证时必须设置 API_KEY")
        return self

class QueueSettings(BaseSettings):
    model_config = SettingsConfigDict(env_file=ENV_FILE, env_prefix="TASK_", extra="ignore", hide_input_in_errors=True)
    redis_url: SecretStr = Field(validation_alias="REDIS_URL")
    queue_name: str = Field(default="interetread", min_length=1, max_length=100, pattern=r"^[A-Za-z0-9_-]+$")
    worker_concurrency: int = Field(default=1, ge=1)
    poll_interval: float = Field(default=2, ge=0.1, le=60)
    redispatch_seconds: int = Field(default=30, ge=5, le=300)

    @field_validator("redis_url")
    @classmethod
    def valid_redis(cls, value):
        try:
            TypeAdapter(RedisDsn).validate_python(value.get_secret_value())
        except ValueError:
            raise ValueError("REDIS_URL 格式无效") from None
        return value
