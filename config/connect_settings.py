"""从项目根目录 .env 读取数据库、Redis 和 SMTP 连接配置。"""

from pathlib import Path

from pydantic import Field, PostgresDsn, RedisDsn, SecretStr, TypeAdapter, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict
from sqlalchemy.engine import URL, make_url


class DatabaseSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=Path(__file__).resolve().parents[1] / ".env",
        env_file_encoding="utf-8",
        extra="ignore",
        hide_input_in_errors=True,
        populate_by_name=True,
    )

    database_url: SecretStr = Field(validation_alias="DATABASE_URL")

    @field_validator("database_url")
    @classmethod
    def validate_database_url(cls, value: SecretStr) -> SecretStr:
        try:
            TypeAdapter(PostgresDsn).validate_python(value.get_secret_value())
        except ValueError:
            raise ValueError("DATABASE_URL 格式无效") from None
        return value

    def sqlalchemy_url(self) -> URL:
        return make_url(self.database_url.get_secret_value()).set(drivername="postgresql+psycopg")


class ConnectSettings(DatabaseSettings):
    redis_url: SecretStr = Field(validation_alias="REDIS_URL")

    smtp_enabled: bool = False
    smtp_host: str = "smtp.qq.com"
    smtp_port: int = Field(default=465, ge=1, le=65535)
    smtp_use_ssl: bool = True
    smtp_username: str = ""
    smtp_password: SecretStr = SecretStr("")  # QQ 邮箱授权码。
    smtp_from_email: str = ""
    smtp_to_email: str = ""
    smtp_timeout: int = Field(default=10, ge=1, le=120)

    @field_validator("redis_url")
    @classmethod
    def validate_redis_url(cls, value: SecretStr) -> SecretStr:
        try:
            TypeAdapter(RedisDsn).validate_python(value.get_secret_value())
        except ValueError:
            raise ValueError("REDIS_URL 格式无效") from None
        return value

    @model_validator(mode="after")
    def validate_smtp_settings(self):
        if self.smtp_enabled:
            if not all(
                (
                    self.smtp_host,
                    self.smtp_username,
                    self.smtp_password.get_secret_value(),
                    self.smtp_from_email,
                    self.smtp_to_email,
                )
            ):
                raise ValueError("启用 SMTP 前必须填写服务器、账号、授权码和收发邮箱")
            if not self.smtp_use_ssl:
                raise ValueError("当前 QQ 邮箱配置使用 SSL，请启用 SMTP_USE_SSL")
        return self
