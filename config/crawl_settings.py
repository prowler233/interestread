"""三类采集渠道统一配置：YAML 定义来源，.env 保存平台凭据。"""
from pathlib import Path
from typing import Annotated, Literal

import yaml
from pydantic import BaseModel, ConfigDict, Field, HttpUrl, StringConstraints, TypeAdapter, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

ROOT = Path(__file__).resolve().parent


class Source(BaseModel):
    model_config = ConfigDict(extra="forbid")
    id: str = Field(min_length=1)
    name: str
    enabled: bool = True
    limit: int = Field(default=30, ge=1, le=100)


class NewsNowSource(Source):
    @property
    def adapter(self):
        return "newsnow"


class RAJSource(Source):
    url: HttpUrl

    @property
    def adapter(self):
        return "RAJ"


OpenCLIToken = Annotated[str, StringConstraints(pattern=r"^[A-Za-z0-9][A-Za-z0-9_-]*$")]
PlatformField = Literal[
    "id", "title", "url", "source_excerpt", "content", "author", "published_at",
    "source_updated_at", "rank", "version_summary",
]


class PlatformSource(Source):
    site: OpenCLIToken
    command: OpenCLIToken
    args: list[str] = Field(default_factory=list)
    query: str | None = None
    fields: dict[PlatformField, Annotated[str, StringConstraints(min_length=1)]]
    max_limit: int = Field(default=100, ge=1, le=100)
    limit_flag: str | None = "--limit"
    query_key: OpenCLIToken | None = None
    url_template: str | None = None
    title_limit: int | None = Field(default=None, ge=1)

    @property
    def adapter(self):
        return "platforms"

    @property
    def needs_query(self):
        return "{query}" in self.args

    @model_validator(mode="after")
    def check_source(self):
        if self.limit > self.max_limit:
            raise ValueError("采集条数超出来源配置的上限")
        if self.args.count("{query}") > 1 or (self.query_key and not self.needs_query):
            raise ValueError("搜索覆盖键需要在 args 里配置一个独立的 {query} 参数")
        if "title" not in self.fields or ("url" not in self.fields and self.url_template is None):
            raise ValueError("来源必须映射 title，并映射 url 或配置 url_template")
        if self.url_template is not None:
            candidate_url = self.url_template.replace("{id}", "example")
            if (self.url_template.count("{id}") != 1 or "id" not in self.fields
                    or "{" in candidate_url or "}" in candidate_url):
                raise ValueError("url_template 需要一个 {id} 占位符及对应的 id 字段映射")
            url = TypeAdapter(HttpUrl).validate_python(candidate_url)
            if url.username or url.password:
                raise ValueError("链接模板不能包含账号信息")
        return self


class NewsNowSettings(BaseModel):
    model_config = ConfigDict(extra="forbid")
    enabled: bool = True
    api_url: HttpUrl = "https://newsnow.busiyi.world/api/s"
    headers: dict[str, str] = Field(default_factory=dict)
    sources: list[NewsNowSource] = Field(default_factory=list)


class PlatformSettings(BaseModel):
    model_config = ConfigDict(extra="forbid")
    enabled: bool = True
    timeout: float = Field(default=90, gt=0, le=600)
    sources: list[PlatformSource] = Field(default_factory=list)


class RAJSettings(BaseModel):
    model_config = ConfigDict(extra="forbid")
    enabled: bool = True
    concurrency: int | None = Field(default=None, ge=1)
    feeds: list[RAJSource] = Field(default_factory=list)


class SourceCheckSettings(BaseModel):
    model_config = ConfigDict(extra="forbid")
    baseline_dir: Path = ROOT.parent / ".state" / "source_checks"
    field_drop: float = Field(default=0.5, gt=0, le=1)
    count_ratio: float = Field(default=0.2, gt=0, lt=1)
    min_baseline_rows: int = Field(default=10, ge=1)


class CrawlSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="CRAWL_", env_file=ROOT.parent / ".env",
        env_file_encoding="utf-8", extra="ignore", hide_input_in_errors=True,
    )
    enabled: bool = True
    newsnow: NewsNowSettings = Field(default_factory=NewsNowSettings)
    RAJ: RAJSettings = Field(default_factory=RAJSettings)
    platforms: PlatformSettings = Field(default_factory=PlatformSettings)
    checks: SourceCheckSettings = Field(default_factory=SourceCheckSettings)
    timeout: float = Field(default=20, gt=0, le=120)
    retries: int = Field(default=2, ge=0, le=5)
    concurrency: int = Field(default=4, ge=1, le=32)
    interval: float = Field(default=1, ge=0)
    max_response_bytes: int = Field(default=5_000_000, ge=1024, le=20_000_000)
    user_agent: str = "InteretRead/0.1"

    @classmethod
    def settings_customise_sources(
        cls, settings_cls, init_settings, env_settings, dotenv_settings, file_secret_settings
    ):
        # YAML 通过初始化参数传入；环境变量和 .env 优先。
        return env_settings, dotenv_settings, init_settings, file_secret_settings

    @model_validator(mode="after")
    def unique_ids(self):
        ids = [s.id for s in self.iter_sources()]
        if len(ids) != len(set(ids)):
            raise ValueError("来源 id 必须唯一")
        return self

    def iter_sources(self):
        for enabled, sources in (
            (self.newsnow.enabled, self.newsnow.sources),
            (self.RAJ.enabled, self.RAJ.feeds),
            (self.platforms.enabled, self.platforms.sources),
        ):
            for source in sources:
                yield source.model_copy(update={"enabled": self.enabled and enabled and source.enabled})

    @classmethod
    def load(cls, path: Path = ROOT / "crawl.yaml"):
        data = yaml.safe_load(path.read_text(encoding="utf-8"))
        if not isinstance(data, dict) or data.keys() - cls.model_fields.keys():
            raise ValueError("采集 YAML 格式错误或包含未知字段")
        return cls(**data)
