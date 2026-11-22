"""筛选业务配置在 YAML，模型凭据在 .env。"""
import re
from pathlib import Path
from typing import Literal

import yaml
from pydantic import BaseModel, ConfigDict, Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from src.ai_client import AISettings, ai_config_values

ROOT = Path(__file__).resolve().parent


class KeywordSettings(BaseModel):
    model_config = ConfigDict(extra="forbid")
    include: list[str] = Field(default_factory=list)
    exclude: list[str] = Field(default_factory=list)
    match: Literal["any", "all"] = "any"

    def groups(self):
        groups = {}
        for rule in self.include:
            words, separator, category = rule.partition("->")
            category = category.strip() if separator else "未分类"
            clauses = [tuple(word.strip() for word in re.split(r"[&＆]", branch))
                       for branch in re.split(r"[,，]", words)]
            if not category or "->" in category or any(not word for clause in clauses for word in clause):
                raise ValueError("关键词格式应为 关键词1&关键词2, 关键词3 -> 类别，词与类别不能为空")
            groups.setdefault(category, []).extend(clauses)
        return {category: list(dict.fromkeys(clauses)) for category, clauses in groups.items()}


class FilterSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=ROOT.parent / ".env", env_file_encoding="utf-8",
        extra="ignore", hide_input_in_errors=True, env_prefix="FILTER_",
        env_nested_delimiter="__",
    )
    mode: Literal["keyword", "ai"] = "keyword"
    keyword: KeywordSettings = Field(default_factory=KeywordSettings)
    ai: AISettings = Field(default_factory=AISettings)
    search_max_terms: int = Field(default=4, ge=1, le=20)
    search_max_chars: int = Field(default=200, ge=32, le=512)

    @classmethod
    def settings_customise_sources(cls, settings_cls, init_settings, env_settings,
                                   dotenv_settings, file_secret_settings):
        return env_settings, dotenv_settings, init_settings, file_secret_settings

    @model_validator(mode="after")
    def validate_mode(self):
        if self.mode == "keyword" and any(
            not word.strip() for word in self.keyword.include + self.keyword.exclude
        ):
            raise ValueError("关键词不能是空字符串")
        if self.mode == "keyword":
            self.keyword.groups()
        if self.mode == "ai":
            self.ai.read_interests()
        if self.mode == "ai" and self.ai.provider != "codex" and self.ai.url is None:
            raise ValueError("API 筛选必须填写 ai.url 完整请求地址")
        if self.mode == "ai" and self.ai.provider != "codex" and not self.ai.models:
            raise ValueError("API 筛选必须填写 ai.models 模型列表")
        if self.mode == "ai" and self.ai.provider != "codex" and not self.ai.api_key.get_secret_value():
            raise ValueError("API 筛选必须填写 ai.api_key 或 FILTER_AI__API_KEY")
        return self

    @classmethod
    def load(cls, path: Path = ROOT / "filter.yaml"):
        data = yaml.safe_load(path.read_text(encoding="utf-8"))
        if not isinstance(data, dict) or data.keys() - cls.model_fields.keys():
            raise ValueError("筛选 YAML 格式错误或包含未知字段")
        return cls(**{**data, "ai": ai_config_values(data.get("ai", {}))})
