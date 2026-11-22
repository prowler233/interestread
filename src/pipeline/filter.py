"""筛选。按关键词或个人兴趣判断资讯是否保留，返回逐条结果和理由。

AI 模式统一调用 ai_client；正常排除计入本轮汇总，执行失败另行上报。"""
import json
from pathlib import Path
from typing import Literal

import yaml
from pydantic import BaseModel, ConfigDict, Field, model_validator

from config.filter_settings import FilterSettings, ROOT
from src.ai_client import AIError, ai_config_values, create_ai_client
from src.db.failures import ProcessingError
from src.pipeline.parse import NewsItem


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

class InterestPlan(StrictModel):
    terms: list[str] = Field(min_length=1, max_length=8)
    criteria: str = Field(min_length=1)

    @model_validator(mode="after")
    def nonempty(self):
        if not self.criteria.strip() or any(not x.strip() for x in self.terms):
            raise ValueError("搜索主题和依据不能为空")
        return self

class Decision(StrictModel):
    index: int
    keep: bool
    insufficient: bool
    reason: str = Field(min_length=1, max_length=1000)
    tags: list[str] = Field(max_length=20)
    summary: str | None

    @model_validator(mode="after")
    def consistent(self):
        if not self.reason.strip():
            raise ValueError("理由不能为空")
        if self.keep:
            if self.insufficient or not self.tags or not self.summary or not self.summary.strip():
                raise ValueError("保留结果需要摘要和标签，不能标记信息不足")
            if len(self.summary) > 4000 or any(not t.strip() or len(t) > 100 for t in self.tags):
                raise ValueError("摘要或标签长度无效")
        elif self.tags or self.summary is not None:
            raise ValueError("未保留结果不能填写摘要或标签")
        return self

class Decisions(StrictModel):
    decisions: list[Decision]

class FilterOutcome(BaseModel):
    index: int
    status: Literal["kept", "excluded", "insufficient", "failed"]
    reason: str
    tags: list[str] = Field(default_factory=list)
    summary: str | None = None
    identity: str | None = None


def load_filter_settings(path: Path = ROOT / "filter.yaml", *, overrides: dict | None = None) -> FilterSettings:
    # 保持用户 YAML 不变；仅解释它现有的环境变量占位符。
    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(data, dict) or data.keys() - FilterSettings.model_fields.keys():
        raise ValueError("筛选 YAML 格式错误或包含未知字段")
    data = {**data, "ai": ai_config_values(data.get("ai", {}))}
    if not overrides:
        return FilterSettings(**data)
    class RequestSettings(FilterSettings):
        @classmethod
        def settings_customise_sources(cls, settings_cls, init_settings, env_settings,
                                       dotenv_settings, file_secret_settings):
            return lambda: overrides, env_settings, dotenv_settings, init_settings, file_secret_settings
    return RequestSettings(**data)

def keyword_outcome(item: NewsItem, index: int, settings: FilterSettings) -> FilterOutcome:
    text = "\n".join((item.title, item.source_excerpt, item.content)).casefold()
    rules = settings.keyword
    if any(word.casefold() in text for word in rules.exclude):
        return FilterOutcome(index=index, status="excluded", reason="命中排除关键词")
    groups = rules.groups()
    matches = {tag: [all(word.casefold() in text for word in clause) for clause in clauses]
               for tag, clauses in groups.items()}
    branches = [value for values in matches.values() for value in values]
    keep = not branches or (all(branches) if rules.match == "all" else any(branches))
    tags = [tag for tag, values in matches.items() if any(values)] if keep else []
    return FilterOutcome(index=index, status="kept" if keep else "excluded",
                         reason="符合关键词规则" if keep else "未满足包含关键词规则",
                         tags=tags or (["未分类"] if keep else []))

def split_terms(groups: list[list[str]], max_terms: int, max_chars: int) -> list[str]:
    queries = []
    for terms in groups:
        chunk = []
        for term in dict.fromkeys(terms):
            if len(term) > max_chars:
                raise ValueError("单个搜索条件超过 search_max_chars，不能无损拆分")
            candidate = " OR ".join([*chunk, term])
            if chunk and (len(chunk) >= max_terms or len(candidate) > max_chars):
                queries.append(" OR ".join(chunk))
                chunk = []
            chunk.append(term)
        if chunk:
            queries.append(" OR ".join(chunk))
    return list(dict.fromkeys(queries))

async def plan_search(crawl, settings, *, ai=None, on_error=None, interests=None):
    sources = [s for s in crawl.iter_sources() if s.enabled and s.adapter == "platforms"
               and s.needs_query and not (s.query or "").strip()]
    if not sources:
        return {}, None
    plan = None
    try:
        if settings.mode == "keyword":
            groups = settings.keyword.groups()
            terms = [["(" + " ".join(clause) + ")" if len(clause) > 1 else clause[0]
                      for clause in clauses] for clauses in groups.values()]
        else:
            if ai is None:
                ai = create_ai_client(settings.ai)
            plan = await ai.complete({"stage": "plan", "interests": interests if interests is not None else settings.ai.read_interests()}, InterestPlan)
            terms = [[term] for term in plan.terms]
        queries = split_terms(terms, settings.search_max_terms, settings.search_max_chars)
        if not queries:
            raise ValueError("搜索来源没有可用的关注词")
        return {s.id: queries for s in sources}, plan
    except Exception as exc:
        # 只隔离需要自动生成查询词的来源，不影响热榜、Feed 和固定列表。
        for source in sources:
            if on_error:
                await on_error(ProcessingError(
                    stage="search", source_id=source.id, error_type=type(exc).__name__,
                    reason=str(exc) if isinstance(exc, AIError) else "无法生成有效搜索条件",
                    attempts=getattr(exc, "attempts", 1)))
        return {s.id: [] for s in sources}, None

def batches(items, size, max_chars=48000):
    batch = []
    length = 0
    for index, item in enumerate(items):
        row = {"index": index, **item.model_dump(mode="json", include={
            "title", "source_excerpt", "content", "source", "published_at"})}
        # 只截取给模型的上下文，原采集内容不修改。
        for field in ("title", "source_excerpt", "content"):
            row[field] = row[field][:12000]
        row_length = len(json.dumps(row, ensure_ascii=False))
        if batch and (len(batch) >= size or length + row_length > max_chars):
            yield batch
            batch, length = [], 0
        batch.append(row)
        length += row_length
    if batch:
        yield batch

async def filter_items(items, settings, *, ai=None, plan=None, on_error=None, interests=None, existing_tags=None):
    outcomes = []
    if settings.mode == "keyword":
        for index, item in enumerate(items):
            try:
                outcomes.append(keyword_outcome(item, index, settings))
            except Exception as exc:
                outcome = FilterOutcome(index=index, status="failed", reason="关键词筛选未完成")
                outcomes.append(outcome)
                if on_error:
                    await on_error(ProcessingError(stage="filter", source_id=item.source,
                        item_positions=[index], error_type=type(exc).__name__, reason=outcome.reason))
        return outcomes
    if ai is None:
        ai = create_ai_client(settings.ai)
    interests = interests if interests is not None else settings.ai.read_interests()
    tags = existing_tags if existing_tags is not None else set()
    for batch in batches(items, settings.ai.batch_size):
        expected = {row["index"] for row in batch}
        def check(result):
            indices = [d.index for d in result.decisions]
            if len(indices) != len(expected) or set(indices) != expected:
                raise ValueError("结果编号没有与输入逐一对应")
        try:
            result = await ai.complete({
                "stage": "process", "interests": interests,
                "interest_plan": plan.model_dump() if plan else None,
                "existing_tags": sorted(tags)[:100], "items": batch,
            }, Decisions, check=check)
            # 注入的适配器也必须通过相同校验。
            result = Decisions.model_validate(result.model_dump(), strict=True)
            check(result)
            for d in result.decisions:
                outcomes.append(FilterOutcome(index=d.index,
                    status="kept" if d.keep else "insufficient" if d.insufficient else "excluded",
                    reason=d.reason, tags=d.tags, summary=d.summary))
                tags.update(d.tags)
        except Exception as exc:
            reason = str(exc) if isinstance(exc, AIError) else "AI 筛选未完成或结构化结果不可用"
            outcomes.extend(FilterOutcome(index=i, status="failed", reason=reason) for i in sorted(expected))
            if on_error:
                await on_error(ProcessingError(stage="filter", item_positions=sorted(expected),
                    error_type=type(exc).__name__, reason=reason, attempts=getattr(exc, "attempts", 1)))
    return sorted(outcomes, key=lambda x: x.index)
