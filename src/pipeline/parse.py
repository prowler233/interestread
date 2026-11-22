"""解析。把 NewsNow、Feed 和 OpenCLI 响应转换成统一的 NewsItem。

校验字段，并记录无效条目的位置和原因，供采集阶段上报。"""
import calendar
import json
import logging
from collections import Counter
from dataclasses import dataclass, field
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from html.parser import HTMLParser
from typing import Literal
from urllib.parse import quote, urljoin

import feedparser
from pydantic import AwareDatetime, BaseModel, Field, HttpUrl, ValidationError

from config.crawl_settings import NewsNowSource, PlatformSource, RAJSource, Source


class SourceError(ValueError):
    """适合直接显示的来源错误原因。"""


logger = logging.getLogger(__name__)
Channel = Literal["newsnow", "feed", "opencli"]


@dataclass
class ParseStats:
    """原始条数包含超出采集上限的条目，跳过数只统计已检查的无效条目。"""

    raw_count: int | None = None
    examined_count: int = 0
    issues: list[tuple[int, tuple[str, ...]]] = field(default_factory=list)
    skipped_count: int = 0
    skip_reasons: Counter[str] = field(default_factory=Counter)

    def reject(self, *reasons: str, position: int = 0) -> None:
        self.issues.append((position, reasons))
        self.skipped_count += 1
        self.skip_reasons.update(reasons)


class NewsItem(BaseModel):
    channel: Channel
    source: str
    title: str = Field(min_length=1)
    url: HttpUrl
    source_item_id: str | None = None
    source_excerpt: str = ""
    content: str = ""
    author: str | None = None
    published_at: datetime | None = None
    rank: int | None = None
    source_position: int | None = None
    source_updated_at: datetime | None = None
    version_summary: str = ""
    observed_at: AwareDatetime | None = Field(default_factory=lambda: datetime.now(UTC))


def _text(value):
    return value.replace("\x00", "").strip() if isinstance(value, str) else ""


def _source_item_id(value) -> str | None:
    if isinstance(value, int) and not isinstance(value, bool):
        return str(value)
    return _text(value) or None


class _HTMLText(HTMLParser):
    def __init__(self):
        super().__init__()
        self.parts = []
        self.hidden = False

    def handle_starttag(self, tag, attrs):
        if tag in ("script", "style"):
            self.hidden = True

    def handle_endtag(self, tag):
        if tag in ("script", "style"):
            self.hidden = False

    def handle_data(self, data):
        if not self.hidden:
            self.parts.append(data)


def _html_text(value):
    parser = _HTMLText()
    parser.feed(_text(value))
    parser.close()
    return " ".join(" ".join(parser.parts).split())


def _feed_text(part):
    value = part.get("value")
    if part.get("type") in ("text/html", "application/xhtml+xml"):
        return _html_text(value)
    return _text(value)


def _published_at(value):
    try:
        if isinstance(value, (tuple, list)):
            return datetime.fromtimestamp(calendar.timegm(value), UTC)
        if isinstance(value, str) and value.strip().isdigit():
            value = int(value)
        if isinstance(value, (int, float)) and not isinstance(value, bool):
            seconds = value / 1000 if abs(value) >= 10**11 else value
            return datetime.fromtimestamp(seconds, UTC)
        if not _text(value):
            return None
        try:
            date = datetime.fromisoformat(value.strip())
        except ValueError:
            date = parsedate_to_datetime(value)
        return date.astimezone(UTC) if date.tzinfo else None
    except (OSError, TypeError, ValueError, OverflowError):
        return None


def _normalize(
    source: Source, channel: Channel, entries, read_fields, *,
    base_url: str | None = None, stats: ParseStats | None = None,
) -> list[NewsItem]:
    stats = stats if stats is not None else ParseStats()
    stats.raw_count = len(entries)
    observed_at = datetime.now(UTC)
    items = []
    for position, entry in enumerate(entries, 1):
        stats.examined_count += 1
        if not isinstance(entry, dict):
            stats.reject("invalid_entry", position=position)
            logger.warning("来源 %s 第 %d 条已跳过：条目应为对象", source.id, position)
            continue
        try:
            row = read_fields(entry, position)
        except (ValueError, TypeError, AttributeError, AssertionError) as exc:
            stats.reject("invalid_fields", position=position)
            logger.warning("来源 %s 第 %d 条无法读取（%s）", source.id, position, type(exc).__name__)
            continue

        url = row.get("url")
        if base_url and _text(url):
            try:
                url = urljoin(base_url, url.strip())
            except ValueError:
                url = None
        raw_rank = row.get("rank")
        try:
            rank = int(str(raw_rank))
        except ValueError:
            rank = None
        if rank is not None and not 1 <= rank <= 2_147_483_647:
            rank = None

        raw_date = row.get("published_at")
        published_at = _published_at(raw_date)
        try:
            item = NewsItem(
                channel=channel, source=source.id, title=_text(row.get("title")),
                source_item_id=_source_item_id(row.get("id")),
                url=url, source_excerpt=_text(row.get("source_excerpt")),
                content=_text(row.get("content")), author=_text(row.get("author")) or None,
                published_at=published_at, rank=rank, source_position=position,
                source_updated_at=_published_at(row.get("source_updated_at")),
                version_summary=_text(row.get("version_summary")),
                observed_at=observed_at,
            )
        except ValidationError as exc:
            fields = [str(error["loc"][0]) for error in exc.errors(include_input=False)]
            stats.reject(*(f"invalid_{field}" for field in fields), position=position)
            logger.warning("来源 %s 第 %d 条已跳过：缺少或无效字段 %s", source.id, position, "、".join(fields))
            continue

        invalid = [field for field in ("source_excerpt", "content", "author")
                   if row.get(field) is not None and not isinstance(row[field], str)]
        if raw_rank not in (None, "") and rank is None:
            invalid.append("rank")
        if raw_date not in (None, "") and published_at is None:
            invalid.append("published_at")
        if invalid:
            logger.warning("来源 %s 第 %d 条可选字段无效，已留空：%s", source.id, position, "、".join(invalid))
        items.append(item)
        if len(items) >= source.limit:
            break
    return items


def parse_newsnow(source: Source, payload, *, stats: ParseStats | None = None) -> list[NewsItem]:
    if (not isinstance(payload, dict) or payload.get("status") not in ("success", "cache")
            or not isinstance(payload.get("items"), list)):
        raise SourceError("NewsNow 缺少有效的状态或条目列表")

    def fields(entry, rank):
        extra = entry.get("extra")
        return {
            "id": entry.get("id"), "title": entry.get("title"), "url": entry.get("url"), "rank": rank,
            "source_excerpt": extra.get("hover") if isinstance(extra, dict) else None,
            "published_at": entry.get("pubDate"),
        }

    return _normalize(source, "newsnow", payload["items"], fields, stats=stats)


def parse_feed(source: Source, payload: bytes, base_url: str, *, stats: ParseStats | None = None) -> list[NewsItem]:
    if payload.decode("utf-8-sig", errors="replace").lstrip().startswith("{"):
        feed = json.loads(payload)
        if (not isinstance(feed, dict)
                or feed.get("version") not in ("https://jsonfeed.org/version/1", "https://jsonfeed.org/version/1.1")
                or not isinstance(feed.get("items"), list)):
            raise SourceError("JSON Feed 缺少有效的版本或条目列表")
        return _normalize(source, "feed", feed["items"], _json_feed_fields, base_url=base_url, stats=stats)

    feed = feedparser.parse(payload)
    if not feed.get("version") or (feed.get("bozo") and not feed.entries):
        raise SourceError("无法解析 RSS 或 Atom 订阅")
    if feed.get("bozo"):
        logger.warning("来源 %s 的订阅格式异常，继续处理可读条目", source.id)
    return _normalize(source, "feed", feed.entries, _xml_feed_fields, base_url=base_url, stats=stats)


def _xml_feed_fields(entry, _position):
    return {
        "id": entry.get("id"),
        "title": _feed_text(entry.get("title_detail", {"value": entry.get("title")})),
        "url": entry.get("link"),
        "source_excerpt": _feed_text(entry.get("summary_detail", {})),
        "content": "\n".join(_feed_text(part) for part in entry.get("content", [])),
        "author": entry.get("author"),
        "published_at": entry.get("published_parsed") or entry.get("published"),
    }


def _json_feed_fields(entry, _position):
    authors = entry.get("authors")
    if not isinstance(authors, list):
        authors = [entry.get("author")]
    names = [_text(author.get("name")) for author in authors if isinstance(author, dict)]
    return {
        "id": entry.get("id"),
        "title": entry.get("title"),
        "url": entry.get("url"),
        "source_excerpt": entry.get("summary"),
        "content": _text(entry.get("content_text")) or _html_text(entry.get("content_html")),
        "author": ", ".join(filter(None, names)),
        "published_at": entry.get("date_published"),
    }


def parse_opencli(source: PlatformSource, payload, *, stats: ParseStats | None = None) -> list[NewsItem]:
    if (not isinstance(payload, dict) or payload.get("ok") is not True
            or not isinstance(payload.get("data"), list)):
        raise SourceError("OpenCLI 未返回成功状态和条目列表")

    def fields(entry, _position):
        row = {field: entry.get(key) for field, key in source.fields.items()}
        if not row.get("url") and source.url_template and row.get("id") is not None:
            row["url"] = source.url_template.replace("{id}", quote(str(row["id"]), safe=""))
        if source.title_limit is not None and isinstance(row.get("title"), str):
            row["title"] = row["title"][:source.title_limit]
        return row

    return _normalize(source, "opencli", payload["data"], fields, stats=stats)


class NoValidItemsError(SourceError):
    """来源返回了条目，但没有条目通过校验。"""


def parse_source(source: Source, body: bytes, base_url: str | None, *, stats: ParseStats) -> list[NewsItem]:
    """Parse the fetched data in the format selected by the source."""
    if isinstance(source, NewsNowSource):
        items = parse_newsnow(source, json.loads(body), stats=stats)
    elif isinstance(source, RAJSource):
        items = parse_feed(source, body, base_url or str(source.url), stats=stats)
    else:
        items = parse_opencli(source, json.loads(body), stats=stats)
    if stats.raw_count and not items:
        raise NoValidItemsError(f"来源返回的 {stats.raw_count} 条资讯均未通过校验")
    return items
