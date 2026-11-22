"""AI 调用入口。读取模型和连接配置，发起请求，控制并发与重试，校验结构化响应。"""
import asyncio
import contextlib
import copy
import json
import tempfile
from pathlib import Path
from typing import Literal, Protocol
from urllib.parse import urlsplit
import httpx
import yaml
from pydantic import BaseModel, ConfigDict, Field, HttpUrl, SecretStr, ValidationError, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


CONFIG_DIR = Path(__file__).resolve().parents[1] / "config"

class AISettings(BaseModel):
    model_config = ConfigDict(extra="forbid")
    provider: Literal["deepseek", "zhipu", "codex"] = "deepseek"
    models: list[str] = Field(default_factory=list)
    url: HttpUrl | None = None
    api_key: SecretStr = SecretStr("")
    interests_file: Path = CONFIG_DIR / "ai_interests.txt"
    prompt_file: Path = CONFIG_DIR / "ai_processing_prompt.txt"
    concurrency: int = Field(default=2, ge=1, le=8)
    batch_size: int = Field(default=12, ge=1, le=200)
    timeout: float = Field(default=120, gt=0, le=600)
    max_tokens: int = Field(default=4096, ge=256, le=16384)
    format_retries: int = Field(default=1, ge=0, le=3)
    codex_command: str = "codex"

    def read_interests(self):
        path = self.interests_file if self.interests_file.is_absolute() else CONFIG_DIR / self.interests_file
        interests = path.read_text(encoding="utf-8").strip()
        if not interests:
            raise ValueError("AI 兴趣文件不能为空，请填写 interests_file")
        return interests

    @field_validator("models")
    @classmethod
    def clean_models(cls, values):
        models = [value.strip() for value in values]
        if any(not model for model in models):
            raise ValueError("模型名称不能为空")
        return list(dict.fromkeys(models))

    def read_prompt(self):
        path = self.prompt_file if self.prompt_file.is_absolute() else CONFIG_DIR / self.prompt_file
        prompt = path.read_text(encoding="utf-8").strip()
        if not prompt:
            raise ValueError("AI 筛选提示词文件不能为空")
        return prompt

    @field_validator("url", mode="before")
    @classmethod
    def empty_url(cls, value):
        # YAML 中的环境变量名称只是占位符；未选 AI 模式时不要求凭据。
        return None if value == "" else value


class AIConnectionSettings(BaseSettings):
    """恢复时只重新读取连接地址与凭据，不重新读取兴趣及提示词内容。"""
    model_config = SettingsConfigDict(
        env_file=CONFIG_DIR.parent / ".env", env_prefix="FILTER_AI__", extra="ignore", hide_input_in_errors=True)
    url: HttpUrl | None = None
    api_key: SecretStr = SecretStr("")

    @classmethod
    def settings_customise_sources(cls, settings_cls, init_settings, env_settings,
                                   dotenv_settings, file_secret_settings):
        return env_settings, dotenv_settings, init_settings, file_secret_settings

    @field_validator("url", mode="before")
    @classmethod
    def empty_url(cls, value):
        return None if value == "" else value


class StructuredClient(Protocol):
    async def complete(self, payload: dict, output: type[BaseModel], *, check=None) -> BaseModel: ...

class AIError(ValueError):
    def __init__(self, reason: str, attempts: int = 1):
        super().__init__(reason)
        self.attempts = attempts

def strict_schema(output, *, deepseek=False):
    schema = copy.deepcopy(output.model_json_schema())
    definitions = schema.pop("$defs", {})
    def visit(node):
        if isinstance(node, list):
            return [visit(x) for x in node]
        if not isinstance(node, dict):
            return node
        if "$ref" in node:
            return visit(copy.deepcopy(definitions[node["$ref"].rsplit("/", 1)[1]]))
        result = {k: visit(v) for k, v in node.items() if k not in ("title", "default")}
        if result.get("type") == "object":
            result["additionalProperties"] = False
            result["required"] = list(result.get("properties", {}))
        if deepseek:
            for key in ("minLength", "maxLength", "minItems", "maxItems"):
                result.pop(key, None)
        return result
    return visit(schema)

class AIClient:
    def __init__(self, settings: AISettings, *, transport=None, prompt=None):
        self.settings = settings
        self.transport = transport
        self.prompt = prompt if prompt is not None else settings.read_prompt()

    async def complete(self, payload, output, *, check=None):
        attempts = 0
        last_reason = "未配置可用模型"
        models = self.settings.models or ([None] if self.settings.provider == "codex" else [])
        for model in models:
            for repair in range(self.settings.format_retries + 1):
                request = payload if not repair else {
                    "original_request": payload,
                    "validation_errors": ["结果须符合 schema，并完整对应原始输入"],
                }
                format_error = False
                for network_attempt in range(3):
                    attempts += 1
                    try:
                        raw = await self._call(request, output, model)
                        result = output.model_validate_json(raw, strict=True)
                        if check is not None:
                            check(result)
                        return result
                    except AIError as exc:
                        last_reason = str(exc)
                        break
                    except (ValidationError, ValueError, KeyError, IndexError, TypeError, AttributeError):
                        last_reason = "结构化结果未通过完整性或业务校验"
                        format_error = True
                        break
                    except (httpx.HTTPError, OSError, TimeoutError) as exc:
                        if isinstance(exc, httpx.HTTPStatusError):
                            last_reason = f"模型接口返回 HTTP {exc.response.status_code}"
                        else:
                            last_reason = f"模型调用失败（{type(exc).__name__}）"
                        retryable = isinstance(exc, (httpx.TransportError, TimeoutError)) or (
                            isinstance(exc, httpx.HTTPStatusError)
                            and (exc.response.status_code == 429 or exc.response.status_code >= 500)
                        )
                        if retryable and network_attempt < 2:
                            await asyncio.sleep(2 ** network_attempt)
                            continue
                        break
                if not format_error:
                    break
        raise AIError(last_reason, attempts)

    async def _call(self, payload, output, model):
        if self.settings.provider == "codex":
            return await self._codex(payload, output, model)
        if self.settings.url is None:
            raise AIError("未配置模型请求地址")
        deepseek = self.settings.provider == "deepseek"
        if deepseek and "/beta/" not in urlsplit(str(self.settings.url)).path:
            raise AIError("DeepSeek 严格输出需要 beta 请求地址，不会降级为普通 JSON")
        body = {
            "model": model,
            "messages": [
                {"role": "system", "content": self.prompt},
                {"role": "user", "content": json.dumps(payload, ensure_ascii=False)},
            ],
            "max_tokens": self.settings.max_tokens,
        }
        schema = strict_schema(output, deepseek=deepseek)
        if deepseek:
            body.update({
                "thinking": {"type": "disabled"},  # DeepSeek requires this for a named tool choice.
                "tools": [{"type": "function", "function": {
                    "name": "submit_result", "description": "提交结构化结果",
                    "strict": True, "parameters": schema,
                }}],
                "tool_choice": {"type": "function", "function": {"name": "submit_result"}},
                "parallel_tool_calls": False,
            })
        else:
            body["response_format"] = {"type": "json_schema", "json_schema": {
                "name": "screening_result", "strict": True, "schema": schema,
            }}
        async with httpx.AsyncClient(timeout=self.settings.timeout, transport=self.transport,
                                     follow_redirects=False) as client:
            response = await client.post(str(self.settings.url), json=body, headers={
                "Authorization": f"Bearer {self.settings.api_key.get_secret_value()}",
            })
            response.raise_for_status()
            choice = response.json()["choices"][0]
            if choice.get("finish_reason") not in ("stop", "tool_calls"):
                raise ValueError("输出被截断或未正常完成")
            message = choice["message"]
            if deepseek:
                calls = message["tool_calls"]
                if len(calls) != 1 or calls[0]["function"]["name"] != "submit_result":
                    raise ValueError("未提交唯一的结构化结果")
                return calls[0]["function"]["arguments"]
            if message.get("refusal"):
                raise ValueError("模型拒绝处理")
            return message["content"]

    async def _codex(self, payload, output, model):
        with tempfile.TemporaryDirectory(prefix="interetread-ai-") as directory:
            schema_path = Path(directory) / "schema.json"
            output_path = Path(directory) / "result.json"
            schema_path.write_text(json.dumps(strict_schema(output)), encoding="utf-8")
            args = [self.settings.codex_command, "exec", "--skip-git-repo-check", "--ephemeral",
                    "--sandbox", "read-only", "--output-schema", str(schema_path),
                    "--output-last-message", str(output_path)]
            if model:
                args.extend(("--model", model))
            args.append("-")
            process = await asyncio.create_subprocess_exec(
                *args, cwd=directory, stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL)
            try:
                async with asyncio.timeout(self.settings.timeout):
                    await process.communicate((self.prompt + "\n" + json.dumps(payload, ensure_ascii=False)).encode())
                    if process.returncode != 0 or not output_path.is_file():
                        raise AIError("Codex 结构化筛选执行失败")
                    if output_path.stat().st_size > 1_000_000:
                        raise AIError("Codex 结果超过大小限制")
                    return output_path.read_text(encoding="utf-8")
            finally:
                if process.returncode is None:
                    with contextlib.suppress(ProcessLookupError):
                        process.kill()
                    await process.communicate()


class LimitedAIClient:
    """搜索规划和筛选共用模型请求额度，避免并发规划后超出配置上限。"""
    def __init__(self, client, concurrency):
        self.client = client
        self.slots = asyncio.Semaphore(concurrency)

    async def complete(self, payload, output, *, check=None):
        async with self.slots:
            return await self.client.complete(payload, output, check=check)



def ai_config_values(values):
    """Interpret the existing YAML placeholders before environment overrides."""
    ai = dict(values)
    if ai.get("url") == "FILTER_AI__URL":
        ai["url"] = None
    if ai.get("api_key") == "FILTER_AI__API_KEY":
        ai["api_key"] = ""
    return ai


def load_ai_connection():
    data = yaml.safe_load((CONFIG_DIR / "filter.yaml").read_text(encoding="utf-8"))
    ai = ai_config_values(data.get("ai", {}))
    return AIConnectionSettings(url=ai.get("url"), api_key=ai.get("api_key", ""))


def create_ai_client(settings, *, client=None, transport=None, prompt=None, concurrency=None):
    if client is None:
        client = AIClient(settings, transport=transport, prompt=prompt)
    return LimitedAIClient(client, concurrency) if concurrency is not None else client
