"""HTTP 接口。通过 schemas 校验请求和响应，处理认证、任务操作和资讯查询。"""
import secrets
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query, Request, Response
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from src.service.extraction_service import InvalidRunSettings, schedule_extraction
from src.service.run_service import (
    NewsNotFound, RunCannotCancel, RunNotFound, RunResultUnavailable, get_completed_result,
    get_news_history, get_run_failures, list_news as list_news_records, list_run_records,
    request_cancellation, require_news_item, require_run, resume_run,
)
from src.schemas import (
    ErrorRead, FilterStatus, NewsRead, ObservationRead, Page, PipelineResult, RunAccepted,
    RunCreate, RunRead, RunStatus,
)


bearer = HTTPBearer(auto_error=False)

async def authenticate(request: Request, credentials: Annotated[HTTPAuthorizationCredentials | None, Depends(bearer)]):
    settings = request.app.state.api_settings
    if settings.auth_enabled and (credentials is None or not secrets.compare_digest(
            credentials.credentials.encode(), settings.key.get_secret_value().encode())):
        raise HTTPException(401, "缺少或无效的 API 密钥", headers={"WWW-Authenticate": "Bearer"})

router = APIRouter(prefix="/api", dependencies=[Depends(authenticate)])
Limit = Annotated[int, Query(ge=1, le=100)]
Offset = Annotated[int, Query(ge=0)]

async def get_run(request, run_id):
    try:
        row = await require_run(request.app.state.sessions, run_id)
    except RunNotFound:
        raise HTTPException(404, "任务不存在") from None
    return {**row, "run_id": row["id"]}

def accepted(run_id, status):
    return RunAccepted(run_id=run_id, status=status, status_url=f"/api/runs/{run_id}",
                       result_url=f"/api/runs/{run_id}/result")

@router.post("/runs", response_model=RunAccepted, status_code=202, tags=["任务"])
async def create_run(request: Request, response: Response, body: RunCreate = RunCreate()):
    try:
        run_id, status = await schedule_extraction(request.app.state.sessions, body)
    except InvalidRunSettings:
        raise HTTPException(422, "请求参数或服务器业务配置无效，请检查来源、关键词和 AI 配置") from None
    except RunNotFound:
        raise HTTPException(404, "任务不存在") from None
    response.headers["Location"] = f"/api/runs/{run_id}"
    return accepted(run_id, status)

@router.get("/runs", response_model=Page[RunRead], tags=["任务"])
async def list_runs(request: Request, status: RunStatus | None = None, limit: Limit = 20, offset: Offset = 0):
    page = await list_run_records(request.app.state.sessions, status=status, limit=limit, offset=offset)
    for row in page["items"]:
        row["run_id"] = row["id"]
    return page

@router.get("/runs/{run_id}", response_model=RunRead, tags=["任务"])
async def run_status(request: Request, run_id: UUID):
    return await get_run(request, run_id)

@router.get("/runs/{run_id}/result", response_model=PipelineResult, tags=["任务"])
async def run_result(request: Request, run_id: UUID):
    try:
        return await get_completed_result(request.app.state.sessions, run_id)
    except RunNotFound:
        raise HTTPException(404, "任务不存在") from None
    except RunResultUnavailable as error:
        raise HTTPException(409, str(error), headers={"Retry-After": "2"}) from None

@router.get("/runs/{run_id}/errors", response_model=Page[ErrorRead], tags=["任务"])
async def run_errors(request: Request, run_id: UUID, limit: Limit = 20, offset: Offset = 0):
    try:
        return await get_run_failures(request.app.state.sessions, run_id, limit=limit, offset=offset)
    except RunNotFound:
        raise HTTPException(404, "任务不存在") from None

@router.post("/runs/{run_id}/cancel", response_model=RunRead, tags=["任务"])
async def cancel(request: Request, run_id: UUID):
    try:
        row = await request_cancellation(request.app.state.sessions, run_id)
    except RunNotFound:
        raise HTTPException(404, "任务不存在") from None
    except RunCannotCancel:
        raise HTTPException(409, "任务已经结束，不能取消") from None
    return {**row, "run_id": row["id"]}

@router.post("/runs/{run_id}/resume", response_model=RunAccepted, status_code=202, tags=["任务"])
async def resume(request: Request, run_id: UUID):
    state = await resume_run(request.app.state.sessions, run_id)
    if state == "missing":
        raise HTTPException(404, "任务不存在")
    if state == "active":
        raise HTTPException(409, "任务仍持有有效执行权，无需恢复")
    if state == "terminal":
        raise HTTPException(409, "任务已提交结果或已取消；重新处理请创建新任务")
    return accepted(run_id, "queued")

@router.get("/news", response_model=Page[NewsRead], tags=["资讯"])
async def list_news(request: Request, status: FilterStatus | None = "kept", source_id: str | None = None,
                    q: Annotated[str | None, Query(min_length=1, max_length=200)] = None,
                    limit: Limit = 20, offset: Offset = 0):
    return await list_news_records(request.app.state.sessions, status=status, source_id=source_id,
                                   q=q, limit=limit, offset=offset)

@router.get("/news/{news_item_id}", response_model=NewsRead, tags=["资讯"])
async def news_detail(request: Request, news_item_id: UUID):
    try:
        return await require_news_item(request.app.state.sessions, news_item_id)
    except NewsNotFound:
        raise HTTPException(404, "资讯不存在") from None

@router.get("/news/{news_item_id}/observations", response_model=Page[ObservationRead], tags=["资讯"])
async def history(request: Request, news_item_id: UUID, source_id: str | None = None,
                  limit: Limit = 20, offset: Offset = 0):
    try:
        return await get_news_history(request.app.state.sessions, news_item_id, source_id=source_id,
                                      limit=limit, offset=offset)
    except NewsNotFound:
        raise HTTPException(404, "资讯不存在") from None
