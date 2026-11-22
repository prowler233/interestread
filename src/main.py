"""FastAPI 启动入口。初始化数据库，注册路由、健康检查和通用异常处理。"""
import asyncio
import json
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from config.api_settings import APISettings
from config.connect_settings import DatabaseSettings
from src.db.database import initialize_database
from src.router import router

logger = logging.getLogger(__name__)


def create_app(*, session_factory=None, api_settings=None):
    @asynccontextmanager
    async def lifespan(app):
        app.state.api_settings = api_settings or APISettings()
        engine = None
        if session_factory is None:
            url = DatabaseSettings().sqlalchemy_url()
            await asyncio.to_thread(initialize_database, url)
            engine = create_async_engine(url, hide_parameters=True)
            app.state.sessions = async_sessionmaker(engine)
        else:
            app.state.sessions = session_factory
        try:
            yield
        finally:
            if engine is not None:
                await engine.dispose()

    app = FastAPI(title="InteretRead API", version="0.1.0", lifespan=lifespan,
                  description="启动采集→解析→筛选→统计→补字段→入库，并查询任务、资讯及采集历史。")
    app.include_router(router)

    @app.exception_handler(RequestValidationError)
    async def invalid_request(request, exc):
        return JSONResponse(status_code=422, content={"detail": [
            {"loc": list(error["loc"]), "msg": error["msg"], "type": error["type"]}
            for error in exc.errors()]})

    @app.exception_handler(SQLAlchemyError)
    async def unavailable_database(request, exc):
        logger.error(json.dumps({"event": "api_database_error", "error_type": type(exc).__name__}))
        return JSONResponse(status_code=503, content={"detail": "数据库暂不可用，请稍后重试"})

    @app.get("/health/live", tags=["健康检查"])
    async def live():
        return {"status": "ok"}

    @app.get("/health/ready", tags=["健康检查"])
    async def ready(request: Request):
        async with request.app.state.sessions() as session:
            await session.execute(text("SELECT 1"))
        return {"status": "ready"}

    return app

app = create_app()
