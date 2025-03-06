import logging
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI
from that_depends.providers import DIContextMiddleware

from webapp.dependencies import Dependencies
from webapp.tasks.special import special_job
from webapp.worker import broker, so_much_effort

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(_app: FastAPI) -> AsyncGenerator[None]:
    """Lifespan (startup/shutdown) for webapp."""
    try:
        await Dependencies.init_resources()
        if not broker.is_worker_process:
            await broker.startup()
        yield
    finally:
        if not broker.is_worker_process:
            await broker.shutdown()
        await Dependencies.tear_down()


def make_app() -> FastAPI:
    """Factory for webapp."""
    app = FastAPI(
        lifespan=lifespan,
    )

    app.add_middleware(DIContextMiddleware)

    @app.get("/")
    async def root() -> str:  # pyright: ignore[reportUnusedFunction]
        return "OK"

    @app.post("/do-work")
    async def do_work() -> str:  # pyright: ignore[reportUnusedFunction]
        task = await so_much_effort.kiq()
        result = await task.wait_result(timeout=2, with_logs=True)
        logger.info("result=%s", result)
        return result.return_value

    @app.post("/do-special-work")
    async def do_special_work() -> str:  # pyright: ignore[reportUnusedFunction]
        task = await special_job.kiq()
        result = await task.wait_result(timeout=2, with_logs=True)
        logger.info("result=%s", result)
        return result.return_value

    return app


def entrypoint() -> None:
    """Entrypoint to run webapp."""
    uvicorn.run(make_app, factory=True)
