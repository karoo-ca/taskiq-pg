import asyncio
import logging

import taskiq_fastapi  # type: ignore[import-untyped]
from taskiq.cli.worker.args import WorkerArgs
from taskiq.cli.worker.run import start_listen
from taskiq.serializers import JSONSerializer
from that_depends import Provide, container_context, inject

from taskiq_pg import AsyncpgBroker, AsyncpgResultBackend
from webapp.dependencies import Dependencies
from webapp.settings import Settings

logger = logging.getLogger(__name__)


@container_context()
def _get_db_dsn() -> str:
    return Dependencies.settings.sync_resolve().db_url


result_backend: AsyncpgResultBackend[object] = AsyncpgResultBackend(
    dsn=_get_db_dsn,
    serializer=JSONSerializer(),
)
broker = AsyncpgBroker(dsn=_get_db_dsn).with_result_backend(result_backend)

taskiq_fastapi.init(broker, "webapp.webapp:make_app")


@broker.task()
@container_context()
@inject
async def so_much_effort(settings: Settings = Provide[Dependencies.settings]) -> str:
    """Worker task."""
    logger.info("starting so_much_effort (using db=%s)", settings.PGDATABASE)
    await asyncio.sleep(1)
    logger.info("completed so_much_effort")
    return "no more work, please."


def entrypoint() -> int | None:
    """Entrypoint to run worker."""
    logging.basicConfig(level=logging.INFO)

    args = WorkerArgs(
        broker="webapp.worker:broker",
        modules=["webapp.tasks"],
        workers=1,
        configure_logging=False,
    )
    # since we want parallelism to be handled by multiple containers, instead of
    # multiprocessing on one container, we use the start_listen method that is
    # used within taskiq's run_worker method:
    return start_listen(args)
