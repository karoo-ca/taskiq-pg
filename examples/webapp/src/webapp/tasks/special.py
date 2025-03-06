import asyncio
import logging

from that_depends import Provide, container_context, inject

from webapp.dependencies import Dependencies
from webapp.settings import Settings
from webapp.worker import broker

logger = logging.getLogger(__name__)

@broker.task()
@container_context()
@inject
async def special_job(settings: Settings = Provide[Dependencies.settings]) -> str:
    """Another worker task."""
    logger.info("starting special_job (using db=%s)", settings.PGDATABASE)
    await asyncio.sleep(1)
    logger.info("completed special_job")
    return "special job."
