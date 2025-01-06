import os
import random
import string
from typing import AsyncGenerator

import asyncpg
import pytest

from taskiq_asyncpg.broker import AsyncpgBroker
from taskiq_asyncpg.result_backend import AsyncpgResultBackend


@pytest.fixture(scope="session")
def anyio_backend() -> str:
    """
    Anyio backend.

    Backend for anyio pytest plugin.
    :return: backend name.
    """
    return "asyncio"


@pytest.fixture
def postgres_table() -> str:
    """
    Name of a postgresql table for current test.

    :return: random string.
    """
    return "".join(
        random.choice(
            string.ascii_uppercase,
        )
        for _ in range(10)
    )


@pytest.fixture
def postgresql_dsn() -> str:
    """
    DSN to PostgreSQL.

    :return: dsn to PostgreSQL.
    """
    return (
        os.environ.get("POSTGRESQL_URL")
        or "postgresql://postgres:postgres@localhost:5432/taskiqasyncpg"
    )


@pytest.fixture()
async def asyncpg_result_backend(
    postgresql_dsn: str,
    postgres_table: str,
) -> AsyncGenerator[AsyncpgResultBackend[object], None]:
    backend: AsyncpgResultBackend[object] = AsyncpgResultBackend(
        dsn=postgresql_dsn,
        table_name=postgres_table,
    )
    await backend.startup()
    yield backend
    await backend._database_pool.execute(
        f"DROP TABLE {postgres_table}",
    )
    await backend.shutdown()


@pytest.fixture()
async def asyncpg_broker(postgresql_dsn: str) -> AsyncGenerator[AsyncpgBroker, None]:
    """
    Fixture to set up and tear down the broker.

    Initializes the broker with test parameters.
    """
    broker = AsyncpgBroker(
        dsn=postgresql_dsn,
        channel_name="taskiq_test_channel",
        table_name="taskiq_test_messages",
    )
    await broker.startup()
    yield broker
    await broker.shutdown()


@pytest.fixture(autouse=True)
async def clean_messages_table(asyncpg_broker: AsyncpgBroker) -> None:
    """Fixture to clean up the messages table before each test."""
    conn = await asyncpg.connect(dsn=asyncpg_broker.dsn)

    await conn.execute("DELETE FROM {}".format(asyncpg_broker.table_name))  # noqa: S608
    await conn.close()
