import os
import random
import string
from collections.abc import AsyncGenerator

import pytest

from taskiq_pg.broker import AsyncpgBroker
from taskiq_pg.result_backend import AsyncpgResultBackend


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
            string.ascii_lowercase,
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
        or "postgresql://postgres:postgres@localhost:5432/taskiqpg"
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
async def asyncpg_broker(
    postgresql_dsn: str,
    postgres_table: str,
) -> AsyncGenerator[AsyncpgBroker, None]:
    """
    Fixture to set up and tear down the broker.

    Initializes the broker with test parameters.
    """
    # Use a random lock keyspace to avoid conflicts in parallel tests
    job_lock_keyspace = random.randint(1000, 9999)

    broker = AsyncpgBroker(
        dsn=postgresql_dsn,
        channel_name=f"{postgres_table}_channel",
        table_name=postgres_table,
        job_lock_keyspace=job_lock_keyspace,
        enable_sweeping=False,  # Disable sweeping in tests to avoid conflicts
    )
    await broker.startup()
    yield broker
    assert broker.write_pool
    await broker.write_pool.execute(
        f"DROP TABLE {postgres_table}",
    )
    await broker.shutdown()
