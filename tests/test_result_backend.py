import uuid
from typing import Any, TypeVar

import pytest
from pydantic import BaseModel
from taskiq import TaskiqResult

from taskiq_pg.exceptions import ResultIsMissingError
from taskiq_pg.result_backend import AsyncpgResultBackend

_ReturnType = TypeVar("_ReturnType")
pytestmark = pytest.mark.anyio


class ResultForTest(BaseModel):
    """Just test class for testing."""

    test_arg: str = uuid.uuid4().hex


@pytest.fixture
def task_id() -> str:
    """
    Generates ID for taskiq result.

    :returns: uuid as string.
    """
    return str(uuid.uuid4())


@pytest.fixture
def default_taskiq_result() -> TaskiqResult[Any]:
    """
    Generates default TaskiqResult.

    :returns: TaskiqResult with generic result.
    """
    return TaskiqResult(
        is_err=False,
        log=None,
        return_value="Best test ever.",
        execution_time=0.1,
    )


@pytest.fixture
def custom_taskiq_result() -> TaskiqResult[Any]:
    """
    Generates custom TaskiqResult.

    :returns: TaskiqResult with custom class result.
    """
    return TaskiqResult(
        is_err=False,
        log=None,
        return_value=ResultForTest(),
        execution_time=0.1,
    )


async def test_failure_backend_result(
    asyncpg_result_backend: AsyncpgResultBackend[_ReturnType],
    task_id: str,
) -> None:
    """Test exception raising in `get_result` method."""
    with pytest.raises(expected_exception=ResultIsMissingError):
        await asyncpg_result_backend.get_result(task_id=task_id)


async def test_success_backend_default_result_delete_res(
    postgresql_dsn: str,
    postgres_table: str,
    default_taskiq_result: TaskiqResult[_ReturnType],
    task_id: str,
) -> None:
    backend: AsyncpgResultBackend[_ReturnType] = AsyncpgResultBackend(
        dsn=postgresql_dsn,
        table_name=postgres_table,
        keep_results=False,
    )
    await backend.startup()

    await backend.set_result(
        task_id=task_id,
        result=default_taskiq_result,
    )
    await backend.get_result(task_id=task_id)

    with pytest.raises(expected_exception=ResultIsMissingError):
        await backend.get_result(task_id=task_id)

    await backend._database_pool.execute(
        f"DROP TABLE {postgres_table}",
    )

    await backend.shutdown()


async def test_success_backend_default_result(
    asyncpg_result_backend: AsyncpgResultBackend[_ReturnType],
    default_taskiq_result: TaskiqResult[_ReturnType],
    task_id: str,
) -> None:
    """
    Tests normal behavior with default result in TaskiqResult.

    :param default_taskiq_result: TaskiqResult with default result.
    :param task_id: ID for task.
    :param nats_urls: urls to NATS.
    """
    await asyncpg_result_backend.set_result(
        task_id=task_id,
        result=default_taskiq_result,
    )
    result = await asyncpg_result_backend.get_result(task_id=task_id)

    assert result == default_taskiq_result


async def test_success_backend_custom_result(
    asyncpg_result_backend: AsyncpgResultBackend[_ReturnType],
    custom_taskiq_result: TaskiqResult[_ReturnType],
    task_id: str,
) -> None:
    """
    Tests normal behavior with custom result in TaskiqResult.

    :param custom_taskiq_result: TaskiqResult with custom result.
    :param task_id: ID for task.
    :param redis_url: url to redis.
    """
    await asyncpg_result_backend.set_result(
        task_id=task_id,
        result=custom_taskiq_result,
    )
    result = await asyncpg_result_backend.get_result(task_id=task_id)

    assert (
        result.return_value["test_arg"]  # type: ignore
        == custom_taskiq_result.return_value.test_arg  # type: ignore
    )
    assert result.is_err == custom_taskiq_result.is_err
    assert result.execution_time == custom_taskiq_result.execution_time
    assert result.log == custom_taskiq_result.log


async def test_success_backend_is_result_ready(
    asyncpg_result_backend: AsyncpgResultBackend[_ReturnType],
    custom_taskiq_result: TaskiqResult[_ReturnType],
    task_id: str,
) -> None:
    """Tests `is_result_ready` method."""
    assert not await asyncpg_result_backend.is_result_ready(task_id=task_id)
    await asyncpg_result_backend.set_result(
        task_id=task_id,
        result=custom_taskiq_result,
    )

    assert await asyncpg_result_backend.is_result_ready(task_id=task_id)


async def test_test_success_backend_custom_result_set_same_task_id(
    asyncpg_result_backend: AsyncpgResultBackend[_ReturnType],
    custom_taskiq_result: TaskiqResult[_ReturnType],
    task_id: str,
) -> None:
    await asyncpg_result_backend.set_result(
        task_id=task_id,
        result=custom_taskiq_result,
    )
    result = await asyncpg_result_backend.get_result(task_id=task_id)

    assert (
        result.return_value["test_arg"]  # type: ignore
        == custom_taskiq_result.return_value.test_arg  # type: ignore
    )

    await asyncpg_result_backend.set_result(
        task_id=task_id,
        result=custom_taskiq_result,
    )

    another_taskiq_res_uuid = uuid.uuid4().hex
    another_taskiq_res = TaskiqResult(
        is_err=False,
        log=None,
        return_value=ResultForTest(test_arg=another_taskiq_res_uuid),
        execution_time=0.1,
    )

    await asyncpg_result_backend.set_result(
        task_id=task_id,
        result=another_taskiq_res,  # type: ignore[arg-type]
    )
    result = await asyncpg_result_backend.get_result(task_id=task_id)

    assert result.return_value["test_arg"] == another_taskiq_res_uuid  # type: ignore
