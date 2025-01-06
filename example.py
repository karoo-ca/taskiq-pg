"""
Example usage of AsyncpgBroker and AsyncpgResultBackend.

**shell 1: start a worker**

```sh
$ taskiq worker example:broker
[2025-01-06 11:48:14,171][taskiq.worker][INFO   ][MainProcess] Pid of a main process: 80434
[2025-01-06 11:48:14,171][taskiq.worker][INFO   ][MainProcess] Starting 2 worker processes.
[2025-01-06 11:48:14,175][taskiq.process-manager][INFO   ][MainProcess] Started process worker-0 with pid 80436
[2025-01-06 11:48:14,176][taskiq.process-manager][INFO   ][MainProcess] Started process worker-1 with pid 80437
```

**shell 2: run the example script**

```sh
$ python example.py
is_err=False log=None return_value='All problems are solved!' execution_time=1.0 labels={} error=None
```
"""  # noqa: E501

import asyncio

from taskiq_pg import AsyncpgBroker, AsyncpgResultBackend

asyncpg_result_backend: AsyncpgResultBackend[object] = AsyncpgResultBackend(
    dsn="postgres://postgres:postgres@localhost:15432/postgres",
)

broker = AsyncpgBroker(
    dsn="postgres://postgres:postgres@localhost:15432/postgres",
).with_result_backend(asyncpg_result_backend)


@broker.task()
async def best_task_ever() -> str:
    """Solve all problems in the world."""
    await asyncio.sleep(1.0)
    return "All problems are solved!"


async def main() -> None:
    """Main."""
    await broker.startup()
    task = await best_task_ever.kiq()
    result = await task.wait_result(timeout=2)
    print(result)  # noqa: T201
    await broker.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
