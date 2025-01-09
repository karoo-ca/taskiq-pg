"""
Example usage of AsyncpgBroker and AsyncpgResultBackend.

**shell 1: start a worker**

```sh
$ taskiq worker example:broker
[2025-01-06 11:48:14,171][taskiq.worker][INFO   ][MainProcess] Pid of a main process: 80434
[2025-01-06 11:48:14,171][taskiq.worker][INFO   ][MainProcess] Starting 2 worker processes.
[2025-01-06 11:48:14,175][taskiq.process-manager][INFO   ][MainProcess] Started process worker-0 with pid 80436
[2025-01-06 11:48:14,176][taskiq.process-manager][INFO   ][MainProcess] Started process worker-1 with pid 80437
Exception found while executing function: Borked
Traceback (most recent call last):
  File "/Users/oliverlambson/Github/karoo/taskiq-pg/.venv/lib/python3.13/site-packages/taskiq/receiver/receiver.py", line 271, in run_task
    returned = await target_future
               ^^^^^^^^^^^^^^^^^^^
  File "/Users/oliverlambson/Github/karoo/taskiq-pg/example.py", line 50, in worst_task_ever
    raise ValueError(msg)
ValueError: Borked
Exception found while executing function: Borked
Traceback (most recent call last):
  File "/Users/oliverlambson/Github/karoo/taskiq-pg/.venv/lib/python3.13/site-packages/taskiq/receiver/receiver.py", line 271, in run_task
    returned = await target_future
               ^^^^^^^^^^^^^^^^^^^
  File "/Users/oliverlambson/Github/karoo/taskiq-pg/example.py", line 50, in worst_task_ever
    raise ValueError(msg)
ValueError: Borked
```

**shell 2: run the example script**

```sh
$ python example.py
is_err=False log=None return_value='All problems are solved!' execution_time=1.0 labels={} error=None
Save reference to fc238b66b9554315b5ca3d16bcab5a8d so you can look at result later
```
"""  # noqa: E501

import asyncio

from taskiq.serializers import JSONSerializer

from taskiq_pg import AsyncpgBroker, AsyncpgResultBackend

asyncpg_result_backend: AsyncpgResultBackend[object] = AsyncpgResultBackend(
    dsn="postgres://postgres:postgres@localhost:15432/postgres",
    serializer=JSONSerializer(),
)

broker = AsyncpgBroker(
    dsn="postgres://postgres:postgres@localhost:15432/postgres",
).with_result_backend(asyncpg_result_backend)


@broker.task()
async def best_task_ever() -> str:
    """Solve all problems in the world."""
    await asyncio.sleep(1.0)
    return "All problems are solved!"


@broker.task()
async def worst_task_ever() -> str:
    """Solve all problems in the world."""
    await asyncio.sleep(10.0)
    msg = "Borked"
    raise ValueError(msg)
    return "borked"


async def main() -> None:
    """Main."""
    await broker.startup()
    task = await best_task_ever.kiq()
    result = await task.wait_result(timeout=2)
    print(result)  # noqa: T201
    task = await worst_task_ever.kiq()  # even though this fails, broker continues on
    print(f"Save reference to {task.task_id} so you can look at result later")  # noqa: T201
    await broker.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
