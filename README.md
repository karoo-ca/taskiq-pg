# TaskIQ - asyncpg

TaskIQ-pg is a plugin for taskiq that adds a new result backend and a new broker based on PostgreSQL and [asyncpg](https://github.com/MagicStack/asyncpg).

The broker makes use of Postgres' built in `LISTEN/NOTIFY` functionality.

This is a fork of [taskiq-psqlpy](https://github.com/taskiq-python/taskiq-psqlpy) that adds a broker (because PSQLPy does not currently support `LISTEN/NOTIFY`).

## Installation

To use this project you must have installed core taskiq library:

```bash
pip install taskiq
```

This project can be installed using pip:

```bash
pip install taskiq-pg
```

Or using poetry:

```
poetry add taskiq-pg
```

## Usage

An example with the broker and result backend:

```python
# example.py
import asyncio

from taskiq.serializers.json_serializer import JSONSerializer
from taskiq_pg import AsyncpgBroker, AsyncpgResultBackend

asyncpg_result_backend = AsyncpgResultBackend(
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


async def main() -> None:
    """Main."""
    await broker.startup()
    task = await best_task_ever.kiq()
    result = await task.wait_result(timeout=2)
    print(result)
    await broker.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
```

### Run example

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

### Details

The result backend stores the data as raw bytes by default, you can decode them in SQL:

```sql
select convert_from(result, 'UTF8') from taskiq_results;
-- Example results:
-- - success:
--   {
--     "is_err": false,
--     "log": null,
--     "return_value": "All problems are solved!",
--     "execution_time": 1.0,
--     "labels": {},
--     "error": null
--   }
-- - failure:
--   {
--     "is_err": true,
--     "log": null,
--     "return_value": null,
--     "execution_time": 10.0,
--     "labels": {},
--     "error": {
--       "exc_type": "ValueError",
--       "exc_message": ["Borked"],
--       "exc_module": "builtins",
--       "exc_cause": null,
--       "exc_context": null,
--       "exc_suppress_context": false
--     }
--   }
```

## AsyncpgResultBackend configuration

- `dsn`: connection string to PostgreSQL.
- `keep_results`: flag to not remove results from Redis after reading.
- `table_name`: name of the table in PostgreSQL to store TaskIQ results.
- `field_for_task_id`: type of a field for `task_id`, you may need it if you want to have length of task_id more than 255 symbols.
- `**connect_kwargs`: additional connection parameters, you can read more about it in [asyncpg](https://github.com/MagicStack/asyncpg) repository.

## AsyncpgBroker configuration

- `dsn`: Connection string to PostgreSQL.
- `result_backend`: Custom result backend.
- `task_id_generator`: Custom task_id generator.
- `channel_name`: Name of the channel to listen on.
- `table_name`: Name of the table to store messages.
- `max_retry_attempts`: Maximum number of message processing attempts.
- `connection_kwargs`: Additional arguments for asyncpg connection.
- `pool_kwargs`: Additional arguments for asyncpg pool creation.

## Benchmarking

`bench/throughput.py` is a standalone end-to-end throughput test. It spawns a real
`taskiq worker`, enqueues N tasks, and times the drain. The task is a 50/50 mix of
CPU busy-loop and `asyncio.sleep`, so the worker is the bottleneck.

```sh
bin/pg-up
export POSTGRESQL_URL="postgresql://postgres:postgres@localhost:25432/postgres"

# 5000 tasks, 1 worker, up to 100 concurrent async tasks
uv run python -m bench.throughput -n 5000 -w 1 -m 100
# tune per-task cost (seconds): uv run python -m bench.throughput --busy-seconds 0.002 --sleep-seconds 0.01

bin/pg-down
```

```text
end-to-end : 15.363s (325.5 tasks/s)
```

Note: the broker has no per-message lock, so with `-w > 1` every worker runs every
task — use `-w 1` for a clean number. See `--help` and the module docstring for more.

## Acknowledgements

Builds on work from [pgmq](https://github.com/oliverlambson/pgmq).
