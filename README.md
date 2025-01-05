# TaskIQ - asyncpg

TaskIQ-asyncpg is a plugin for taskiq that adds a new result backend based on PostgreSQL and [asyncpg](https://github.com/MagicStack/asyncpg).

## Installation

To use this project you must have installed core taskiq library:

```bash
pip install taskiq
```

This project can be installed using pip:

```bash
pip install taskiq-asyncpg
```

Or using poetry:

```
poetry add taskiq-asyncpg
```

## Usage

Let's see the example with the redis broker and PostgreSQL asyncpg result backend:

```python
# broker.py
import asyncio

from taskiq_redis import ListQueueBroker
from taskiq_asyncpg import AsyncpgResultBackend

asyncpg_result_backend = AsyncpgResultBackend(
    dsn="postgres://postgres:postgres@localhost:5432/postgres",
)

# Or you can use PubSubBroker if you need broadcasting
broker = ListQueueBroker(
    url="redis://localhost:6379",
    result_backend=asyncpg_result_backend,
)


@broker.task
async def best_task_ever() -> None:
    """Solve all problems in the world."""
    await asyncio.sleep(5.5)
    print("All problems are solved!")


async def main():
    await broker.startup()
    task = await best_task_ever.kiq()
    print(await task.wait_result())
    await broker.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
```

## AsyncpgResultBackend configuration

- `dsn`: connection string to PostgreSQL.
- `keep_results`: flag to not remove results from Redis after reading.
- `table_name`: name of the table in PostgreSQL to store TaskIQ results.
- `field_for_task_id`: type of a field for `task_id`, you may need it if you want to have length of task_id more than 255 symbols.
- `**connect_kwargs`: additional connection parameters, you can read more about it in [asyncpg](https://github.com/MagicStack/asyncpg) repository.
