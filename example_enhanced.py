"""
Example demonstrating the enhanced features of taskiq-pg with advisory locking, group coordination, and automatic cleanup.

**shell 1: start a worker**

```sh
$ bin/pg-up
$ export POSTGRESQL_URL="postgresql://postgres:postgres@localhost:25432/postgres"
$ uv run taskiq worker example_enhanced:broker
[2025-01-06 11:48:14,171][taskiq.worker][INFO   ][MainProcess] Pid of a main process: 80434
[2025-01-06 11:48:14,171][taskiq.worker][INFO   ][MainProcess] Starting 2 worker processes.
[2025-01-06 11:48:14,175][taskiq.process-manager][INFO   ][MainProcess] Started process worker-0 with pid 80436
[2025-01-06 11:48:14,176][taskiq.process-manager][INFO   ][MainProcess] Started process worker-1 with pid 80437
Processing update_profile for user user_123
Processing update_settings for user user_123
Generating daily_summary report
```

**shell 2: run the example script**

```sh
$ export POSTGRESQL_URL="postgresql://postgres:postgres@localhost:25432/postgres"
$ uv run example_enhanced.py
Example 1: Group coordination

Example 2: Message TTL
Task 1 result: is_err=False log=None return_value={'user_id': 'user_123', 'action': 'update_profile', 'status': 'completed'} execution_time=2.0 labels={'group_key': 'user_123'} error=None
Task 2 result: is_err=False log=None return_value={'user_id': 'user_123', 'action': 'update_settings', 'status': 'completed'} execution_time=2.0 labels={'group_key': 'user_123'} error=None
Task 3 result: is_err=False log=None return_value='daily_summary report generated successfully' execution_time=1.0 labels={'ttl': 3600} error=None
```

Note: Tasks 1 and 2 have the same group_key, so they execute sequentially, not concurrently.

**shell 1: stop the postgres db**

```sh
$ bin/pg-down
```
"""

import asyncio
import os

from taskiq.serializers.json_serializer import JSONSerializer

from taskiq_pg import AsyncpgBroker, AsyncpgResultBackend

# Connection string - update as needed for your PostgreSQL instance
dsn = os.getenv(
    "POSTGRESQL_URL",
    "postgres://postgres:postgres@localhost:15432/postgres",
)

# Initialize result backend
asyncpg_result_backend = AsyncpgResultBackend[object](
    dsn=dsn,
    serializer=JSONSerializer(),
)

# Initialize broker with enhanced features
broker = AsyncpgBroker(
    dsn=dsn,
    job_lock_keyspace=1,  # Advisory lock keyspace
    message_ttl=300,  # Keep completed messages for 5 minutes
    stuck_message_timeout=60,  # Consider message stuck after 1 minute
    enable_sweeping=True,  # Enable automatic cleanup
    sweep_interval=30,  # Sweep every 30 seconds
).with_result_backend(asyncpg_result_backend)


@broker.task()
async def process_user_data(user_id: str, action: str) -> dict[str, str]:
    """Process user data with group coordination."""
    print(f"process_user_data({user_id=}, {action=})")  # noqa: T201
    await asyncio.sleep(2)  # Simulate work
    return {"user_id": user_id, "action": action, "status": "completed"}


@broker.task()
async def generate_report(report_type: str) -> str:
    """Generate a report with TTL."""
    print(f"generate_report({report_type=})")  # noqa: T201
    await asyncio.sleep(1)
    return f"{report_type} report generated successfully"


async def main() -> None:
    """Demonstrate enhanced features."""
    await broker.startup()

    # Example 1: Group coordination
    # These tasks won't run concurrently because they have the same group_key
    task1 = (
        await process_user_data.kicker()
        .with_labels(group_key="user_123")
        .kiq("user_123", "update_profile")
    )

    task2 = (
        await process_user_data.kicker()
        .with_labels(group_key="user_123")
        .kiq("user_123", "update_settings")
    )

    # Example 2: Message TTL
    # This message will be automatically cleaned up after 1 hour
    task3 = (
        await generate_report.kicker()
        .with_labels(
            ttl=3600  # 1 hour
        )
        .kiq("daily_summary")
    )

    # Wait for results
    result1 = await task1.wait_result(timeout=5)
    print(f"{result1=}")  # noqa: T201

    result2 = await task2.wait_result(timeout=5)
    print(f"{result2=}")  # noqa: T201

    result3 = await task3.wait_result(timeout=3)
    print(f"{result3=}")  # noqa: T201

    await broker.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
