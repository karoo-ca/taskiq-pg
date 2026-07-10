"""Throughput stress test for the taskiq-pg broker.

This module is both the worker import target (``bench.throughput:broker``) and
the benchmark driver (``python -m bench.throughput``). The driver spawns a real
``taskiq worker``, enqueues N tasks, and times how long the worker takes to
drain them.

DSN/table/channel are read from ``BENCH_*`` env vars so the spawned worker
rebuilds an identical broker:

* ``BENCH_DSN``           - PostgreSQL DSN (default: POSTGRESQL_URL or localhost:25432)
* ``BENCH_TABLE``         - messages table name (default: random ``bench_<rand>``)
* ``BENCH_CHANNEL``       - LISTEN/NOTIFY channel (default: ``<table>_ch``)
* ``BENCH_BUSY_SECONDS``  - CPU busy-loop seconds for CPU-bound tasks (default: 0.005)
* ``BENCH_SLEEP_SECONDS`` - asyncio.sleep seconds for I/O-bound tasks (default: 0.005)

This broker has no claim/lock mechanism: every worker receives every NOTIFY, so
``--workers > 1`` runs each task once per worker. Use ``--workers 1`` for a
clean per-process number.
"""

from __future__ import annotations

import argparse
import asyncio
import os
import random
import signal
import string
import sys
import time

from taskiq_pg.broker import AsyncpgBroker

DSN = os.environ.setdefault(
    "BENCH_DSN",
    os.environ.get("POSTGRESQL_URL")
    or "postgresql://postgres:postgres@localhost:25432/postgres",
)
TABLE = os.environ.setdefault(
    "BENCH_TABLE",
    "bench_" + "".join(random.choice(string.ascii_lowercase) for _ in range(8)),
)
CHANNEL = os.environ.setdefault("BENCH_CHANNEL", f"{TABLE}_ch")
BUSY_SECONDS = float(os.environ.setdefault("BENCH_BUSY_SECONDS", "0.005"))
SLEEP_SECONDS = float(os.environ.setdefault("BENCH_SLEEP_SECONDS", "0.005"))

broker = AsyncpgBroker(
    dsn=DSN,
    channel_name=CHANNEL,
    table_name=TABLE,
    connection_kwargs={"server_settings": {"application_name": "bench_worker"}},
    pool_kwargs={"server_settings": {"application_name": "bench_worker"}},
)


@broker.task(task_name="bench_task")
async def bench_task() -> int:
    """Mixed workload: ~50% CPU busy-loop, ~50% async sleep."""
    if random.random() < 0.5:
        deadline = time.perf_counter() + BUSY_SECONDS
        total = 0
        while time.perf_counter() < deadline:
            total += 1
        return total
    await asyncio.sleep(SLEEP_SECONDS)
    return 0


# --- Driver -------------------------------------------------------------------


def _repo_root() -> str:
    from pathlib import Path

    return str(Path(__file__).resolve().parents[1])


async def _count_rows() -> int:
    value = await broker.write_pool.fetchval(f"SELECT count(*) FROM {TABLE}")
    return int(value or 0)


async def _remaining_ids() -> list[int]:
    rows = await broker.write_pool.fetch(f"SELECT id FROM {TABLE}")
    return [int(r["id"]) for r in rows]


async def _notify(ids: list[int]) -> None:
    async with broker.write_pool.acquire() as conn:
        for message_id in ids:
            await conn.execute(f"NOTIFY {CHANNEL}, '{message_id}'")


async def _spawn_worker(
    workers: int, max_async_tasks: int, show_output: bool
) -> asyncio.subprocess.Process:
    root = _repo_root()
    env = os.environ.copy()
    env["PYTHONPATH"] = root + os.pathsep + env.get("PYTHONPATH", "")
    cmd = [
        "uv",
        "run",
        "taskiq",
        "worker",
        "bench.throughput:broker",
        "--workers",
        str(workers),
        "--max-async-tasks",
        str(max_async_tasks),
        "--ack-type",
        "when_executed",
        "--log-level",
        "INFO" if show_output else "ERROR",
    ]
    pipe = None if show_output else asyncio.subprocess.DEVNULL
    return await asyncio.create_subprocess_exec(
        *cmd,
        cwd=root,
        env=env,
        stdout=pipe,
        stderr=pipe,
        start_new_session=True,  # own process group for clean teardown
    )


async def _wait_ready(timeout: float) -> bool:
    """Drain a single sentinel task to confirm the full path works.

    Re-NOTIFYs periodically in case the worker is not LISTENing yet; on this
    broker a missed NOTIFY is lost forever.
    """
    await bench_task.kiq()
    sentinel_id = await broker.write_pool.fetchval(
        f"SELECT id FROM {TABLE} ORDER BY id DESC LIMIT 1"
    )
    if sentinel_id is None:
        # The sentinel was processed before we could read its id; if the table
        # has drained the worker is functioning, so treat this as success.
        return await _count_rows() == 0

    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if await _count_rows() == 0:
            return True
        await _notify([int(sentinel_id)])
        await asyncio.sleep(0.5)
    return await _count_rows() == 0


async def _kick_many(count: int, concurrency: int) -> float:
    sem = asyncio.Semaphore(concurrency)

    async def one() -> None:
        async with sem:
            await bench_task.kiq()

    start = time.monotonic()
    await asyncio.gather(*(one() for _ in range(count)))
    return time.monotonic() - start


async def _drain(drain_timeout: float, stall_timeout: float) -> float:
    """Poll until the table is empty, returning processing wall-time.

    Re-NOTIFYs remaining rows if no progress is made within ``stall_timeout``.
    """
    start = time.monotonic()
    deadline = start + drain_timeout
    last_count = await _count_rows()
    last_progress = start
    recovered = False

    while True:
        remaining = await _count_rows()
        if remaining == 0:
            if recovered:
                print(
                    "  WARNING: had to re-NOTIFY stalled rows; numbers are unreliable",
                    file=sys.stderr,
                )
            return time.monotonic() - start

        now = time.monotonic()
        if remaining < last_count:
            last_count = remaining
            last_progress = now
        elif now - last_progress > stall_timeout:
            ids = await _remaining_ids()
            print(f"  stall detected ({remaining} rows); re-notifying", file=sys.stderr)
            await _notify(ids)
            recovered = True
            last_progress = now

        if now > deadline:
            ids = await _remaining_ids()
            raise TimeoutError(
                f"drain timed out with {len(ids)} rows remaining after "
                f"{drain_timeout:.0f}s"
            )
        await asyncio.sleep(0.1)


async def _teardown_worker(proc: asyncio.subprocess.Process) -> None:
    if proc.returncode is not None:
        return
    try:
        os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
    except ProcessLookupError:
        return
    try:
        await asyncio.wait_for(proc.wait(), timeout=10)
    except asyncio.TimeoutError:
        os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
        await proc.wait()


def _print_report(
    count: int, workers: int, max_async_tasks: int, total_elapsed: float
) -> None:
    print("\n=== throughput report ===")
    print(f"tasks                : {count}")
    print(f"workers              : {workers}")
    print(f"max_async_tasks      : {max_async_tasks}")
    print(f"table                : {TABLE}")
    print("-")
    print(
        f"end-to-end           : {total_elapsed:8.3f}s  "
        f"({count / total_elapsed:10.1f} tasks/s)"
    )
    if workers > 1:
        print(
            f"\nNOTE: --workers {workers} -> each task executed up to {workers}x "
            "(no claim/lock on this broker); drain reflects duplicate work."
        )


async def _run(args: argparse.Namespace) -> int:
    os.environ["BENCH_BUSY_SECONDS"] = str(args.busy_seconds)
    os.environ["BENCH_SLEEP_SECONDS"] = str(args.sleep_seconds)

    await broker.startup()
    # The driver only enqueues + polls, so drop its listener.
    if broker.read_conn is not None:
        await broker.read_conn.remove_listener(CHANNEL, broker._notification_handler)

    proc = None
    try:
        proc = await _spawn_worker(
            args.workers, args.max_async_tasks, args.worker_output
        )
        print(f"spawned worker (pid={proc.pid}); waiting for readiness...")
        if not await _wait_ready(args.ready_timeout):
            print("worker did not become ready in time", file=sys.stderr)
            return 1
        print("worker ready; enqueuing tasks...")

        kick_start = time.monotonic()
        await _kick_many(args.count, args.kick_concurrency)
        await _drain(args.drain_timeout, args.stall_timeout)
        total_elapsed = time.monotonic() - kick_start

        _print_report(args.count, args.workers, args.max_async_tasks, total_elapsed)
        return 0
    finally:
        if proc is not None:
            await _teardown_worker(proc)
        await broker.write_pool.execute(f"DROP TABLE IF EXISTS {TABLE}")
        await broker.shutdown()


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="End-to-end throughput stress test for the taskiq-pg broker "
        "(uses a real taskiq worker). DSN/table/channel are read from BENCH_* "
        "env vars; see module docstring."
    )
    parser.add_argument("-n", "--count", type=int, default=5000)
    parser.add_argument("-w", "--workers", type=int, default=1)
    parser.add_argument("-m", "--max-async-tasks", type=int, default=100)
    parser.add_argument("--busy-seconds", type=float, default=BUSY_SECONDS)
    parser.add_argument("--sleep-seconds", type=float, default=SLEEP_SECONDS)
    parser.add_argument("--kick-concurrency", type=int, default=50)
    parser.add_argument("--ready-timeout", type=float, default=30.0)
    parser.add_argument("--drain-timeout", type=float, default=300.0)
    parser.add_argument("--stall-timeout", type=float, default=10.0)
    parser.add_argument("--worker-output", action="store_true")
    return parser.parse_args()


def main() -> None:
    raise SystemExit(asyncio.run(_run(_parse_args())))


if __name__ == "__main__":
    main()
