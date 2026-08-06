# Errata — Known Limitations

Unfixed correctness bugs in the locking/dedup layer. Read before trusting
advisory locking, the stuck-message sweep, or message TTL.

## 1. Long tasks get re-delivered (broken sweep)

Any task running longer than `stuck_message_timeout` (default 300s) is requeued
and re-run, repeatedly, until it completes. At-least-once, not exactly-once.

Cause: dequeue takes a *xact*-scoped advisory lock that's gone once the dequeue
txn commits, so nothing is held during processing. The sweep flags `active` rows
that have no held lock in `pg_locks` — i.e. all of them — and ages them from
`created_at` (enqueue time), not claim time. `max_retry_attempts` is not
enforced.

Workaround: `enable_sweeping=False`, or `stuck_message_timeout` > worst-case
runtime. Make tasks idempotent.

Fix: use a `locked_at` claim timestamp (set on dequeue, sweep on
`status='active' AND locked_at < NOW() - timeout`); drop the `pg_locks` join.

## 2. TTL strands queued messages; `ttl<=0` crashes

`expire_at` is set at *insert*, and dequeue skips `expire_at <= NOW()`. A message
sitting queued past its TTL (default 24h) becomes un-dequeuable and is never
cleaned (cleanup only touches `completed`) — a poison row. A `ttl<=0` label binds
the string `"NULL"` to a `timestamptz` column → asyncpg `DataError` on `kick`.

Workaround: keep `message_ttl` > max queue wait; don't pass `ttl<=0`.

Fix: set `expire_at` on completion only; bind `None`, never `"NULL"`.

## 3. Delays/TTLs use naive local time

`kick` uses naive `datetime.now()` for the `timestamptz` `scheduled_at` /
`expire_at`. asyncpg treats naive datetimes as UTC, so off-UTC hosts skew
delays/TTLs by the local offset.

Workaround: run brokers in UTC.

Fix: `datetime.now(timezone.utc)`, or compute server-side with `NOW() + INTERVAL`.
