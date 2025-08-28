"""Asyncpg broker implementation using asyncpg and PostgreSQL LISTEN/NOTIFY."""

import asyncio
import contextlib
import json
import logging
from collections.abc import AsyncGenerator
from typing import Any, Callable, Optional, TypeVar, Union, Mapping, Sequence

import asyncpg
from taskiq import (
    AckableMessage,
    AsyncBroker,
    AsyncResultBackend,
    BrokerMessage,
)
from typing_extensions import override

from taskiq_pg.broker_queries import (
    CLEANUP_EXPIRED_QUERY,
    COMPLETE_MESSAGE_QUERY,
    CREATE_UPDATE_TABLE_QUERY,
    DEQUEUE_MESSAGE_QUERY,
    SWEEP_MESSAGES_QUERY,
    NOTIFY_EXISTING_MESSAGES_QUERY,
    INSERT_MESSAGE_QUERY,
    RELEASE_ALL_ADVISORY_LOCKS_QUERY,
    NOTIFY_QUERY,
    ACQUIRE_ADVISORY_LOCK_QUERY,
    RELEASE_ADVISORY_LOCK_QUERY,
    UPDATE_MESSAGE_STATUS_QUERY,
)

_T = TypeVar("_T")
logger = logging.getLogger("taskiq.asyncpg_broker")


class AsyncpgBroker(AsyncBroker):
    """Broker that uses PostgreSQL and asyncpg with LISTEN/NOTIFY."""

    def __init__(
        self,
        dsn: Union[
            str,
            Callable[[], str],
        ] = "postgresql://postgres:postgres@localhost:5432/postgres",
        result_backend: Optional[AsyncResultBackend[_T]] = None,
        task_id_generator: Optional[Callable[[], str]] = None,
        channel_name: str = "taskiq",
        table_name: str = "taskiq_messages",
        max_retry_attempts: int = 5,
        connection_kwargs: Optional[dict[str, Any]] = None,
        pool_kwargs: Optional[dict[str, Any]] = None,
        job_lock_keyspace: int = 1,
        message_ttl: int = 86400,  # 24 hours default
        stuck_message_timeout: int = 300,  # 5 minutes default
        enable_sweeping: bool = True,
        sweep_interval: int = 60,  # 1 minute default
    ) -> None:
        """
        Construct a new broker.

        :param dsn: connection string to PostgreSQL, or callable returning one.
        :param result_backend: Custom result backend.
        :param task_id_generator: Custom task_id generator.
        :param channel_name: Name of the channel to listen on.
        :param table_name: Name of the table to store messages.
        :param max_retry_attempts: Maximum number of message processing attempts.
        :param connection_kwargs: Additional arguments for asyncpg connection.
        :param pool_kwargs: Additional arguments for asyncpg pool creation.
        :param job_lock_keyspace: Advisory lock keyspace for jobs.
        :param message_ttl: Time to live for completed messages in seconds.
        :param stuck_message_timeout: Time before message is considered stuck.
        :param enable_sweeping: Enable automatic cleanup of stuck messages.
        :param sweep_interval: Interval between sweep operations in seconds.
        """
        super().__init__(
            result_backend=result_backend,
            task_id_generator=task_id_generator,
        )
        self._dsn: Union[str, Callable[[], str]] = dsn
        self.channel_name: str = channel_name
        self.table_name: str = table_name
        self.connection_kwargs: dict[str, Any] = (
            connection_kwargs if connection_kwargs else {}
        )
        self.pool_kwargs: dict[str, Any] = pool_kwargs if pool_kwargs else {}
        self.max_retry_attempts: int = max_retry_attempts
        self.job_lock_keyspace: int = job_lock_keyspace
        self.message_ttl: int = message_ttl
        self.stuck_message_timeout: int = stuck_message_timeout
        self.enable_sweeping: bool = enable_sweeping
        self.sweep_interval: int = sweep_interval

        self.read_conn: Optional["asyncpg.Connection[asyncpg.Record]"] = None
        self.dequeue_conn: Optional["asyncpg.Connection[asyncpg.Record]"] = None
        self.write_pool: Optional["asyncpg.pool.Pool[asyncpg.Record]"] = None
        self._queue: Optional[asyncio.Queue[str]] = None
        self._sweep_task: Optional[asyncio.Task[None]] = None
        self._dequeue_lock: asyncio.Lock = asyncio.Lock()
        self._connection_lock: asyncio.Lock = asyncio.Lock()

    @property
    def dsn(self) -> str:
        """Get the DSN string.

        Returns the DSN string or None if not set.
        """
        if callable(self._dsn):
            return self._dsn()
        return self._dsn

    @override
    async def startup(self) -> None:
        """Initialize the broker."""
        await super().startup()

        self.read_conn = await asyncpg.connect(self.dsn, **self.connection_kwargs)
        self.dequeue_conn = await asyncpg.connect(self.dsn, **self.connection_kwargs)
        self.write_pool = await asyncpg.create_pool(self.dsn, **self.pool_kwargs)

        if self.read_conn is None:
            msg = "read_conn not initialized"
            raise RuntimeError(msg)
        if self.dequeue_conn is None:
            msg = "dequeue_conn not initialized"
            raise RuntimeError(msg)
        if self.write_pool is None:
            msg = "write_pool not initialized"
            raise RuntimeError(msg)

        async with self.write_pool.acquire() as conn:
            # Format the CREATE_TABLE_QUERY with proper table names for indexes
            table_name_safe = self.table_name.replace('"', "").replace(" ", "_")
            create_query = CREATE_UPDATE_TABLE_QUERY.safe_substitute(
                table_name=self.table_name,
                table_name_safe=table_name_safe,
            )

            _ = await conn.execute(create_query)

            # PRIME THE PUMP: Notify all existing queued messages
            notify_query = NOTIFY_EXISTING_MESSAGES_QUERY.safe_substitute(
                table_name=self.table_name,
                channel_name=self.channel_name,
            )

            _ = await conn.execute(notify_query)

        await self.read_conn.add_listener(self.channel_name, self._notification_handler)
        self._queue = asyncio.Queue()

        # Start sweep task if enabled
        if self.enable_sweeping:
            self._sweep_task = asyncio.create_task(self._sweep_loop())

    @override
    async def shutdown(self) -> None:
        """Close all connections on shutdown."""
        await super().shutdown()

        # Stop sweep task
        if self._sweep_task is not None:
            _ = self._sweep_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._sweep_task

        # Release all advisory locks before closing connections
        if self.dequeue_conn is not None:
            try:
                _ = await self.dequeue_conn.execute(RELEASE_ALL_ADVISORY_LOCKS_QUERY)
            except Exception as e:
                logger.warning(f"Failed to release advisory locks: {e}")

        if self.read_conn is not None:
            await self.read_conn.close()
        if self.dequeue_conn is not None:
            await self.dequeue_conn.close()
        if self.write_pool is not None:
            await self.write_pool.close()

    def _notification_handler(
        self,
        _con_ref: Union[
            "asyncpg.Connection[asyncpg.Record]",
            "asyncpg.pool.PoolConnectionProxy[asyncpg.Record]",
        ],
        _pid: int,
        channel: str,
        payload: object,
        /,
    ) -> None:
        """Handle NOTIFY messages.

        From asyncpg.connection.add_listener docstring:
            A callable or a coroutine function receiving the following arguments:
            **con_ref**: a Connection the callback is registered with;
            **pid**: PID of the Postgres server that sent the notification;
            **channel**: name of the channel the notification was sent to;
            **payload**: the payload.
        """
        logger.debug(f"Received notification on channel {channel}: {payload}")
        if self._queue is None:
            logger.error("Notification received but queue is not initialized")
            return

        try:
            # Try to parse the payload as a JSON batch first
            data = json.loads(str(payload))
            if (
                isinstance(data, Mapping)
                and "ids" in data
                and isinstance(data["ids"], Sequence)
            ):
                for message_id in data["ids"]:
                    self._queue.put_nowait(str(message_id))
                logger.info(
                    f"Queued {len(data['ids'])} messages from batch notification.",
                )
            else:
                # Fallback for single ID payload
                self._queue.put_nowait(str(payload))
        except (json.JSONDecodeError, TypeError):
            # If it's not JSON, treat it as a single ID (legacy format)
            self._queue.put_nowait(str(payload))

    @override
    async def kick(self, message: BrokerMessage) -> None:
        """
        Send message to the channel.

        Inserts the message into the database and sends a NOTIFY.

        :param message: Message to send.
        """
        if self.write_pool is None:
            raise ValueError("Please run startup before kicking.")

        async with self.write_pool.acquire() as conn:
            # Extract group_key, ttl, and delay from labels if present
            group_key = message.labels.get("group_key")
            ttl = message.labels.get("ttl", self.message_ttl)
            delay_value = message.labels.get("delay")

            # Calculate expire_at based on TTL
            if ttl and isinstance(ttl, (int, float)) and ttl > 0:
                # Use PostgreSQL interval for expire_at
                expire_at_query = f"NOW() + INTERVAL '{int(ttl)} seconds'"
            else:
                expire_at_query = "NULL"

            # Calculate scheduled_at based on delay
            if delay_value is not None:
                delay_seconds = int(delay_value)
                scheduled_at_query = f"NOW() + INTERVAL '{delay_seconds} seconds'"
            else:
                scheduled_at_query = "NOW()"

            # Insert the message into the database
            result = await conn.fetchrow(
                INSERT_MESSAGE_QUERY.safe_substitute(
                    table_name=self.table_name,
                    task_id=message.task_id,
                    task_name=message.task_name,
                    message=message.message.decode(),
                    labels=json.dumps(message.labels),
                    group_key=group_key,
                    expire_at=expire_at_query,
                    scheduled_at=scheduled_at_query,
                ),
            )
            if result is None:
                raise RuntimeError("Failed to insert message")

            message_inserted_id = result["id"]
            result["lock_key"]

            # Always send a NOTIFY - the dequeue logic will check scheduled_at
            _ = await conn.execute(
                NOTIFY_QUERY.safe_substitute(
                    channel_name=self.channel_name, payload=message_inserted_id,
                ),
            )

    async def _schedule_notification(self, message_id: int, delay_seconds: int) -> None:
        """Schedule a notification to be sent after a delay."""
        await asyncio.sleep(delay_seconds)
        if self.write_pool is None:
            return
        async with self.write_pool.acquire() as conn:
            # Send NOTIFY
            _ = await conn.execute(
                NOTIFY_QUERY.safe_substitute(
                    channel_name=self.channel_name, payload=message_id,
                ),
            )

    @override
    async def listen(self) -> AsyncGenerator[AckableMessage, None]:  # noqa: C901
        """
        Listen to the channel.

        Yields messages as they are received using proper dequeuing with locking.

        :yields: AckableMessage instances.
        """
        if self.dequeue_conn is None:
            raise ValueError("Call startup before starting listening.")
        if self._queue is None:
            raise ValueError("Startup did not initialize the queue.")

        while True:
            try:
                # First try to dequeue a message directly
                message_row = await self._dequeue_message()

                if message_row is None:
                    # No message available, wait for notification or timeout
                    try:
                        _ = await asyncio.wait_for(self._queue.get(), timeout=1.0)
                        # Notification received, try to dequeue again
                        message_row = await self._dequeue_message()
                    except asyncio.TimeoutError:
                        # Timeout - check again for scheduled messages
                        message_row = await self._dequeue_message()

                if message_row is None:
                    continue

                message_id = message_row["id"]
                lock_key = message_row["lock_key"]

                if message_row.get("message") is None:
                    msg = "Message row does not have 'message' column"
                    raise ValueError(msg)
                message_str = message_row["message"]
                if not isinstance(message_str, str):
                    msg = "message is not a string"
                    raise ValueError(msg)
                message_data = message_str.encode()

                async def ack(
                    *, _message_id: int = message_id, _lock_key: int = lock_key
                ) -> None:
                    if self.write_pool is None:
                        raise ValueError("Call startup before starting listening.")

                    async with self.write_pool.acquire() as conn:
                        # Mark message as completed with TTL
                        _ = await conn.execute(
                            COMPLETE_MESSAGE_QUERY.safe_substitute(
                                table_name=self.table_name,
                                message_ttl=self.message_ttl,
                                message_id=_message_id,
                            ),
                        )

                        # Release the advisory lock
                        _ = await conn.execute(
                            RELEASE_ADVISORY_LOCK_QUERY.safe_substitute(
                                job_lock_keyspace=self.job_lock_keyspace,
                                lock_key=_lock_key,
                            ),
                        )

                yield AckableMessage(data=message_data, ack=ack)
            except Exception as e:
                logger.exception(f"Error processing message: {e}")
                continue

    async def _dequeue_message(self) -> Optional[asyncpg.Record]:
        """
        Dequeue a message using FOR UPDATE SKIP LOCKED.

        Returns the message row if one is available, None otherwise.
        """
        if self.dequeue_conn is None:
            return None

        async with self._dequeue_lock:
            try:
                # Check connection health
                await self._ensure_connection_healthy()

                # Format the dequeue query
                dequeue_query = DEQUEUE_MESSAGE_QUERY.safe_substitute(
                    table_name=self.table_name,
                )

                # Execute dequeue with advisory lock acquisition
                async with self.dequeue_conn.transaction():
                    # First dequeue and mark as active
                    message_row = await self.dequeue_conn.fetchrow(dequeue_query)

                    if message_row is None:
                        return None

                    # Try to acquire advisory lock
                    lock_acquired = await self.dequeue_conn.fetchval(
                        ACQUIRE_ADVISORY_LOCK_QUERY.safe_substitute(
                            job_lock_keyspace=self.job_lock_keyspace,
                            lock_key=message_row["lock_key"],
                        ),
                    )

                    if not lock_acquired:
                        logger.warning(
                            f"Could not acquire lock for message {message_row['id']}. This may indicate a race condition.",  # noqa: E501
                        )
                        # Reset message status back to queued
                        _ = await self.dequeue_conn.execute(
                            UPDATE_MESSAGE_STATUS_QUERY.safe_substitute(
                                table_name=self.table_name,
                                message_status="queued",
                                message_id=message_row["id"],
                            ),
                        )
                        return None

                    return message_row

            except Exception as e:
                logger.error(f"Error dequeuing message: {e}")
                return None

    async def _ensure_connection_healthy(self) -> None:
        """Ensure the dequeue connection is healthy, reconnect if needed."""
        if self.dequeue_conn is None:
            return

        async with self._connection_lock:
            try:
                # Simple health check query
                await self.dequeue_conn.fetchval("SELECT 1")
            except Exception as e:
                logger.warning(f"Dequeue connection unhealthy, reconnecting: {e}")
                with contextlib.suppress(Exception):
                    await self.dequeue_conn.close()

                self.dequeue_conn = await asyncpg.connect(
                    self.dsn, **self.connection_kwargs
                )

    async def _sweep_loop(self) -> None:
        """Background task to sweep stuck messages and clean up expired ones."""
        while True:
            try:
                await asyncio.sleep(self.sweep_interval)

                # Sweep stuck messages
                await self._sweep_stuck_messages()

                # Clean up expired messages
                await self._cleanup_expired_messages()

            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error(f"Error in sweep loop: {e}")

    async def _sweep_stuck_messages(self) -> None:
        """Sweep messages that are stuck (no lock held)."""
        if self.write_pool is None:
            return

        try:
            async with self.write_pool.acquire() as conn:
                sweep_query = SWEEP_MESSAGES_QUERY.safe_substitute(
                    table_name=self.table_name,
                )

                requeued = await conn.fetch(
                    sweep_query,
                    self.job_lock_keyspace,
                    self.stuck_message_timeout,
                )

                if requeued and requeued["requeued_ids"]:
                    requeued_ids = requeued["requeued_ids"]
                    logger.info(f"Re-queued {len(requeued_ids)} stuck messages.")
                    # Send a notification for the newly requeued tasks.
                    payload = json.dumps({"ids": requeued_ids})
                    notify_query = NOTIFY_QUERY.safe_substitute(
                        channel_name=self.channel_name, payload=payload,
                    )
                    await conn.execute(notify_query)

        except Exception as e:
            logger.error(f"Error sweeping stuck messages: {e}")

    async def _cleanup_expired_messages(self) -> None:
        """Clean up messages that have expired."""
        if self.write_pool is None:
            return

        try:
            async with self.write_pool.acquire() as conn:
                cleanup_query = CLEANUP_EXPIRED_QUERY.safe_substitute(
                    table_name=self.table_name,
                )

                result = await conn.execute(cleanup_query)
                deleted_count = int(result.split()[-1]) if result else 0

                if deleted_count > 0:
                    logger.debug(f"Cleaned up {deleted_count} expired messages")

        except Exception as e:
            logger.error(f"Error cleaning up expired messages: {e}")
