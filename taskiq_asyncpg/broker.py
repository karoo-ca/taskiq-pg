import asyncio
import json
import logging
from typing import Any, AsyncGenerator, Callable, Optional, Self, TypeVar

import asyncpg
from taskiq import AckableMessage, AsyncBroker, AsyncResultBackend, BrokerMessage

from taskiq_asyncpg.broker_queries import (
    CREATE_TABLE_QUERY,
    DELETE_MESSAGE_QUERY,
    INSERT_MESSAGE_QUERY,
    SELECT_MESSAGE_QUERY,
)

_T = TypeVar("_T")
logger = logging.getLogger("taskiq.asyncpg_broker")


class AsyncpgBroker(AsyncBroker):
    """Broker that uses PostgreSQL and asyncpg with LISTEN/NOTIFY."""

    def __init__(
        self: Self,
        dsn: str = "postgresql://postgres:postgres@localhost:5432/postgres",
        result_backend: Optional[AsyncResultBackend[_T]] = None,
        task_id_generator: Optional[Callable[[], str]] = None,
        channel_name: str = "taskiq",
        table_name: str = "taskiq_messages",
        max_retry_attempts: int = 5,
        **connection_kwargs: Any,
    ) -> None:
        """
        Construct a new broker.

        :param dsn: Connection string to PostgreSQL.
        :param result_backend: Custom result backend.
        :param task_id_generator: Custom task_id generator.
        :param channel_name: Name of the channel to listen on.
        :param table_name: Name of the table to store messages.
        :param max_retry_attempts: Maximum number of message processing attempts.
        :param connection_kwargs: Additional arguments for asyncpg connection.
        """
        super().__init__(
            result_backend=result_backend,
            task_id_generator=task_id_generator,
        )
        self.dsn = dsn
        self.channel_name = channel_name
        self.table_name = table_name
        self.connection_kwargs = connection_kwargs
        self.max_retry_attempts = max_retry_attempts
        self.read_conn: Optional[asyncpg.Connection] = None
        self.write_pool: Optional[asyncpg.pool.Pool] = None
        self._queue: Optional[asyncio.Queue[str]] = None

    async def startup(self: Self) -> None:
        """Initialize the broker."""
        await super().startup()
        self.read_conn = await asyncpg.connect(self.dsn, **self.connection_kwargs)
        self.write_pool = await asyncpg.create_pool(self.dsn, **self.connection_kwargs)

        # Create messages table if it doesn't exist
        if self.read_conn is None:
            raise ValueError("self.read_conn is None")
        await self.read_conn.execute(CREATE_TABLE_QUERY.format(self.table_name))
        # Listen to the specified channel
        await self.read_conn.add_listener(self.channel_name, self._notification_handler)
        self._queue = asyncio.Queue()

    async def shutdown(self: Self) -> None:
        """Close all connections on shutdown."""
        await super().shutdown()
        if self.read_conn is not None:
            await self.read_conn.close()
        if self.write_pool is not None:
            await self.write_pool.close()

    def _notification_handler(self: Self, channel: str, payload: str) -> None:
        """Handle NOTIFY messages."""
        logger.debug(f"Received notification on channel {channel}: {payload}")
        if self._queue is not None:
            self._queue.put_nowait(payload)

    async def kick(self: Self, message: BrokerMessage) -> None:
        """
        Send message to the channel.

        Inserts the message into the database and sends a NOTIFY.

        :param message: Message to send.
        """
        if self.write_pool is None:
            raise ValueError("Please run startup before kicking.")

        async with self.write_pool.acquire() as conn:
            # Insert the message into the database
            message_inserted_id = await conn.fetchval(
                INSERT_MESSAGE_QUERY.format(self.table_name),
                message.task_id,
                message.task_name,
                message.message.decode(),
                json.dumps(message.labels),
            )

            delay_value = message.labels.get("delay")
            if delay_value is not None:
                delay_seconds = int(delay_value)
                asyncio.create_task(  # noqa: RUF006
                    self._schedule_notification(message_inserted_id, delay_seconds)
                )
            else:
                # Send a NOTIFY with the message ID as payload
                await conn.execute(
                    f"NOTIFY {self.channel_name}, '{message_inserted_id}'"
                )

    async def _schedule_notification(
        self: Self, message_id: int, delay_seconds: int
    ) -> None:
        """Schedule a notification to be sent after a delay."""
        await asyncio.sleep(delay_seconds)
        if self.write_pool is None:
            return
        async with self.write_pool.acquire() as conn:
            # Send NOTIFY
            await conn.execute(f"NOTIFY {self.channel_name}, '{message_id}'")

    async def listen(self: Self) -> AsyncGenerator[AckableMessage, None]:
        """
        Listen to the channel.

        Yields messages as they are received.

        :yields: AckableMessage instances.
        """
        if self.read_conn is None:
            raise ValueError("Call startup before starting listening.")
        if self._queue is None:
            raise ValueError("Startup did not initialize the queue.")

        while True:
            try:
                payload = await self._queue.get()
                # Payload is the message ID
                message_id = int(payload)
                # Fetch the message from database
                message_row = await self.read_conn.fetchrow(
                    SELECT_MESSAGE_QUERY.format(self.table_name), message_id
                )
                if message_row is None:
                    logger.warning(
                        f"Message with id {message_id} not found in database."
                    )
                    continue
                # Construct AckableMessage
                message_data = message_row["message"].encode()

                async def ack(*, _message_id: int = message_id) -> None:
                    if self.read_conn is None:
                        raise ValueError("Call startup before starting listening.")
                    # Delete the message from the database
                    await self.read_conn.execute(
                        DELETE_MESSAGE_QUERY.format(self.table_name),
                        _message_id,
                    )

                yield AckableMessage(data=message_data, ack=ack)
            except Exception as e:
                logger.exception(f"Error processing message: {e}")
                continue
