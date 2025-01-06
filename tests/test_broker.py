import asyncio
import json
import uuid

import asyncpg
import pytest
from taskiq import AckableMessage, BrokerMessage
from taskiq.utils import maybe_awaitable

from taskiq_pg import AsyncpgBroker
from taskiq_pg.broker_queries import (
    INSERT_MESSAGE_QUERY,
)


async def get_first_task(asyncpg_broker: AsyncpgBroker) -> AckableMessage:
    """
    Get the first message from the broker's listen method.

    :param broker: Instance of AsyncpgBroker.
    :return: The first AckableMessage received.
    """
    async for message in asyncpg_broker.listen():
        return message
    msg = "Unreachable"
    raise RuntimeError(msg)


@pytest.mark.anyio
async def test_kick_success(asyncpg_broker: AsyncpgBroker) -> None:
    """
    Test that messages are published and read correctly.

    We kick the message, listen to the queue, and check that
    the received message matches what was sent.
    """
    # Create a unique task ID and name
    task_id = uuid.uuid4().hex
    task_name = uuid.uuid4().hex

    # Construct the message
    sent = BrokerMessage(
        task_id=task_id,
        task_name=task_name,
        message=b"my_msg",
        labels={
            "label1": "val1",
        },
    )

    # Send the message
    await asyncpg_broker.kick(sent)

    # Listen for the message
    message = await asyncio.wait_for(get_first_task(asyncpg_broker), timeout=1.0)

    # Check that the received message matches the sent message
    assert message.data == sent.message

    # Acknowledge the message
    await maybe_awaitable(message.ack())


@pytest.mark.anyio
async def test_startup(asyncpg_broker: AsyncpgBroker) -> None:
    """
    Test the startup process of the broker.

    We drop the messages table, restart the broker, and ensure
    that the table is recreated.
    """
    # Drop the messages table
    conn = await asyncpg.connect(asyncpg_broker.dsn)
    await conn.execute(f"DROP TABLE IF EXISTS {asyncpg_broker.table_name}")
    await conn.close()

    # Shutdown and restart the broker
    await asyncpg_broker.shutdown()
    await asyncpg_broker.startup()

    # Verify that the table exists
    conn = await asyncpg.connect(asyncpg_broker.dsn)
    table_exists = await conn.fetchval(
        """
        SELECT EXISTS (
            SELECT FROM information_schema.tables
            WHERE table_schema = 'public' AND table_name = $1
        )
        """,
        asyncpg_broker.table_name,
    )
    await conn.close()
    assert table_exists


@pytest.mark.anyio
async def test_listen(asyncpg_broker: AsyncpgBroker) -> None:
    """
    Test listen.

    Test that the broker can listen to messages inserted directly into the database
    and notified via the channel.
    """
    # Insert a message directly into the database
    conn = await asyncpg.connect(dsn=asyncpg_broker.dsn)
    message_content = b"test_message"
    task_id = uuid.uuid4().hex
    task_name = "test_task"
    labels = {"label1": "label_val"}
    message_id = await conn.fetchval(
        INSERT_MESSAGE_QUERY.format(asyncpg_broker.table_name),
        task_id,
        task_name,
        message_content.decode(),
        json.dumps(labels),
    )
    # Send a NOTIFY with the message ID
    await conn.execute(f"NOTIFY {asyncpg_broker.channel_name}, '{message_id}'")
    await conn.close()

    # Listen for the message
    message = await asyncio.wait_for(get_first_task(asyncpg_broker), timeout=1.0)
    assert message.data == message_content

    # Acknowledge the message
    await maybe_awaitable(message.ack())


@pytest.mark.anyio
async def test_wrong_format(asyncpg_broker: AsyncpgBroker) -> None:
    """Test that messages with incorrect formats are still received."""
    # Insert a message with missing task_id and task_name
    conn = await asyncpg.connect(dsn=asyncpg_broker.dsn)
    message_id = await conn.fetchval(
        INSERT_MESSAGE_QUERY.format(asyncpg_broker.table_name),
        "",  # Missing task_id
        "",  # Missing task_name
        "wrong",  # Message content
        json.dumps({}),  # Empty labels
    )
    # Send a NOTIFY with the message ID
    await conn.execute(f"NOTIFY {asyncpg_broker.channel_name}, '{message_id}'")
    await conn.close()

    # Listen for the message
    message = await asyncio.wait_for(get_first_task(asyncpg_broker), timeout=1.0)
    assert message.data == b"wrong"  # noqa: PLR2004

    # Acknowledge the message
    await maybe_awaitable(message.ack())


@pytest.mark.anyio
async def test_delayed_message(asyncpg_broker: AsyncpgBroker) -> None:
    """Test that delayed messages are delivered correctly after the specified delay."""
    # Send a message with a delay
    task_id = uuid.uuid4().hex
    task_name = "test_task"
    sent = BrokerMessage(
        task_id=task_id,
        task_name=task_name,
        message=b"delayed_message",
        labels={
            "delay": "2",  # Delay in seconds
        },
    )
    await asyncpg_broker.kick(sent)

    # Try to get the message immediately (should not be available yet)
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(get_first_task(asyncpg_broker), timeout=1.0)

    # Wait for the delay to pass and receive the message
    message = await asyncio.wait_for(get_first_task(asyncpg_broker), timeout=3.0)
    assert message.data == sent.message

    # Acknowledge the message
    await maybe_awaitable(message.ack())
