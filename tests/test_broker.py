import asyncio
import json
import uuid
from typing import AsyncGenerator

import asyncpg
import pytest
from taskiq import AckableMessage, BrokerMessage
from taskiq.utils import maybe_awaitable

from taskiq_asyncpg import AsyncpgBroker


# Helper function to get the first task from the broker
async def get_first_task(broker: AsyncpgBroker) -> AckableMessage:
    """
    Get the first message from the broker's listen method.

    :param broker: Instance of AsyncpgBroker.
    :return: The first AckableMessage received.
    """
    async for message in broker.listen():
        return message


# Fixture to set up and tear down the broker
@pytest.fixture
async def broker(postgresql_dsn: str) -> AsyncGenerator[AsyncpgBroker, None]:
    # Initialize the broker with test parameters
    broker = AsyncpgBroker(
        dsn=postgresql_dsn,
        channel_name="taskiq_test_channel",
        table_name="taskiq_test_messages",
    )
    await broker.startup()
    yield broker
    await broker.shutdown()


# Fixture to clean up the messages table before each test
@pytest.fixture(autouse=True)
async def clean_messages_table(broker: AsyncpgBroker):
    conn = await asyncpg.connect(dsn=broker.dsn)
    await conn.execute(f"DELETE FROM {broker.table_name}")
    await conn.close()


@pytest.mark.anyio
async def test_kick_success(broker: AsyncpgBroker) -> None:
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
    await broker.kick(sent)

    # Listen for the message
    message = await asyncio.wait_for(get_first_task(broker), timeout=1.0)

    # Check that the received message matches the sent message
    assert message.data == sent.message

    # Acknowledge the message
    await maybe_awaitable(message.ack())


@pytest.mark.anyio
async def test_startup(broker: AsyncpgBroker) -> None:
    """
    Test the startup process of the broker.

    We drop the messages table, restart the broker, and ensure
    that the table is recreated.
    """
    # Drop the messages table
    conn = await asyncpg.connect(broker.dsn)
    await conn.execute(f"DROP TABLE IF EXISTS {broker.table_name}")
    await conn.close()

    # Shutdown and restart the broker
    await broker.shutdown()
    await broker.startup()

    # Verify that the table exists
    conn = await asyncpg.connect(broker.dsn)
    table_exists = await conn.fetchval(
        """
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_schema = 'public' AND table_name = $1
        )
    """,
        broker.table_name,
    )
    await conn.close()
    assert table_exists


@pytest.mark.anyio
async def test_listen(broker: AsyncpgBroker) -> None:
    """
    Test that the broker can listen to messages inserted directly
    into the database and notified via the channel.
    """
    # Insert a message directly into the database
    conn = await asyncpg.connect(dsn=broker.dsn)
    message_content = b"test_message"
    task_id = uuid.uuid4().hex
    task_name = "test_task"
    labels = {"label1": "label_val"}
    message_id = await conn.fetchval(
        f"""
        INSERT INTO {broker.table_name} (task_id, task_name, message, labels)
        VALUES ($1, $2, $3, $4)
        RETURNING id
        """,
        task_id,
        task_name,
        message_content.decode(),
        json.dumps(labels),
    )
    # Send a NOTIFY with the message ID
    await conn.execute(f"NOTIFY {broker.channel_name}, '{message_id}'")
    await conn.close()

    # Listen for the message
    message = await asyncio.wait_for(get_first_task(broker), timeout=1.0)
    assert message.data == message_content

    # Acknowledge the message
    await maybe_awaitable(message.ack())


@pytest.mark.anyio
async def test_wrong_format(broker: AsyncpgBroker) -> None:
    """Test that messages with incorrect formats are still received."""
    # Insert a message with missing task_id and task_name
    conn = await asyncpg.connect(dsn=broker.dsn)
    message_id = await conn.fetchval(
        f"""
        INSERT INTO {broker.table_name} (task_id, task_name, message, labels)
        VALUES ($1, $2, $3, $4)
        RETURNING id
        """,
        "",  # Missing task_id
        "",  # Missing task_name
        "wrong",  # Message content
        json.dumps({}),  # Empty labels
    )
    # Send a NOTIFY with the message ID
    await conn.execute(f"NOTIFY {broker.channel_name}, '{message_id}'")
    await conn.close()

    # Listen for the message
    message = await asyncio.wait_for(get_first_task(broker), timeout=1.0)
    assert message.data == b"wrong"

    # Acknowledge the message
    await maybe_awaitable(message.ack())


@pytest.mark.anyio
async def test_delayed_message(broker: AsyncpgBroker) -> None:
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
    await broker.kick(sent)

    # Try to get the message immediately (should not be available yet)
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(get_first_task(broker), timeout=1.0)

    # Wait for the delay to pass and receive the message
    message = await asyncio.wait_for(get_first_task(broker), timeout=3.0)
    assert message.data == sent.message

    # Acknowledge the message
    await maybe_awaitable(message.ack())
