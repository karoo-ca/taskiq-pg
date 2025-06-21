import asyncio
import json
import uuid
from typing import Optional

import asyncpg
import pytest
from taskiq import AckableMessage, BrokerMessage
from taskiq.utils import maybe_awaitable

from taskiq_pg import AsyncpgBroker


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
    # For test, insert directly with NOW() for scheduled_at
    result = await conn.fetchrow(
        f"""
        INSERT INTO {asyncpg_broker.table_name}
        (task_id, task_name, message, labels, group_key, expire_at, scheduled_at)
        VALUES ($1, $2, $3, $4, $5, $6, NOW())
        RETURNING id, lock_key
        """,
        task_id,
        task_name,
        message_content.decode(),
        json.dumps(labels),
        None,  # group_key
        None,  # expire_at
    )
    assert result is not None
    message_id = result["id"]
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
    # For test, insert directly with NOW() for scheduled_at
    result = await conn.fetchrow(
        f"""
        INSERT INTO {asyncpg_broker.table_name}
        (task_id, task_name, message, labels, group_key, expire_at, scheduled_at)
        VALUES ($1, $2, $3, $4, $5, $6, NOW())
        RETURNING id, lock_key
        """,
        "",  # Missing task_id
        "",  # Missing task_name
        "wrong",  # Message content
        json.dumps({}),  # Empty labels
        None,  # group_key
        None,  # expire_at
    )
    assert result is not None
    message_id = result["id"]
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

    # The message will be inserted immediately but notification will be delayed
    # So we should be able to see it's queued but not get notified
    start_time = asyncio.get_event_loop().time()

    # Wait for the notification (should take ~2 seconds)
    message = await asyncio.wait_for(get_first_task(asyncpg_broker), timeout=3.0)
    elapsed = asyncio.get_event_loop().time() - start_time

    # Check that it took at least 1.5 seconds (allowing some margin)
    assert elapsed >= 1.5, f"Message arrived too quickly: {elapsed}s"
    assert message.data == sent.message

    # Acknowledge the message
    await maybe_awaitable(message.ack())


@pytest.mark.anyio
async def test_group_key_coordination(asyncpg_broker: AsyncpgBroker) -> None:
    """Test that messages with the same group_key are not processed concurrently."""
    # Send two messages with the same group_key
    group_key = "test_group_123"

    sent1 = BrokerMessage(
        task_id=uuid.uuid4().hex,
        task_name="test_task_1",
        message=b"message_1",
        labels={"group_key": group_key},
    )

    sent2 = BrokerMessage(
        task_id=uuid.uuid4().hex,
        task_name="test_task_2",
        message=b"message_2",
        labels={"group_key": group_key},
    )

    await asyncpg_broker.kick(sent1)
    await asyncpg_broker.kick(sent2)

    # Create two listeners to simulate concurrent workers
    async def get_message_with_timeout(timeout: float) -> Optional[AckableMessage]:
        try:
            return await asyncio.wait_for(
                get_first_task(asyncpg_broker), timeout=timeout
            )
        except asyncio.TimeoutError:
            return None

    # Start two concurrent tasks to get messages
    task1 = asyncio.create_task(get_message_with_timeout(1.0))
    await asyncio.sleep(0.1)  # Small delay to ensure first task starts
    task2 = asyncio.create_task(get_message_with_timeout(0.5))

    # Wait for both tasks
    msg1, msg2 = await asyncio.gather(task1, task2)

    # One should get a message, the other should timeout
    assert msg1 is not None
    assert msg2 is None

    # Acknowledge the first message
    await maybe_awaitable(msg1.ack())

    # Now the second message should be available
    message2 = await asyncio.wait_for(get_first_task(asyncpg_broker), timeout=1.0)
    await maybe_awaitable(message2.ack())


@pytest.mark.anyio
async def test_message_ttl(asyncpg_broker: AsyncpgBroker) -> None:
    """Test that messages respect TTL settings."""
    # Send a message with a short TTL
    sent = BrokerMessage(
        task_id=uuid.uuid4().hex,
        task_name="test_task",
        message=b"ttl_message",
        labels={"ttl": 2},  # 2 seconds TTL
    )

    await asyncpg_broker.kick(sent)

    # Receive and acknowledge the message
    message = await asyncio.wait_for(get_first_task(asyncpg_broker), timeout=1.0)
    await maybe_awaitable(message.ack())

    # Check that the message is marked as completed with expire_at set
    conn = await asyncpg.connect(asyncpg_broker.dsn)
    row = await conn.fetchrow(
        f"SELECT status, expire_at FROM {asyncpg_broker.table_name} WHERE task_id = $1",  # noqa: S608
        sent.task_id,
    )
    await conn.close()

    assert row is not None
    assert row["status"] == "completed"
    assert row["expire_at"] is not None
