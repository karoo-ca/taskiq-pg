"""Queries for managing task messages."""

from string import Template

CREATE_UPDATE_TABLE_QUERY = Template(
    """
-- Create the messages table
CREATE TABLE IF NOT EXISTS ${table_name} (
    id SERIAL PRIMARY KEY,
    task_id VARCHAR NOT NULL,
    task_name VARCHAR NOT NULL,
    message TEXT NOT NULL,
    labels JSONB NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW());

-- Add additional columns needed to implement LISTEN/NOTIFY
ALTER TABLE ${table_name} ADD COLUMN IF NOT EXISTS scheduled_at TIMESTAMP WITH TIME ZONE DEFAULT NOW();
ALTER TABLE ${table_name} ADD COLUMN IF NOT EXISTS status VARCHAR(20) DEFAULT 'queued' CHECK (status IN ('queued', 'active', 'completed'));
ALTER TABLE ${table_name} ADD COLUMN IF NOT EXISTS lock_key SERIAL NOT NULL;
ALTER TABLE ${table_name} ADD COLUMN IF NOT EXISTS expire_at TIMESTAMP WITH TIME ZONE;
ALTER TABLE ${table_name} ADD COLUMN IF NOT EXISTS group_key VARCHAR;
ALTER TABLE ${table_name} ADD COLUMN IF NOT EXISTS retry_count INTEGER DEFAULT 0;

-- Necessary indexes
CREATE INDEX IF NOT EXISTS idx_${table_name_safe}_status_scheduled ON ${table_name} (status, scheduled_at) WHERE status = 'queued';
CREATE INDEX IF NOT EXISTS idx_${table_name_safe}_group_key ON ${table_name} (group_key) WHERE group_key IS NOT NULL AND status = 'active';
CREATE INDEX IF NOT EXISTS idx_${table_name_safe}_expire_at ON ${table_name} (expire_at) WHERE expire_at IS NOT NULL;
""",
)


NOTIFY_EXISTING_MESSAGES_QUERY = Template(
    """
DO $$$$
DECLARE
    payload json;
    ids integer[];
BEGIN
    -- Select all IDs that are ready to be processed.
    SELECT array_agg(id) INTO ids
    FROM ${table_name}
    WHERE status = 'queued' AND scheduled_at <= NOW();

    -- Only send a notification if there are tasks to process.
    IF array_length(ids, 1) > 0 THEN
        payload := json_build_object('ids', ids);
        PERFORM pg_notify('${channel_name}', payload::text);
    END IF;
END $$$$;
""",
)

INSERT_MESSAGE_QUERY = Template(
    """
INSERT INTO ${table_name} (task_id, task_name, message, labels, group_key, expire_at, scheduled_at)
VALUES ('${task_id}', '${task_name}', '${message}', '${labels}', '${group_key}', ${expire_at}, ${scheduled_at})
RETURNING id, lock_key
""",
)


UPDATE_MESSAGE_STATUS_QUERY = Template(
    """
UPDATE ${table_name}
SET status = '${message_status}'
WHERE id = '${message_id}'
""",
)

# Enhanced queries for dequeue with locking
DEQUEUE_MESSAGE_QUERY = Template(
    """
WITH next_message AS (
    SELECT id, lock_key
    FROM ${table_name}
    WHERE status = 'queued'
      AND scheduled_at <= NOW()
      AND (expire_at IS NULL OR expire_at > NOW())
      AND (group_key IS NULL OR group_key NOT IN (
          SELECT DISTINCT group_key
          FROM ${table_name}
          WHERE status = 'active'
            AND group_key IS NOT NULL
      ))
    ORDER BY scheduled_at, created_at
    LIMIT 1
    FOR UPDATE SKIP LOCKED
)
UPDATE ${table_name}
SET status = 'active'
FROM next_message
WHERE ${table_name}.id = next_message.id
RETURNING ${table_name}.*, next_message.lock_key
""",
)

# Query to mark message as completed
COMPLETE_MESSAGE_QUERY = Template(
    """
UPDATE ${table_name}
SET status = 'completed', expire_at = NOW() + (${message_ttl}::INTEGER * INTERVAL '1 second')
WHERE id = '${message_id}' AND status = 'active'
""",
)

# Query for sweeping stuck messages
SWEEP_MESSAGES_QUERY = Template(
    """
WITH locks AS (
    SELECT objid
    FROM pg_locks
    WHERE locktype = 'advisory'
      AND classid = $1
      AND objsubid = 2
),
stuck_messages AS (
    SELECT m.id
    FROM ${table_name} m
    LEFT JOIN locks l ON m.lock_key = l.objid
    WHERE m.status = 'active'
      AND m.created_at < NOW() - ($2::INTEGER * INTERVAL '1 second')
      AND l.objid IS NULL
    LIMIT 100
),
requeued AS (
    UPDATE ${table_name}
    SET status = 'queued', retry_count = retry_count + 1
    FROM stuck_messages
    WHERE ${table_name}.id = stuck_messages.id
    RETURNING ${table_name}.id
)
SELECT array_agg(id) as requeued_ids FROM requeued;
""",
)

# Query to clean up expired messages
CLEANUP_EXPIRED_QUERY = Template(
    """
DELETE FROM ${table_name}
WHERE id IN (
    SELECT id
    FROM ${table_name}
    WHERE expire_at IS NOT NULL
      AND expire_at < NOW()
      AND status = 'completed'
    LIMIT 1000
)
""",
)

RELEASE_ALL_ADVISORY_LOCKS_QUERY = """
SELECT pg_advisory_unlock_all()
"""

NOTIFY_QUERY = Template(
    """
SELECT pg_notify('${channel_name}', ${payload}::text);
""",
)


RELEASE_ADVISORY_LOCK_QUERY = Template(
    """
SELECT pg_advisory_unlock(${job_lock_keyspace}, ${lock_key})
""",
)

ACQUIRE_ADVISORY_LOCK_QUERY = Template(
    "SELECT pg_try_advisory_xact_lock(${job_lock_keyspace}, ${lock_key})",
)
