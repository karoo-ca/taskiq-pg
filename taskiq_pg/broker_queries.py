CREATE_TABLE_QUERY = """
CREATE TABLE IF NOT EXISTS {} (
    id SERIAL PRIMARY KEY,
    task_id VARCHAR NOT NULL,
    task_name VARCHAR NOT NULL,
    message TEXT NOT NULL,
    labels JSONB NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    scheduled_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    status VARCHAR(20) DEFAULT 'queued' CHECK (status IN ('queued', 'active', 'completed')),
    lock_key SERIAL NOT NULL,
    expire_at TIMESTAMP WITH TIME ZONE,
    group_key VARCHAR,
    retry_count INTEGER DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_{}_status_scheduled ON {} (status, scheduled_at) WHERE status = 'queued';
CREATE INDEX IF NOT EXISTS idx_{}_group_key ON {} (group_key) WHERE group_key IS NOT NULL AND status = 'active';
CREATE INDEX IF NOT EXISTS idx_{}_expire_at ON {} (expire_at) WHERE expire_at IS NOT NULL;
"""  # noqa: E501

INSERT_MESSAGE_QUERY = """
INSERT INTO {} (task_id, task_name, message, labels, group_key, expire_at, scheduled_at)
VALUES ($1, $2, $3, $4, $5, $6, $7)
RETURNING id, lock_key
"""

SELECT_MESSAGE_QUERY = "SELECT * FROM {} WHERE id = $1"

DELETE_MESSAGE_QUERY = "DELETE FROM {} WHERE id = $1"

# Enhanced queries for dequeue with locking
DEQUEUE_MESSAGE_QUERY = """
WITH next_message AS (
    SELECT id, lock_key
    FROM {}
    WHERE status = 'queued'
      AND scheduled_at <= NOW()
      AND (expire_at IS NULL OR expire_at > NOW())
      AND (group_key IS NULL OR group_key NOT IN (
          SELECT DISTINCT group_key
          FROM {}
          WHERE status = 'active'
            AND group_key IS NOT NULL
      ))
    ORDER BY scheduled_at, created_at
    LIMIT 1
    FOR UPDATE SKIP LOCKED
)
UPDATE {}
SET status = 'active'
FROM next_message
WHERE {}.id = next_message.id
RETURNING {}.*, next_message.lock_key
"""

# Query to mark message as completed
COMPLETE_MESSAGE_QUERY = """
UPDATE {}
SET status = 'completed', expire_at = NOW() + ($1::INTEGER * INTERVAL '1 second')
WHERE id = $2 AND status = 'active'
"""

# Query for sweeping stuck messages
SWEEP_MESSAGES_QUERY = """
WITH locks AS (
    SELECT objid
    FROM pg_locks
    WHERE locktype = 'advisory'
      AND classid = $1
      AND objsubid = 2
), stuck_messages AS (
    SELECT m.id, m.lock_key
    FROM {} m
    LEFT JOIN locks l ON m.lock_key = l.objid
    WHERE m.status = 'active'
      AND m.created_at < NOW() - ($2::INTEGER * INTERVAL '1 second')
      AND l.objid IS NULL
    LIMIT 100
)
UPDATE {}
SET status = 'queued', retry_count = retry_count + 1
FROM stuck_messages
WHERE {}.id = stuck_messages.id
RETURNING {}.id
"""

# Query to clean up expired messages
CLEANUP_EXPIRED_QUERY = """
DELETE FROM {}
WHERE id IN (
    SELECT id
    FROM {}
    WHERE expire_at IS NOT NULL
      AND expire_at < NOW()
      AND status = 'completed'
    LIMIT 1000
)
"""
