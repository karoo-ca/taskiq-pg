# Upgrade Notes: Enhanced PostgreSQL Features

This document describes the breaking changes and new features added to taskiq-pg inspired by SAQ's PostgreSQL implementation.

## Breaking Changes

### Database Schema Changes

The broker now uses an enhanced database schema with additional columns:

- `status`: Tracks message state (queued, active, completed)
- `scheduled_at`: Controls when messages become available for processing
- `lock_key`: Used for PostgreSQL advisory locking
- `expire_at`: Automatic cleanup timestamp
- `group_key`: For coordinating related messages
- `retry_count`: Tracks retry attempts

**Migration Required**: If you have existing messages in your database, you'll need to either:
1. Drop and recreate the messages table (losing existing messages)
2. Manually add the new columns with appropriate defaults

### API Changes

The `AsyncpgBroker` constructor now accepts additional parameters:
- `job_lock_keyspace`: Advisory lock keyspace (default: 1)
- `message_ttl`: Time to live for completed messages in seconds (default: 86400)
- `stuck_message_timeout`: Time before message is considered stuck (default: 300)
- `enable_sweeping`: Enable automatic cleanup (default: True)
- `sweep_interval`: Interval between sweep operations (default: 60)

## New Features

### 1. Advisory Locking
- Prevents duplicate message processing using PostgreSQL advisory locks
- Each message gets a unique lock that's held during processing
- Locks are automatically released on acknowledgment

### 2. Message States
- `queued`: Message waiting to be processed
- `active`: Message currently being processed
- `completed`: Message has been acknowledged

### 3. Scheduled Messages
- Messages with a `delay` label are scheduled for future processing
- The broker efficiently handles delayed messages without blocking

### 4. Group Coordination
- Messages with the same `group_key` won't be processed concurrently
- Useful for ensuring sequential processing of related tasks

### 5. Message TTL
- Completed messages are automatically cleaned up after TTL expires
- Configure per-message with the `ttl` label or globally via `message_ttl`

### 6. Automatic Sweeping
- Stuck messages (no active lock) are automatically returned to queue
- Expired messages are cleaned up periodically
- Configurable sweep interval and timeout

### 7. Connection Resilience
- Dedicated dequeue connection with health checks
- Automatic reconnection on connection failures
- Better connection pool management

## Usage Examples

### Group Coordination
```python
# These tasks won't run concurrently
await my_task.kicker().with_labels(group_key="user_123").kiq()
await another_task.kicker().with_labels(group_key="user_123").kiq()
```

### Message TTL
```python
# This message will be cleaned up after 1 hour
await my_task.kicker().with_labels(ttl=3600).kiq()
```

### Delayed Messages
```python
# This message will be processed after 5 minutes
await my_task.kicker().with_labels(delay="300").kiq()
```

## Performance Improvements

- `FOR UPDATE SKIP LOCKED` for efficient concurrent dequeuing
- Optimized indexes for message queries
- Batch operations for cleanup tasks
- Connection pooling best practices

## Monitoring

The broker logs important events:
- Swept messages returned to queue
- Connection health issues and reconnections
- Expired message cleanup

Monitor these logs to ensure your system is operating correctly.