CREATE_TABLE_QUERY = """
CREATE TABLE IF NOT EXISTS {} (
    task_id {} UNIQUE,
    result BYTEA
)
"""

CREATE_INDEX_QUERY = """
CREATE INDEX IF NOT EXISTS {}_task_id_idx ON {} USING HASH (task_id)
"""

INSERT_RESULT_QUERY = """
INSERT INTO {} VALUES ($1, $2)
ON CONFLICT (task_id)
DO UPDATE
SET result = $2
"""

IS_RESULT_EXISTS_QUERY = """
SELECT EXISTS(
    SELECT 1 FROM {} WHERE task_id = $1
)
"""

SELECT_RESULT_QUERY = """
SELECT result FROM {} WHERE task_id = $1
"""

DELETE_RESULT_QUERY = """
DELETE FROM {} WHERE task_id = $1
"""
