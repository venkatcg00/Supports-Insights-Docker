#!/usr/bin/env python3
"""
SQLite Database Operations Utility.

Functions:
- Connect to SQLite database from env var.
- Fetch distinct lookup values for a source.
- Manage checkpoints for idempotent generation.
- Fetch last execution info.
- Close DB connection.

Designed for use in generator/orchestration scripts.
"""

import os
import sys
import sqlite3
from datetime import datetime, timezone
from typing import List, Tuple, Optional
from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_exception_type


# --------------------------- ENV CHECK --------------------------- #
def _check_env_vars() -> None:
    """Exit if required env vars are missing/empty."""
    required_vars = ["SQLITE_DB_FILE"]
    for var in required_vars:
        if not os.environ.get(var, "").strip():
            sys.stderr.write(f"[ERROR] Missing or empty env var: {var}\n")
            sys.exit(1)


# --------------------------- CONNECTION --------------------------- #
@retry(
    stop=stop_after_attempt(3),
    wait=wait_fixed(5),
    retry=retry_if_exception_type(sqlite3.OperationalError),
)
def connect_to_database() -> sqlite3.Connection:
    """Return a SQLite connection."""
    _check_env_vars()
    return sqlite3.connect(os.environ["SQLITE_DB_FILE"], timeout=10)


def close_database_connection(conn: sqlite3.Connection) -> None:
    """Close the SQLite connection."""
    conn.close()


# --------------------------- LOOKUP FETCH --------------------------- #
def fetch_lookup_list(conn: sqlite3.Connection, table_name: str,
                      source_name: str, column_name: str) -> List[str]:
    """
    Fetch distinct values from a column where source matches and is_active=1.
    """
    sql = f"""
        SELECT DISTINCT {column_name}
        FROM {table_name}
        WHERE source_id = (
            SELECT source_id FROM sources
            WHERE source_name = ? AND is_active = 1
        )
        AND is_active = 1
    """
    try:
        with conn:
            cur = conn.execute(sql, (source_name.strip("'"), ))
            return [str(v[0]) for v in cur.fetchall()]
    except sqlite3.Error as e:
        raise sqlite3.Error(
            f"[DB ERROR] fetch_lookup_list({table_name}.{column_name}): {e}")


# --------------------------- CHECKPOINT MGMT --------------------------- #
@retry(
    stop=stop_after_attempt(3),
    wait=wait_fixed(5),
    retry=retry_if_exception_type(sqlite3.OperationalError),
)
def checkpoint_management(
    conn: sqlite3.Connection,
    script_name: str,
    script_status: str,
    max_serial_number: int,
    max_record_id: int,
    processing_count: int,
    new_records: int = 0,
    update_records: int = 0,
    null_records: int = 0,
) -> None:
    """
    Insert or update checkpoint row for a script.
    """
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    try:
        with conn:
            exists = (conn.execute(
                "SELECT 1 FROM checkpoints WHERE script_name = ?",
                (script_name, )).fetchone() is not None)

            if exists:
                conn.execute(
                    """
                    UPDATE checkpoints
                    SET script_status=?, processing_count=?, new_records=?,
                        update_records=?, null_records=?, max_serial_number=?,
                        max_record_id=?, change_timestamp=?
                    WHERE script_name=?
                """,
                    (
                        script_status,
                        processing_count,
                        new_records,
                        update_records,
                        null_records,
                        max_serial_number,
                        max_record_id,
                        timestamp,
                        script_name,
                    ),
                )
            else:
                conn.execute(
                    """
                    INSERT INTO checkpoints (
                        script_name, script_status, processing_count, new_records,
                        update_records, null_records, max_serial_number, max_record_id,
                        change_timestamp
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                    (
                        script_name,
                        script_status,
                        processing_count,
                        new_records,
                        update_records,
                        null_records,
                        max_serial_number,
                        max_record_id,
                        timestamp,
                    ),
                )
    except sqlite3.Error as e:
        raise sqlite3.Error(
            f"[DB ERROR] checkpoint_management({script_name}): {e}")


# --------------------------- LAST EXEC INFO --------------------------- #
@retry(
    stop=stop_after_attempt(3),
    wait=wait_fixed(5),
    retry=retry_if_exception_type(sqlite3.OperationalError),
)
def get_last_execution_info(
        conn: sqlite3.Connection, script_name: str
) -> Optional[Tuple[str, int, int, int, int, int, int]]:
    """
    Return last execution info tuple or None.
    """
    try:
        with conn:
            return conn.execute(
                """
                SELECT script_status, processing_count, new_records,
                       update_records, null_records, max_serial_number, max_record_id
                FROM checkpoints
                WHERE script_name = ?
            """,
                (script_name, ),
            ).fetchone()
    except sqlite3.Error as e:
        raise sqlite3.Error(
            f"[DB ERROR] get_last_execution_info({script_name}): {e}")
