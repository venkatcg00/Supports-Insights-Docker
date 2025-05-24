"""
Database Operations Utility for fetching lookup lists and updating script checkpoints from SQLite database.

This module provides functions to connect to a SQLite database, fetch distinct values from a specified column based on a source name, 
manage checkpoints for idempotent record generation, fetch last execution info, and close the database connection.
"""
import os
import sys
import sqlite3
from datetime import datetime, timezone
from typing import List, Tuple, Optional
from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_exception_type

def check_env_vars() -> None:
    """
    Checks for required environment variables and exits if any are missing or empty.

    Raises:
        SystemExit: If any required environment variable is missing or empty.
    """
    required_vars = ["SQLITE_DB_FILE"]
    for var in required_vars:
        value = os.environ.get(var)
        if value is None or value.strip() == "":
            print(f"Missing or empty environment variable: {var}")
            sys.exit(1)

@retry(
    stop=stop_after_attempt(3),
    wait=wait_fixed(5),
    retry=retry_if_exception_type(sqlite3.OperationalError)
)
def connect_to_database() -> sqlite3.Connection:
    """
    Establishes a connection to the SQLite database specified by SQLITE_DB_FILE.

    Returns:
        SQLite Connection instance for database operations.
    """
    check_env_vars()
    db_file = os.environ["SQLITE_DB_FILE"]
    conn = sqlite3.connect(db_file, timeout=10)
    return conn

def fetch_lookup_dictionary(
    conn: sqlite3.Connection,
    table_name: str,
    source_name: str,
    column_name: str
) -> List[str]:
    """
    Fetches distinct allowed values from a specified column in a SQLite table.

    Retrieves unique values from the specified column where the source matches
    the provided `source_name` and `is_active` is 1, using a parameterized query.

    Args:
        conn: SQLite Connection instance for database operations.
        table_name: The name of the table to query (e.g., "support_areas").
        source_name: The source name to filter by (e.g., "client_alpha").
        column_name: The name of the column to fetch distinct values from (e.g., "support_area_name").

    Returns:
        A list of distinct values from the specified column as strings.

    Raises:
        sqlite3.Error: If the query execution fails.
    """
    try:
        cursor = conn.cursor()
        query = f"""
            SELECT DISTINCT {column_name}
            FROM {table_name}
            WHERE source_id = (SELECT source_id FROM sources 
                               WHERE source_name = ? AND is_active = 1)
            AND is_active = 1
        """
        source_name = source_name.strip("'")  # Remove quotes if present
        cursor.execute(query, (source_name,))
        values = [str(row[0]) for row in cursor.fetchall()]
        return values
    except sqlite3.Error as e:
        raise sqlite3.Error(f"Failed to fetch lookup values from {table_name}.{column_name}: {e}")
    finally:
        cursor.close()

@retry(
    stop=stop_after_attempt(3),
    wait=wait_fixed(5),
    retry=retry_if_exception_type(sqlite3.OperationalError)
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
    null_records: int = 0
) -> None:
    """
    Manages inserts and updates to the checkpoints table for idempotent record generation.

    Inserts a new checkpoint if the script_name doesn't exist; otherwise, updates the existing record.

    Args:
        conn: SQLite Connection instance for database operations.
        script_name: Name of the script (e.g., "client_alpha_data_generator").
        script_status: Status of the script (e.g., "RUNNING", "COMPLETED", "FAILURE").
        max_serial_number: Maximum serial number processed.
        max_record_id: Maximum record ID processed.
        processing_count: Number of records processed.
        new_records: Number of new records created (default: 0).
        update_records: Number of records updated (default: 0).
        null_records: Number of null records encountered (default: 0).

    Raises:
        sqlite3.Error: If the query execution fails.
    """
    try:
        cursor = conn.cursor()
        select_query = "SELECT 1 FROM checkpoints WHERE script_name = ?"
        cursor.execute(select_query, (script_name,))
        exists = cursor.fetchone() is not None

        timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        if exists:
            update_query = """
                UPDATE checkpoints
                SET script_status = ?,
                    processing_count = ?,
                    new_records = ?,
                    update_records = ?,
                    null_records = ?,
                    max_serial_number = ?,
                    max_record_id = ?,
                    change_timestamp = ?
                WHERE script_name = ?
            """
            cursor.execute(update_query, (
                script_status, processing_count, new_records, update_records,
                null_records, max_serial_number, max_record_id, timestamp, script_name
            ))
        else:
            insert_query = """
                INSERT INTO checkpoints (
                    script_name, script_status, processing_count, new_records,
                    update_records, null_records, max_serial_number, max_record_id,
                    change_timestamp
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
            cursor.execute(insert_query, (
                script_name, script_status, processing_count, new_records,
                update_records, null_records, max_serial_number, max_record_id, timestamp
            ))

        conn.commit()
    except sqlite3.Error as e:
        raise sqlite3.Error(f"Failed to manage checkpoint for {script_name}: {e}")
    finally:
        cursor.close()

@retry(
    stop=stop_after_attempt(3),
    wait=wait_fixed(5),
    retry=retry_if_exception_type(sqlite3.OperationalError)
)
def get_last_execution_info(
    conn: sqlite3.Connection,
    script_name: str
) -> Optional[Tuple[str, int, int, int, int, int, int]]:
    """
    Fetches information about the last execution of the specified script.

    Args:
        conn: SQLite Connection instance.
        script_name: The script name whose status is to be fetched.

    Returns:
        Tuple of (script_status, processing_count, new_records, update_records, null_records,
        max_serial_number, max_record_id), or None if no checkpoint exists.

    Raises:
        sqlite3.Error: If query execution fails.
    """
    try:
        cursor = conn.cursor()
        query = """
            SELECT script_status, processing_count, new_records, update_records, 
                   null_records, max_serial_number, max_record_id
            FROM checkpoints
            WHERE script_name = ?
        """
        cursor.execute(query, (script_name,))
        row = cursor.fetchone()
        return row if row else None
    except sqlite3.Error as e:
        raise sqlite3.Error(f"Failed to fetch last execution info for {script_name}: {e}")
    finally:
        cursor.close()

def close_database_connection(conn: sqlite3.Connection) -> None:
    """
    Closes the connection to the SQLite database.

    Args:
        conn: SQLite Connection instance to close.
    """
    conn.close()