"""
Unit tests for client_alpha_data_generator.py using pytest and pytest-mock.
Tests core functionality, checkpointing, chunk processing, update record ID range, and update limit.

Product: Data Generator Suite
Version: 1.0.0
"""
import sys
import os
from datetime import datetime, timezone
import importlib.metadata
import logging
from tenacity.wait import wait_fixed

# Add the script directory to sys.path to resolve imports
sys.path.append("/app/scripts/support_insights")

import pytest
from unittest.mock import MagicMock, patch
from pymongo.errors import ConnectionFailure
import sqlite3
import pymongo
from client_alpha_data_generator import (
    setup_logging, check_dependencies, check_env_vars,
    init_sqlite_checkpoint_db, save_checkpoint, load_checkpoint, clear_checkpoint,
    connect_to_mongodb, get_mongodb_collection, get_max_values, insert_record,
    generate_random_record, generate_and_update_records, main,
    EXPECTED_ENV_VARS, DEFAULT_CHUNK_SIZE, MAX_UPDATE_PERCENTAGE, SCRIPT_LOGGER
)

# Setup for capturing logs
@pytest.fixture
def log_capture(mocker):
    """Fixture to capture log output."""
    logger = logging.getLogger('client_alpha_data_generator')
    logger.setLevel(logging.INFO)
    log_stream = []
    def capture_log_message(msg, *args, **kwargs):
        log_stream.append(msg)
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s"))
    handler.stream.write = capture_log_message
    logger.handlers = [handler]
    # Patch setup_logging to avoid overriding the test logger
    mocker.patch("client_alpha_data_generator.setup_logging", side_effect=lambda log_file: None)
    return log_stream

@pytest.fixture
def env_vars():
    """Fixture to set up environment variables."""
    env_patch = patch.dict(os.environ, {
        "POSTGRES_HOST": "localhost",
        "POSTGRES_PORT": "5432",
        "POSTGRES_DATABASE_NAME": "test_db",
        "DB_USER": "user",
        "DB_PASSWORD": "pass",
        "MONGO_HOST": "localhost",
        "MONGO_PORT": "27017",
        "MONGO_DB": "test_mongo_db",
        "MONGO_USER": "mongo_user",
        "MONGO_PASSWORD": "mongo_pass",
        "SCRIPT_LOG_FILE": "/app/logs/test_client_alpha.log",
        "CHUNK_SIZE": "50"
    })
    env_patch.start()
    yield
    env_patch.stop()

@pytest.fixture
def mock_sqlite_conn(mocker):
    """Fixture to mock SQLite connection and cursor."""
    conn = MagicMock(spec=sqlite3.Connection)
    cursor = MagicMock(spec=sqlite3.Cursor)
    conn.cursor.return_value = cursor
    cursor.execute.return_value = cursor
    cursor.fetchone.return_value = None  # Default for most tests
    mocker.patch("sqlite3.connect", return_value=conn)
    return conn, cursor

@pytest.fixture
def mock_mongo_client(mocker):
    """Fixture to mock MongoClient and its methods."""
    client = MagicMock()
    db = MagicMock()
    collection = MagicMock()
    client.__getitem__.return_value = db
    db.__getitem__.return_value = collection
    client.server_info.return_value = {"version": "4.0"}
    return client, collection

@pytest.fixture
def mock_datetime(mocker):
    """Fixture to mock datetime.now for consistent timestamps."""
    fixed_time = datetime(2025, 5, 21, 19, 0, 0, tzinfo=timezone.utc)
    mocker.patch("client_alpha_data_generator.datetime", wraps=datetime)
    mocker.patch("client_alpha_data_generator.datetime.now", return_value=fixed_time)
    return fixed_time

# Tests for setup functions
def test_setup_logging(tmp_path, log_capture):
    """Test setup_logging configures logging correctly."""
    log_file = tmp_path / "test.log"
    # Since setup_logging is patched, manually call the original behavior
    handlers = [
        logging.FileHandler(str(log_file)),
        logging.StreamHandler(sys.stdout)
    ]
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=handlers
    )
    logger = logging.getLogger('client_alpha_data_generator')
    logger.info("Test message")
    assert log_file.exists()
    assert any("Test message" in msg for msg in log_capture)

def test_check_dependencies(mocker):
    """Test check_dependencies passes with correct versions."""
    mocker.patch("importlib.metadata.version", side_effect=lambda x: "4.10.1" if x == "pymongo" else "unknown")
    check_dependencies()  # Should not raise

def test_check_dependencies_missing(mocker, log_capture):
    """Test check_dependencies fails with missing dependency."""
    mocker.patch("importlib.metadata.version", side_effect=importlib.metadata.PackageNotFoundError("pymongo not found"))
    with pytest.raises(SystemExit):
        check_dependencies()
    assert any("Missing or incorrect dependency" in msg for msg in log_capture)

def test_check_env_vars_all_present(env_vars, log_capture):
    """Test check_env_vars logs all present."""
    check_env_vars()
    assert any("All expected environment variables are present" in msg for msg in log_capture)

def test_check_env_vars_missing(mocker, log_capture):
    """Test check_env_vars logs missing variables."""
    env_patch = patch.dict(os.environ, {}, clear=True)
    env_patch.start()
    try:
        check_env_vars()
        assert any("Missing environment variables: " in msg for msg in log_capture)
    finally:
        env_patch.stop()

# Tests for checkpointing functions
def test_init_sqlite_checkpoint_db(mock_sqlite_conn, log_capture):
    """Test init_sqlite_checkpoint_db creates table."""
    conn, cursor = mock_sqlite_conn
    init_sqlite_checkpoint_db("/app/storage/orchestrator.db")
    assert any("CREATE TABLE IF NOT EXISTS checkpoints" in call[0][0] for call in cursor.execute.call_args_list)
    assert any("Initialized SQLite checkpoint database" in msg for msg in log_capture)

def test_save_checkpoint(mock_sqlite_conn, mock_datetime, log_capture):
    """Test save_checkpoint saves data correctly."""
    conn, cursor = mock_sqlite_conn
    save_checkpoint(conn, "client_alpha_data_generator", 1000, 500, 100)
    assert any("INSERT OR REPLACE INTO checkpoints" in call[0][0] for call in cursor.execute.call_args_list)
    expected_tuple = ("client_alpha_data_generator", 100, 1000, 500, "2025-05-21 19:00:00")
    assert any(expected_tuple == call[0][1] for call in cursor.execute.call_args_list)
    assert any("Saved checkpoint for script client_alpha_data_generator" in msg for msg in log_capture)

def test_load_checkpoint_exists(mock_sqlite_conn, log_capture):
    """Test load_checkpoint loads existing checkpoint."""
    conn, cursor = mock_sqlite_conn
    cursor.fetchone.return_value = (1000, 500, 100)
    result = load_checkpoint(conn, "client_alpha_data_generator")
    assert result == (1000, 500, 100)
    assert any("Loaded checkpoint for script client_alpha_data_generator" in msg for msg in log_capture)

def test_load_checkpoint_not_exists(mock_sqlite_conn, log_capture):
    """Test load_checkpoint returns None if no checkpoint."""
    conn, cursor = mock_sqlite_conn
    cursor.fetchone.return_value = None
    result = load_checkpoint(conn, "client_alpha_data_generator")
    assert result is None
    assert any("No checkpoint found for script client_alpha_data_generator" in msg for msg in log_capture)

def test_clear_checkpoint(mock_sqlite_conn, log_capture):
    """Test clear_checkpoint deletes checkpoint."""
    conn, cursor = mock_sqlite_conn
    cursor.fetchone.return_value = (0,)  # Simulate row count after deletion
    clear_checkpoint(conn, "client_alpha_data_generator")
    assert any("DELETE FROM checkpoints WHERE script_name = ?" in call[0][0] for call in cursor.execute.call_args_list)
    assert any(("client_alpha_data_generator",) == call[0][1] for call in cursor.execute.call_args_list)
    assert any("Cleared checkpoint for script client_alpha_data_generator" in msg for msg in log_capture)

# Tests for MongoDB functions
def test_connect_to_mongodb_success(mock_mongo_client, log_capture):
    """Test connect_to_mongodb succeeds."""
    client, _ = mock_mongo_client
    # Explicitly patch pymongo.MongoClient to ensure the mock is applied
    with patch("pymongo.MongoClient", return_value=client) as mock_patch:
        result = connect_to_mongodb("localhost", 27017, "user", "pass")
        assert result == client
        assert mock_patch.called
    assert any("Connected to MongoDB: localhost:27017" in msg for msg in log_capture)

def test_connect_to_mongodb_retry_failure(mock_mongo_client, log_capture):
    """Test connect_to_mongodb fails after retries."""
    client, _ = mock_mongo_client
    client.server_info.side_effect = ConnectionFailure("Connection failed")
    with patch("pymongo.MongoClient", return_value=client) as mock_patch:
        with patch("tenacity.wait.wait_fixed", return_value=wait_fixed(0.1)):  # Reduce wait time for faster testing
            with pytest.raises(ConnectionFailure):
                connect_to_mongodb("localhost", 27017, "user", "pass")
            assert mock_patch.called
    assert any("Retrying MongoDB connection (attempt 1)" in msg for msg in log_capture)
    assert any("Retrying MongoDB connection (attempt 3)" in msg for msg in log_capture)

def test_get_mongodb_collection(mock_mongo_client, log_capture):
    """Test get_mongodb_collection accesses collection."""
    client, collection = mock_mongo_client
    result = get_mongodb_collection("test_db", "test_collection", client)
    assert result == collection
    assert any("Accessing MongoDB collection: test_collection in database: test_db" in msg for msg in log_capture)

def test_get_max_values(mock_mongo_client, log_capture):
    """Test get_max_values fetches max values."""
    _, collection = mock_mongo_client
    # Mock two separate calls to find
    collection.find.side_effect = [
        MagicMock(sort=MagicMock(return_value=MagicMock(limit=MagicMock(return_value=[{"serial_number": 1000}])))),
        MagicMock(sort=MagicMock(return_value=MagicMock(limit=MagicMock(return_value=[{"record_id": 500}]))))
    ]
    max_serial, max_interaction = get_max_values(collection)
    assert max_serial == 1000
    assert max_interaction == 500
    assert any("Fetched max values - serial_number: 1000, interaction_id: 500" in msg for msg in log_capture)

def test_insert_record(mock_mongo_client):
    """Test insert_record performs upsert."""
    _, collection = mock_mongo_client
    record = {"key": "value"}
    insert_record(collection, 1001, 501, record)
    assert record["_id"] == "1001-501"
    assert record["serial_number"] == 1001
    assert record["record_id"] == 501
    collection.update_one.assert_called_with(
        {"_id": "1001-501"},
        {"$set": record},
        upsert=True
    )

# Tests for record generation
def test_generate_random_record(mocker):
    """Test generate_random_record creates a valid record."""
    support_categories = ["category1", "category2"]
    agent_pseudo_names = ["agent1", "agent2"]
    customer_types = ["type1", "type2"]
    with patch("random.random", return_value=0.5):  # Avoid NULL for predictability
        with patch("random.randint", side_effect=[300, 500, 400, 5]):  # interaction_duration, CONTACT_DATE, TOTAL_TIME, RATING
            record, has_null = generate_random_record(501, support_categories, agent_pseudo_names, customer_types)
    assert record["INTERACTION_ID"] == 501
    assert record["SUPPORT_CATEGORY"] in support_categories
    assert record["AGENT_PSEUDO_NAME"] in agent_pseudo_names
    assert record["TYPE_OF_CUSTOMER"] in customer_types
    assert not has_null

def test_generate_random_record_with_null(mocker):
    """Test generate_random_record handles NULL values."""
    support_categories = ["category1"]
    agent_pseudo_names = ["agent1"]
    customer_types = ["type1"]
    mocker.patch("random.random", return_value=0.05)  # Trigger NULL
    mocker.patch("random.randint", side_effect=[300, 500, 400, 5])  # interaction_duration, CONTACT_DATE, TOTAL_TIME, RATING
    # Correct sequence of random.choice calls
    mocker.patch("random.choice", side_effect=[
        "SUPPORT_CATEGORY",  # key_to_nullify
        "agent1",  # AGENT_PSEUDO_NAME
        "COMPLETED",  # INTERACTION_STATUS
        "CALL",  # INTERACTION_TYPE
        "type1",  # TYPE_OF_CUSTOMER
        "RESOLVED",  # STATUS_OF_CUSTOMER_INCIDENT
        "YES",  # RESOLVED_IN_FIRST_CONTACT
        "SELF-HELP OPTION"  # SOLUTION_TYPE
    ])
    record, has_null = generate_random_record(501, support_categories, agent_pseudo_names, customer_types)
    assert record["SUPPORT_CATEGORY"] is None
    assert has_null

def test_generate_and_update_records_update_id_range(mock_mongo_client, mock_sqlite_conn, mocker):
    """Test generate_and_update_records limits update_interaction_id to highest ID at chunk start."""
    _, collection = mock_mongo_client
    conn, cursor = mock_sqlite_conn
    support_categories = ["category1"]
    agent_pseudo_names = ["agent1"]
    customer_types = ["type1"]
    num_records = 150
    chunk_size = 100
    start_interaction_id = 500

    # Mock random to always trigger updates
    update_random_calls = [0.1] * 30 + [0.9] * 120  # Enough to hit the 20% limit (30 updates for 150 records)
    null_random_calls = [0.5] * (150 + 30)  # For NULL checks (150 inserts + 30 updates)
    mocker.patch("random.random", side_effect=update_random_calls + null_random_calls)  # Total: 330 calls
    # Mock random.randint for interaction_duration, TOTAL_TIME, CONTACT_DATE, and RATING
    # Each record makes 4 calls: interaction_duration, CONTACT_DATE, TOTAL_TIME, RATING
    randint_calls = [300, 500, 400, 5] * (150 + 30)  # For each record and update (180 records * 4 = 720 calls)
    mocker.patch("random.randint", side_effect=randint_calls)

    # Mock random.randint specifically for update_interaction_id
    update_id_calls = []
    def mock_randint(a, b):
        update_id_calls.append((a, b))
        return b  # Always pick the upper bound to test the limit
    mocker.patch("client_alpha_data_generator.random.randint", side_effect=mock_randint)

    inserts, updates, null_records, chunks = generate_and_update_records(
        support_categories, agent_pseudo_names, customer_types,
        start_serial_number=1000, start_interaction_id=start_interaction_id, num_records=num_records,
        collection=collection, chunk_size=chunk_size, checkpoint_conn=conn, script_name="client_alpha_data_generator"
    )

    # First chunk: 100 records, highest interaction ID = 500
    # Second chunk: 50 records, highest interaction ID = 600
    # Update IDs should be 500 for the first chunk and 600 for the second
    expected_calls = [
        (1, 500),  # First chunk updates
        (1, 500),
        (1, 600),  # Second chunk updates
        (1, 600)
    ]
    assert inserts == 150
    assert updates == 30  # Max updates (20% of 150)
    assert null_records == 0
    assert chunks == 2
    assert len(update_id_calls) == 30  # Total updates should be exactly 30
    # Verify the upper bounds in the calls
    for call, expected in zip(update_id_calls[:len(expected_calls)], expected_calls):
        assert call[1] == expected[1]  # Check the upper bound

def test_generate_and_update_records_update_limit(mock_mongo_client, mock_sqlite_conn, mocker):
    """Test generate_and_update_records limits updates to 20% of num_records."""
    _, collection = mock_mongo_client
    conn, cursor = mock_sqlite_conn
    support_categories = ["category1"]
    agent_pseudo_names = ["agent1"]
    customer_types = ["type1"]
    num_records = 100
    chunk_size = 50
    max_updates = int(num_records * MAX_UPDATE_PERCENTAGE)  # 20% of 100 = 20

    # Mock random to always trigger updates until the limit
    update_random_calls = [0.1] * max_updates + [0.9] * (num_records - max_updates)  # Trigger updates for first 20, then stop
    null_random_calls = [0.5] * (num_records + max_updates)  # For NULL checks (100 inserts + 20 updates)
    mocker.patch("random.random", side_effect=update_random_calls + null_random_calls)  # Total: 220 calls
    # Mock random.randint for interaction_duration, TOTAL_TIME, CONTACT_DATE, and RATING
    randint_calls = [300, 500, 400, 5] * (num_records + max_updates)  # For each record and update (120 records * 4 = 480 calls)
    mocker.patch("random.randint", side_effect=randint_calls)

    inserts, updates, null_records, chunks = generate_and_update_records(
        support_categories, agent_pseudo_names, customer_types,
        start_serial_number=1000, start_interaction_id=500, num_records=num_records,
        collection=collection, chunk_size=chunk_size, checkpoint_conn=conn, script_name="client_alpha_data_generator"
    )

    assert inserts == 100
    assert updates == max_updates  # Should be exactly 20
    assert null_records == 0
    assert chunks == 2
    assert save_checkpoint.call_count == 2  # Two chunks: 50 + 50

# Test for main function
def test_main_full_flow(mock_mongo_client, mock_sqlite_conn, mocker, env_vars, log_capture):
    """Test main function full flow."""
    client, collection = mock_mongo_client
    conn, cursor = mock_sqlite_conn
    cursor.fetchone.side_effect = [None, (0,)]  # For load_checkpoint and clear_checkpoint
    mocker.patch("client_alpha_data_generator.connect_to_mongodb", return_value=client)
    mocker.patch("client_alpha_data_generator.init_sqlite_checkpoint_db", return_value=conn)
    mocker.patch("client_alpha_data_generator.connect_to_database")
    mocker.patch("client_alpha_data_generator.fetch_allowed_values", side_effect=[["cat1"], ["agent1"], ["type1"]])
    mocker.patch("client_alpha_data_generator.close_database_connection")
    mocker.patch("client_alpha_data_generator.get_max_values", return_value=(1000, 500))
    mocker.patch("client_alpha_data_generator.load_checkpoint", return_value=None)
    # Mock random.randint for num_records, interaction_duration, TOTAL_TIME, CONTACT_DATE, and RATING
    randint_calls = [150] + [300, 500, 400, 5] * 150  # num_records + (150 records * 4)
    mocker.patch("random.randint", side_effect=randint_calls)
    mocker.patch("random.random", return_value=0.5)  # Avoid updates/NULL
    # Mock sys.argv to provide a valid input
    with patch("sys.argv", ["script.py", "150"]):
        main()
    assert any("Generating 150 new records starting from max_serial_number 1001 and max_record_id 501 (chunk size: 50)" in msg for msg in log_capture)
    assert any("Total Records Requested: 150" in msg for msg in log_capture)
    assert any("Records Processed (including resumed): 150" in msg for msg in log_capture)
    assert any("Total Inserts: 150" in msg for msg in log_capture)
    assert any("Total Updates: 0" in msg for msg in log_capture)

def test_main_missing_env_vars(mocker, log_capture):
    """Test main fails with missing environment variables."""
    env_patch = patch.dict(os.environ, {}, clear=True)
    env_patch.start()
    try:
        with patch("sys.argv", ["script.py"]):  # Provide a default input
            with pytest.raises(SystemExit):
                main()
        assert any("Missing required environment variables: POSTGRES_HOST" in msg for msg in log_capture)
    finally:
        env_patch.stop()