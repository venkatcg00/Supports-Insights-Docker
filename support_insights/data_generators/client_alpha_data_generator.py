"""
This script generates random support records for client_alpha based on allowed values from a SQLite database,
fetches the maximum serial number and interaction ID from a checkpoint table, and inserts records in a specified
MongoDB collection. It supports configurable record counts via command-line arguments, checkpointing for resumability,
and chunk size processing with bulk writes for efficient batch operations. It is designed to run under Data_Generator_Orchestrator.py.

Product: Data Generator Suite
Version: 1.0.0
"""
import random
import os
import sys
import logging
import gc
import traceback
import sqlite3
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Any, Optional, Tuple
import argparse
from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.errors import ConnectionFailure, BulkWriteError
from pymongo.operations import UpdateOne
from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_exception_type
from db_operations import connect_to_database, fetch_lookup_dictionary, checkpoint_management, get_last_execution_info, close_database_connection

DEFAULT_CHUNK_SIZE = 100
MAX_UPDATE_PERCENTAGE = 0.20
MAX_NULL_PERCENTAGE = 0.10

# Configure structured logging
SCRIPT_LOGGER = logging.getLogger("client_alpha_data_generator")
SCRIPT_LOGGER.setLevel(logging.INFO)

# Use orchestrator-provided log file or fallback
script_name = "client_alpha_data_generator"
log_file = os.environ.get("SCRIPT_LOG_FILE", f"/app/logs/{script_name}_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.log")
formatter = logging.Formatter("timestamp=%(asctime)s level=%(levelname)s script=%(name)s message=%(message)s")
file_handler = logging.FileHandler(log_file)
file_handler.setFormatter(formatter)
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setFormatter(formatter)
SCRIPT_LOGGER.addHandler(file_handler)
SCRIPT_LOGGER.addHandler(console_handler)

def generate_random_record(
    interaction_id: int,
    support_categories: List[str],
    agent_pseudo_names: List[str],
    customer_types: List[str]
) -> Dict[str, Optional[Any]]:
    """
    Generate a random record with specified data fields.

    Args:
        interaction_id: Interaction ID for the record.
        support_categories: List of allowed support categories.
        agent_pseudo_names: List of allowed agent pseudo names.
        customer_types: List of allowed customer types.

    Returns:
        Generated record dictionary.
    """
    interaction_duration = random.randint(10, 600)
    record = {
        "INTERACTION_ID": interaction_id,
        "SUPPORT_CATEGORY": random.choice(support_categories),
        "AGENT_PSEUDO_NAME": random.choice(agent_pseudo_names),
        "CONTACT_DATE": (
            datetime.now(timezone.utc) - timedelta(days=random.randint(0, 1000))
        ).strftime("%d/%m/%Y %H:%M:%S"),
        "INTERACTION_STATUS": random.choice(["COMPLETED", "DROPPED", "TRANSFERRED"]),
        "INTERACTION_TYPE": random.choice(["CALL", "CHAT"]),
        "TYPE_OF_CUSTOMER": random.choice(customer_types),
        "INTERACTION_DURATION": interaction_duration,
        "TOTAL_TIME": interaction_duration + random.randint(10, 600),
        "STATUS_OF_CUSTOMER_INCIDENT": random.choice(
            [
                "RESOLVED",
                "PENDING RESOLUTION",
                "PENDING CUSTOMER UPDATE",
                "WORK IN PROGRESS",
                "TRANSFERRED TO ANOTHER QUEUE",
            ]
        ),
        "RESOLVED_IN_FIRST_CONTACT": random.choice(["YES", "NO"]),
        "SOLUTION_TYPE": random.choice(["SELF-HELP OPTION", "SUPPORT TEAM INTERVENTION"]),
        "RATING": random.randint(1, 10),
    }
    return record

def generate_and_update_records(
    support_categories: List[str],
    agent_pseudo_names: List[str],
    customer_types: List[str],
    start_serial_number: int,
    start_interaction_id: int,
    num_records: int,
    collection: Collection,
    chunk_size: int,
    sqlite_connection: sqlite3.Connection,
    script_name: str,
    start_records_processed: int = 0
) -> Tuple[int, int, int, int, int, int]:
    """
    Generate and insert records into MongoDB in chunks using bulk writes, with checkpointing.

    Args:
        support_categories: List of allowed support categories.
        agent_pseudo_names: List of allowed agent pseudo names.
        customer_types: List of allowed customer types.
        start_serial_number: Starting serial number for new records.
        start_interaction_id: Starting interaction ID for new records.
        num_records: Total number of records to generate.
        collection: MongoDB Collection object to insert into.
        chunk_size: Number of records to process per chunk.
        sqlite_connection: SQLite connection for checkpointing.
        script_name: Name of the script (e.g., 'client_alpha_data_generator').
        start_records_processed: Number of records already processed (from checkpoint).

    Returns:
        Tuple of (new_records, update_records, null_records, chunks_processed, serial_number, interaction_id).

    Raises:
        Exception: If record generation or insertion fails.
    """
    serial_number = start_serial_number
    interaction_id = start_interaction_id
    new_records = 0
    update_records = 0
    null_records = 0
    records_processed = start_records_processed
    chunks_processed = 0

    max_updates = int(num_records * MAX_UPDATE_PERCENTAGE)
    max_nulls = int(num_records * MAX_NULL_PERCENTAGE)

    try:
        records_to_process = max(0, num_records - records_processed)
        if records_to_process <= 0:
            SCRIPT_LOGGER.warning(
                "No new records to process: records_processed=%d exceeds or equals num_records=%d",
                records_processed, num_records
            )
            return new_records, update_records, null_records, chunks_processed, serial_number, interaction_id

        SCRIPT_LOGGER.info("Starting record generation: records_to_process=%d, chunk_size=%d", records_to_process, chunk_size)

        for chunk_start in range(0, records_to_process, chunk_size):
            chunk_end = min(chunk_start + chunk_size, records_to_process)
            chunk_records = chunk_end - chunk_start
            highest_interaction_id = interaction_id
            operations = []
            chunk_update_records = 0

            SCRIPT_LOGGER.info(
                "----- Processing chunk %d/%d: %d records (records %d to %d) -----",
                chunks_processed + 1, (records_to_process + chunk_size - 1) // chunk_size,
                chunk_records, records_processed + 1, records_processed + chunk_records
            )

            for _ in range(chunk_records):
                serial_number += 1
                interaction_id += 1
                new_record = generate_random_record(
                    interaction_id, support_categories, agent_pseudo_names, customer_types
                )

                if random.random() < 0.1 and null_records < max_nulls:
                    key_to_nullify = random.choice(
                        [
                            "SUPPORT_CATEGORY", "AGENT_PSEUDO_NAME", "CONTACT_DATE",
                            "INTERACTION_STATUS", "INTERACTION_TYPE", "TYPE_OF_CUSTOMER",
                            "INTERACTION_DURATION", "TOTAL_TIME", "STATUS_OF_CUSTOMER_INCIDENT",
                            "SOLUTION_TYPE", "RATING"
                        ]
                    )
                    new_record[key_to_nullify] = None
                    null_records += 1

                new_record["_id"] = f"{serial_number}-{interaction_id}"
                new_record["serial_number"] = serial_number
                new_record["record_id"] = interaction_id
                operations.append(UpdateOne(
                    {"_id": new_record["_id"]},
                    {"$set": new_record},
                    upsert=True
                ))
                new_records += 1

                if update_records < max_updates and random.random() < 0.25 and highest_interaction_id > 1:
                    serial_number += 1
                    update_interaction_id = random.randint(1, highest_interaction_id)
                    update_record = generate_random_record(
                        update_interaction_id, support_categories, agent_pseudo_names, customer_types
                    )
                    update_record["_id"] = f"{serial_number}-{update_interaction_id}"
                    update_record["serial_number"] = serial_number
                    update_record["record_id"] = update_interaction_id
                    operations.append(UpdateOne(
                        {"_id": update_record["_id"]},
                        {"$set": update_record},
                        upsert=True
                    ))
                    update_records += 1
                    chunk_update_records += 1

                records_processed += 1

            SCRIPT_LOGGER.info(
                "Chunk %d: Generated %d records, including %d pseudo-updates, %d null records",
                chunks_processed + 1, len(operations), chunk_update_records, null_records
            )

            try:
                SCRIPT_LOGGER.debug("Executing bulk write for %d operations", len(operations))
                collection.bulk_write(operations, ordered=False)
                SCRIPT_LOGGER.info("Successfully wrote %d records in chunk %d", len(operations), chunks_processed + 1)
            except BulkWriteError as e:
                SCRIPT_LOGGER.error("Bulk write failed for chunk %d: %s\n%s", chunks_processed + 1, e.details, traceback.format_exc())
                raise
            except Exception as e:
                SCRIPT_LOGGER.error("Unexpected error during bulk write for chunk %d: %s\n%s", chunks_processed + 1, str(e), traceback.format_exc())
                raise

            del operations
            operations = []
            gc.collect()
            SCRIPT_LOGGER.debug("Cleared memory for chunk %d: operations list freed", chunks_processed + 1)

            try:
                SCRIPT_LOGGER.info(
                    "Before checkpoint update: new_records=%d, update_records=%d, null_records=%d, max_serial_number=%d, max_record_id=%d",
                    new_records, update_records, null_records, serial_number, interaction_id
                )
                checkpoint_management(
                    sqlite_connection, script_name, "PROCESSING", serial_number,
                    interaction_id, num_records, new_records, update_records, null_records
                )
                SCRIPT_LOGGER.info(
                    "Checkpoint updated: new_records=%d, update_records=%d, null_records=%d, max_serial_number=%d, max_record_id=%d",
                    new_records, update_records, null_records, serial_number, interaction_id
                )
            except Exception as e:
                SCRIPT_LOGGER.error("Failed to update checkpoint for chunk %d: %s\n%s", chunks_processed + 1, str(e), traceback.format_exc())
                raise

            chunks_processed += 1

        SCRIPT_LOGGER.info("Completed record generation: total_new_records=%d, total_update_records=%d, total_null_records=%d", new_records, update_records, null_records)
        return new_records, update_records, null_records, chunks_processed, serial_number, interaction_id
    except Exception as e:
        SCRIPT_LOGGER.error("Failed to generate or insert records: %s\n%s", str(e), traceback.format_exc())
        raise

@retry(
    stop=stop_after_attempt(3),
    wait=wait_fixed(5),
    retry=retry_if_exception_type(ConnectionFailure)
)
def connect_to_mongodb(
    mongo_host: str,
    mongo_port: int,
    mongo_user: str,
    mongo_password: str,
    mongo_db: str,
    mongo_collection_name: str
) -> Collection:
    """
    Connect to MongoDB and return the specified collection.

    Args:
        mongo_host: MongoDB host address.
        mongo_port: MongoDB port.
        mongo_user: MongoDB username.
        mongo_password: MongoDB password.
        mongo_db: MongoDB database name.
        mongo_collection_name: MongoDB collection name.

    Returns:
        MongoDB Collection instance.

    Raises:
        ConnectionFailure: If connection fails after retries.
    """
    try:
        client = MongoClient(
            host=mongo_host,
            port=mongo_port,
            username=mongo_user,
            password=mongo_password,
            authSource="admin"
        )
        client.server_info()  # Test connection
        collection = client[mongo_db][mongo_collection_name]
        SCRIPT_LOGGER.info("Connected to MongoDB: %s:%d, database=%s, collection=%s", mongo_host, mongo_port, mongo_db, mongo_collection_name)
        return collection
    except ConnectionFailure as e:
        SCRIPT_LOGGER.error("Failed to connect to MongoDB: %s\n%s", str(e), traceback.format_exc())
        raise
    except Exception as e:
        SCRIPT_LOGGER.error("Unexpected error connecting to MongoDB: %s\n%s", str(e), traceback.format_exc())
        raise

def main() -> None:
    """
    Main function to generate and insert random client_alpha support records into MongoDB.
    """
    start_time = datetime.now(timezone.utc)

    # Parse command-line arguments
    try:
        parser = argparse.ArgumentParser(description="Generate and insert records into MongoDB.")
        parser.add_argument(
            "input",
            nargs="?",
            default=None,
            type=int,
            help="Number of records to generate (default: random between 1 and 100_000)"
        )
        args = parser.parse_args()
        num_records = args.input if args.input is not None else random.randint(1, 100_000)
        if num_records <= 0:
            raise ValueError(f"Number of records must be a positive integer, got {num_records}")
        SCRIPT_LOGGER.info("Starting %s with input %d, logging to %s", script_name, num_records, log_file)
    except Exception as e:
        SCRIPT_LOGGER.error("Failed to parse arguments: %s\n%s", str(e), traceback.format_exc())
        sys.exit(1)

    # Fetch configuration with error handling
    try:
        mongo_host = os.environ["MONGO_HOST"]
        mongo_port = int(os.environ["MONGO_PORT"])
        mongo_db = os.environ["MONGO_DB"]
        mongo_user = os.environ["MONGO_USER"]
        mongo_password = os.environ["MONGO_PASSWORD"]
        mongo_collection_name = os.environ["MONGO_DB"]  # Intentional, used as SFTP-like path
        chunk_size = int(os.environ["CHUNK_SIZE"])
        SCRIPT_LOGGER.info("----- Configuration Details -----")
        SCRIPT_LOGGER.info(
            "Loaded environment variables: MONGO_HOST=%s, MONGO_PORT=%d, MONGO_DB=%s, MONGO_USER=%s, CHUNK_SIZE=%d",
            mongo_host, mongo_port, mongo_db, mongo_user, chunk_size
        )
        SCRIPT_LOGGER.info("MongoDB collection name set to: %s (used as SFTP-like path)", mongo_collection_name)
        SCRIPT_LOGGER.info("--------------------------------")
    except KeyError as e:
        SCRIPT_LOGGER.error("Missing environment variable: %s\n%s", e, traceback.format_exc())
        sys.exit(1)
    except ValueError as e:
        SCRIPT_LOGGER.error("Invalid environment variable value: %s\n%s", e, traceback.format_exc())
        sys.exit(1)

    # Connect to databases
    try:
        sqlite_connection = connect_to_database()
        SCRIPT_LOGGER.info("Connected to SQLite database: %s", os.environ["SQLITE_DB_FILE"])
    except Exception as e:
        SCRIPT_LOGGER.error("Failed to connect to SQLite database: %s\n%s", str(e), traceback.format_exc())
        sys.exit(1)

    try:
        collection = connect_to_mongodb(
            mongo_host, mongo_port, mongo_user, mongo_password, mongo_db, mongo_collection_name
        )
    except Exception as e:
        SCRIPT_LOGGER.error("Failed to connect to MongoDB: %s\n%s", str(e), traceback.format_exc())
        close_database_connection(sqlite_connection)
        sys.exit(1)

    # Fetch allowed values
    try:
        SCRIPT_LOGGER.info("----- Fetching Allowed Values from SQLite -----")
        support_categories = fetch_lookup_dictionary(
            conn=sqlite_connection,
            table_name="support_areas",
            source_name="CLIENT_ALPHA",
            column_name="support_area_name"
        )
        agent_pseudo_names = fetch_lookup_dictionary(
            conn=sqlite_connection,
            table_name="agents",
            source_name="CLIENT_ALPHA",
            column_name="pseudo_code"
        )
        customer_types = fetch_lookup_dictionary(
            conn=sqlite_connection,
            table_name="customer_types",
            source_name="CLIENT_ALPHA",
            column_name="customer_type_name"
        )
        SCRIPT_LOGGER.info(
            "Fetched allowed values - Support Categories: %d, Agent Pseudo Names: %d, Customer Types: %d",
            len(support_categories), len(agent_pseudo_names), len(customer_types)
        )
        SCRIPT_LOGGER.debug("Support Categories: %s", support_categories)
        SCRIPT_LOGGER.debug("Agent Pseudo Names: %s", agent_pseudo_names)
        SCRIPT_LOGGER.debug("Customer Types: %s", customer_types)
        SCRIPT_LOGGER.info("---------------------------------------------")
    except Exception as e:
        SCRIPT_LOGGER.error("Failed to fetch allowed values from SQLite: %s\n%s", str(e), traceback.format_exc())
        close_database_connection(sqlite_connection)
        sys.exit(1)

    # Initialize from checkpoint
    try:
        SCRIPT_LOGGER.info("----- Initializing from Checkpoint -----")
        last_execution_info = get_last_execution_info(sqlite_connection, script_name)
        if last_execution_info is None:
            script_status = "NEW"
            max_serial_number = 0
            max_interaction_id = 0
            start_records_processed = 0
            new_records = 0
            update_records = 0
            null_records = 0
            SCRIPT_LOGGER.info("No previous checkpoint found, starting new run")
        else:
            script_status, processing_count, new_records, update_records, null_records, max_serial_number, max_interaction_id = last_execution_info
            start_records_processed = 0  # Reset to 0 to start fresh
            if script_status == "FAILURE":
                script_status = "RE-PROCESSING"
                num_records = processing_count
                new_records = new_records # Retain on failure
                update_records = update_records  # Retain on failure
                null_records = null_records  # Retain on failure
                SCRIPT_LOGGER.info(
                    "Resuming from failed run: max_serial_number=%d, max_interaction_id=%d, retained update_records=%d, null_records=%d",
                    max_serial_number, max_interaction_id, update_records, null_records
                )
            else:
                script_status = "PROCESSING"
                new_records = 0  # Reset for new run
                update_records = 0  # Reset for new run
                null_records = 0  # Reset for new run
                SCRIPT_LOGGER.info(
                    "Starting new run from checkpoint: max_serial_number=%d, max_interaction_id=%d",
                    max_serial_number, max_interaction_id
                )

        checkpoint_management(
            sqlite_connection, script_name, script_status, max_serial_number, max_interaction_id,
            num_records, new_records, update_records, null_records
        )
        SCRIPT_LOGGER.info(
            "Initialized checkpoint: script_status=%s, processing_count=%d, new_records=%d, update_records=%d, null_records=%d",
            script_status, num_records, new_records, update_records, null_records
        )
        SCRIPT_LOGGER.info("---------------------------------------")
    except Exception as e:
        SCRIPT_LOGGER.error("Failed to initialize from checkpoint: %s\n%s", str(e), traceback.format_exc())
        close_database_connection(sqlite_connection)
        sys.exit(1)

    # Generate and insert records
    total_new_records = 0
    total_update_records = 0
    total_null_records = 0
    total_chunks_processed = 0
    try:
        new_records, update_records, null_records, chunks_processed, max_serial_number, max_interaction_id = generate_and_update_records(
            support_categories=support_categories,
            agent_pseudo_names=agent_pseudo_names,
            customer_types=customer_types,
            start_serial_number=max_serial_number,
            start_interaction_id=max_interaction_id,
            num_records=num_records,
            collection=collection,
            chunk_size=chunk_size,
            sqlite_connection=sqlite_connection,
            script_name=script_name,
            start_records_processed=start_records_processed
        )
        total_new_records += new_records
        total_update_records += update_records
        total_null_records += null_records
        total_chunks_processed += chunks_processed

        checkpoint_management(
            sqlite_connection, script_name, "SUCCESS", max_serial_number, max_interaction_id,
            num_records, 0, 0, 0
        )
        SCRIPT_LOGGER.info("Successfully completed run, checkpoint updated to SUCCESS")
    except Exception as e:
        checkpoint_management(
            sqlite_connection, script_name, "FAILURE", max_serial_number, max_interaction_id,
            num_records, total_new_records, total_update_records, total_null_records
        )
        SCRIPT_LOGGER.error("Run failed, checkpoint updated to FAILURE: %s\n%s", str(e), traceback.format_exc())
        sys.exit(1)
    finally:
        close_database_connection(sqlite_connection)
        collection.database.client.close()
        SCRIPT_LOGGER.info("Closed database connections")

    # Log summary
    end_time = datetime.now(timezone.utc)
    runtime_seconds = (end_time - start_time).total_seconds()
    SCRIPT_LOGGER.info("----- Run Summary -----")
    SCRIPT_LOGGER.info(
        "Total records requested: %d", num_records
    )
    SCRIPT_LOGGER.info(
        "Records processed: %d (including null records present in new records)", total_new_records + total_update_records
    )
    SCRIPT_LOGGER.info(
        "Total new records: %d", total_new_records
    )
    SCRIPT_LOGGER.info(
        "Total update records: %d", total_update_records
    )
    SCRIPT_LOGGER.info(
        "Total null records: %d", total_null_records
    )
    SCRIPT_LOGGER.info(
        "Chunks processed: %d", total_chunks_processed
    )
    SCRIPT_LOGGER.info(
        "Runtime (seconds): %.2f", runtime_seconds
    )
    SCRIPT_LOGGER.info("-----------------------")
    SCRIPT_LOGGER.info("Client Alpha Data Generator completed successfully")

if __name__ == "__main__":
    main()