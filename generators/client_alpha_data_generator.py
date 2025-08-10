"""
Client Alpha Data Generator (Mongo/JSON)

Generates randomized support records for CLIENT_ALPHA:
- Reads allowed values from SQLite (lookup tables)
- Resumes/records progress via checkpoint table
- Inserts/updates documents in MongoDB in bulk
- Supports chunked writes and retrying DB connections

Designed to run under the Data Generator Orchestrator.
"""

from __future__ import annotations

import argparse
import gc
import logging
import os
import random
import sqlite3
import sys
import traceback
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Any, Optional, Tuple

from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.errors import ConnectionFailure, BulkWriteError
from pymongo.operations import UpdateOne
from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_exception_type

from db_operations import (
    connect_to_database,
    fetch_lookup_list,
    checkpoint_management,
    get_last_execution_info,
    close_database_connection,
)

# ========================== Config & Logging =========================== #

DEFAULT_CHUNK_SIZE = 1000
SCRIPT_NAME = "client_alpha_data_generator"

# Structured logger (keeps your format)
SCRIPT_LOGGER = logging.getLogger(SCRIPT_NAME)
SCRIPT_LOGGER.setLevel(logging.INFO)

LOG_FILE = os.environ.get(
    "SCRIPT_LOG_FILE",
    f"/app/logs/{SCRIPT_NAME}_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.log",
)
_formatter = logging.Formatter(
    "timestamp=%(asctime)s level=%(levelname)s script=%(name)s message=%(message)s"
)
_file_handler = logging.FileHandler(LOG_FILE)
_file_handler.setFormatter(_formatter)
_console_handler = logging.StreamHandler(sys.stdout)
_console_handler.setFormatter(_formatter)
SCRIPT_LOGGER.addHandler(_file_handler)
SCRIPT_LOGGER.addHandler(_console_handler)

# Optional: reproducible randomness
if os.getenv("ORCH_RANDOM_SEED"):
    try:
        random.seed(int(os.environ["ORCH_RANDOM_SEED"]))
        SCRIPT_LOGGER.info("Random seed set from ORCH_RANDOM_SEED=%s",
                           os.environ["ORCH_RANDOM_SEED"])
    except ValueError:
        SCRIPT_LOGGER.warning("Invalid ORCH_RANDOM_SEED; ignoring.")

# =========================== MongoDB Connect =========================== #


@retry(
    stop=stop_after_attempt(3),
    wait=wait_fixed(5),
    retry=retry_if_exception_type(ConnectionFailure),
)
def connect_to_mongodb(
    mongo_host: str,
    mongo_port: int,
    mongo_user: str,
    mongo_password: str,
    mongo_db: str,
    mongo_collection_name: str,
) -> Collection:
    """
    Connect to MongoDB and return the specified collection.
    """
    try:
        client = MongoClient(
            host=mongo_host,
            port=mongo_port,
            username=mongo_user,
            password=mongo_password,
            authSource="admin",
        )
        client.server_info()  # Test connection; raises on failure
        coll = client[mongo_db][mongo_collection_name]
        SCRIPT_LOGGER.info(
            "Connected to MongoDB: %s:%d db=%s collection=%s",
            mongo_host,
            mongo_port,
            mongo_db,
            mongo_collection_name,
        )
        return coll
    except ConnectionFailure as e:
        SCRIPT_LOGGER.error("Failed to connect to MongoDB: %s\n%s", str(e),
                            traceback.format_exc())
        raise
    except Exception as e:
        SCRIPT_LOGGER.error("Unexpected MongoDB error: %s\n%s", str(e),
                            traceback.format_exc())
        raise


# ============================ Record Gen ============================== #


def generate_random_record(
    interaction_id: int,
    support_categories: List[str],
    agent_pseudo_names: List[str],
    customer_types: List[str],
) -> Dict[str, Optional[Any]]:
    """
    Generate a single random support record.
    """
    interaction_duration = random.randint(10, 600)
    record = {
        "INTERACTION_ID":
        interaction_id,
        "SUPPORT_CATEGORY":
        random.choice(support_categories),
        "AGENT_PSEUDO_NAME":
        random.choice(agent_pseudo_names),
        "CONTACT_DATE":
        (datetime.now(timezone.utc) - timedelta(days=random.randint(0, 1000))
         ).strftime("%d/%m/%Y %H:%M:%S"),
        "INTERACTION_STATUS":
        random.choice(["COMPLETED", "DROPPED", "TRANSFERRED"]),
        "INTERACTION_TYPE":
        random.choice(["CALL", "CHAT"]),
        "TYPE_OF_CUSTOMER":
        random.choice(customer_types),
        "INTERACTION_DURATION":
        interaction_duration,
        "TOTAL_TIME":
        interaction_duration + random.randint(10, 600),
        "STATUS_OF_CUSTOMER_INCIDENT":
        random.choice([
            "RESOLVED",
            "PENDING RESOLUTION",
            "PENDING CUSTOMER UPDATE",
            "WORK IN PROGRESS",
            "TRANSFERRED TO ANOTHER QUEUE",
        ]),
        "RESOLVED_IN_FIRST_CONTACT":
        random.choice(["YES", "NO"]),
        "SOLUTION_TYPE":
        random.choice(["SELF-HELP OPTION", "SUPPORT TEAM INTERVENTION"]),
        "RATING":
        random.randint(1, 10),
    }

    return record


# =========================== Core Processing =========================== #


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
    start_records_processed: int = 0,
) -> Tuple[int, int, int, int, int, int]:
    """
    Generate and upsert records into MongoDB using bulk writes + checkpoints.

    Returns:
        (new_records, update_records, null_records, chunks_processed, serial_number, interaction_id)
    """
    serial_number = start_serial_number
    interaction_id = start_interaction_id
    new_records = 0
    update_records = 0
    null_records = 0
    records_processed = start_records_processed
    chunks_processed = 0

    # Defensive clamp
    if num_records <= 0:
        SCRIPT_LOGGER.warning(
            "Requested num_records <= 0 (%d); nothing to process.",
            num_records)
        return 0, 0, 0, 0, serial_number, interaction_id

    try:
        records_to_process = max(0, num_records - records_processed)
        if records_to_process <= 0:
            SCRIPT_LOGGER.warning(
                "No new records to process: already processed=%d of requested=%d",
                records_processed,
                num_records,
            )
            return (
                new_records,
                update_records,
                null_records,
                chunks_processed,
                serial_number,
                interaction_id,
            )

        SCRIPT_LOGGER.info(
            "Starting generation: records_to_process=%d chunk_size=%d",
            records_to_process,
            chunk_size,
        )

        total_chunks = (records_to_process + chunk_size - 1) // chunk_size

        for chunk_start in range(0, records_to_process, chunk_size):
            chunk_end = min(chunk_start + chunk_size, records_to_process)
            chunk_records = chunk_end - chunk_start
            highest_interaction_id = interaction_id  # track highest in this chunk
            operations: List[UpdateOne] = []
            chunk_update_records = 0

            SCRIPT_LOGGER.info(
                "----- Processing chunk %d/%d: %d records (records %d to %d) -----",
                chunks_processed + 1,
                total_chunks,
                chunk_records,
                records_processed + 1,
                records_processed + chunk_records,
            )

            for _ in range(chunk_records):
                serial_number += 1
                interaction_id += 1
                highest_interaction_id = interaction_id  # keep updated within the chunk

                new_doc = generate_random_record(
                    interaction_id,
                    support_categories,
                    agent_pseudo_names,
                    customer_types,
                )

                # Random nulls to simulate data quality
                if random.random() < 0.1:
                    key_to_nullify = random.choice([
                        "SUPPORT_CATEGORY",
                        "AGENT_PSEUDO_NAME",
                        "CONTACT_DATE",
                        "INTERACTION_STATUS",
                        "INTERACTION_TYPE",
                        "TYPE_OF_CUSTOMER",
                        "INTERACTION_DURATION",
                        "TOTAL_TIME",
                        "STATUS_OF_CUSTOMER_INCIDENT",
                        "SOLUTION_TYPE",
                        "RATING",
                    ])
                    new_doc[key_to_nullify] = None
                    null_records += 1

                new_doc["_id"] = f"{serial_number}-{interaction_id}"
                new_doc["serial_number"] = serial_number
                new_doc["record_id"] = interaction_id

                operations.append(
                    UpdateOne({"_id": new_doc["_id"]}, {"$set": new_doc},
                              upsert=True))
                new_records += 1

                # Pseudo-update some earlier records (up to the latest seen in this chunk)
                if random.random() < 0.25 and highest_interaction_id > 1:
                    serial_number += 1
                    update_interaction_id = random.randint(
                        1, highest_interaction_id)
                    upd_doc = generate_random_record(
                        update_interaction_id,
                        support_categories,
                        agent_pseudo_names,
                        customer_types,
                    )
                    upd_doc["_id"] = f"{serial_number}-{update_interaction_id}"
                    upd_doc["serial_number"] = serial_number
                    upd_doc["record_id"] = update_interaction_id
                    operations.append(
                        UpdateOne({"_id": upd_doc["_id"]}, {"$set": upd_doc},
                                  upsert=True))
                    update_records += 1
                    chunk_update_records += 1

                records_processed += 1

            SCRIPT_LOGGER.info(
                "Chunk %d: ops=%d (pseudo-updates=%d) nulls_so_far=%d",
                chunks_processed + 1,
                len(operations),
                chunk_update_records,
                null_records,
            )

            # Bulk write
            try:
                collection.bulk_write(operations, ordered=False)
                SCRIPT_LOGGER.info(
                    "Bulk write succeeded: %d ops in chunk %d",
                    len(operations),
                    chunks_processed + 1,
                )
            except BulkWriteError as e:
                SCRIPT_LOGGER.error(
                    "Bulk write failed for chunk %d: %s\n%s",
                    chunks_processed + 1,
                    e.details,
                    traceback.format_exc(),
                )
                raise
            except Exception as e:
                SCRIPT_LOGGER.error(
                    "Unexpected error during bulk write for chunk %d: %s\n%s",
                    chunks_processed + 1,
                    str(e),
                    traceback.format_exc(),
                )
                raise
            finally:
                # release memory
                del operations
                gc.collect()

            # Update checkpoint after each chunk
            try:
                checkpoint_management(
                    sqlite_connection,
                    script_name,
                    "PROCESSING",
                    serial_number,
                    interaction_id,
                    num_records,
                    new_records,
                    update_records,
                    null_records,
                )
                SCRIPT_LOGGER.info(
                    "Checkpoint updated (chunk %d): new=%d upd=%d null=%d max_serial=%d max_rec_id=%d",
                    chunks_processed + 1,
                    new_records,
                    update_records,
                    null_records,
                    serial_number,
                    interaction_id,
                )
            except Exception as e:
                SCRIPT_LOGGER.error(
                    "Failed updating checkpoint (chunk %d): %s\n%s",
                    chunks_processed + 1,
                    str(e),
                    traceback.format_exc(),
                )
                raise

            chunks_processed += 1

        SCRIPT_LOGGER.info(
            "Completed generation: new=%d upd=%d null=%d",
            new_records,
            update_records,
            null_records,
        )
        return (
            new_records,
            update_records,
            null_records,
            chunks_processed,
            serial_number,
            interaction_id,
        )

    except Exception as e:
        SCRIPT_LOGGER.error("Generation failed: %s\n%s", str(e),
                            traceback.format_exc())
        raise


# ============================== Main Flow ============================== #


def _parse_args() -> int:
    parser = argparse.ArgumentParser(
        description="Generate and insert records into MongoDB.")
    parser.add_argument(
        "input",
        nargs="?",
        default=None,
        type=int,
        help="Number of records to generate (default: random 1..100_000)",
    )
    args = parser.parse_args()
    return args.input if args.input is not None else random.randint(1, 100_000)


def _require_env() -> Tuple[str, int, str, str, str, str, int]:
    """
    Validate and return required env vars.
    Returns:
        (mongo_host, mongo_port, mongo_db, mongo_user, mongo_password, mongo_collection_name, chunk_size)
    """
    try:
        mongo_host = os.environ["MONGO_HOST"]
        mongo_port = int(os.environ["MONGO_PORT"])
        mongo_db = os.environ["MONGO_DB"]
        mongo_user = os.environ["MONGO_USER"]
        mongo_password = os.environ["MONGO_PASSWORD"]
        # Preserve your odd-but-intentional default: collection name == MONGO_DB
        mongo_collection_name = os.getenv("MONGO_COLLECTION", mongo_db)
        chunk_size = int(os.getenv("CHUNK_SIZE", str(DEFAULT_CHUNK_SIZE)))
    except KeyError as e:
        SCRIPT_LOGGER.error("Missing environment variable: %s\n%s", e,
                            traceback.format_exc())
        sys.exit(1)
    except ValueError as e:
        SCRIPT_LOGGER.error("Invalid numeric environment value: %s\n%s", e,
                            traceback.format_exc())
        sys.exit(1)

    if chunk_size <= 0:
        SCRIPT_LOGGER.warning(
            "CHUNK_SIZE<=0; falling back to DEFAULT_CHUNK_SIZE=%d",
            DEFAULT_CHUNK_SIZE)
        chunk_size = DEFAULT_CHUNK_SIZE

    return (
        mongo_host,
        mongo_port,
        mongo_db,
        mongo_user,
        mongo_password,
        mongo_collection_name,
        chunk_size,
    )


def _validate_lookups(
    support_categories: List[str],
    agent_pseudo_names: List[str],
    customer_types: List[str],
) -> None:
    """
    Ensure lookup lists are non-empty; fail fast if any are empty to avoid pointless runs.
    """
    problems = []
    if not support_categories:
        problems.append("support_categories")
    if not agent_pseudo_names:
        problems.append("agent_pseudo_names")
    if not customer_types:
        problems.append("customer_types")
    if problems:
        raise RuntimeError(f"Lookup lists are empty: {', '.join(problems)}")


def main() -> None:
    """
    Generate and insert random client_alpha support records into MongoDB.
    """
    start_time = datetime.now(timezone.utc)

    # Parse args
    try:
        num_records = _parse_args()
        if num_records <= 0:
            raise ValueError(
                f"Number of records must be positive, got {num_records}")
        SCRIPT_LOGGER.info("Starting %s with input=%d, log=%s", SCRIPT_NAME,
                           num_records, LOG_FILE)
    except Exception as e:
        SCRIPT_LOGGER.error("Failed to parse arguments: %s\n%s", str(e),
                            traceback.format_exc())
        sys.exit(1)

    # Env/config
    (
        mongo_host,
        mongo_port,
        mongo_db,
        mongo_user,
        mongo_password,
        mongo_collection_name,
        chunk_size,
    ) = _require_env()
    SCRIPT_LOGGER.info(
        "----- Configuration -----\nMONGO_HOST=%s MONGO_PORT=%d MONGO_DB=%s MONGO_USER=%s CHUNK_SIZE=%d COLLECTION=%s\n-------------------------",
        mongo_host,
        mongo_port,
        mongo_db,
        mongo_user,
        chunk_size,
        mongo_collection_name,
    )

    # DB connections
    try:
        sqlite_connection = connect_to_database()
        SCRIPT_LOGGER.info("Connected to SQLite: %s",
                           os.environ.get("SQLITE_DB_FILE"))
    except Exception as e:
        SCRIPT_LOGGER.error("Failed to connect to SQLite: %s\n%s", str(e),
                            traceback.format_exc())
        sys.exit(1)

    try:
        collection = connect_to_mongodb(
            mongo_host,
            mongo_port,
            mongo_user,
            mongo_password,
            mongo_db,
            mongo_collection_name,
        )
    except Exception as e:
        SCRIPT_LOGGER.error("Failed to connect to MongoDB: %s\n%s", str(e),
                            traceback.format_exc())
        close_database_connection(sqlite_connection)
        sys.exit(1)

    # Lookups
    try:
        SCRIPT_LOGGER.info("----- Fetching Lookup Lists from SQLite -----")
        support_categories = fetch_lookup_list(
            conn=sqlite_connection,
            table_name="support_areas",
            source_name="CLIENT_ALPHA",
            column_name="support_area_name",
        )
        agent_pseudo_names = fetch_lookup_list(
            conn=sqlite_connection,
            table_name="agents",
            source_name="CLIENT_ALPHA",
            column_name="pseudo_code",
        )
        customer_types = fetch_lookup_list(
            conn=sqlite_connection,
            table_name="customer_types",
            source_name="CLIENT_ALPHA",
            column_name="customer_type_name",
        )
        _validate_lookups(support_categories, agent_pseudo_names,
                          customer_types)
        SCRIPT_LOGGER.info(
            "Fetched lookups: support_categories=%d agent_pseudo_names=%d customer_types=%d",
            len(support_categories),
            len(agent_pseudo_names),
            len(customer_types),
        )
    except Exception as e:
        SCRIPT_LOGGER.error("Failed to fetch/validate lookups: %s\n%s", str(e),
                            traceback.format_exc())
        close_database_connection(sqlite_connection)
        sys.exit(1)

    # Initialize from checkpoint
    try:
        SCRIPT_LOGGER.info("----- Initializing from Checkpoint -----")
        last_info = get_last_execution_info(sqlite_connection, SCRIPT_NAME)
        if last_info is None:
            script_status = "NEW"
            max_serial_number = 0
            max_interaction_id = 0
            start_records_processed = 0
            new_records = 0
            update_records = 0
            null_records = 0
            SCRIPT_LOGGER.info("No previous checkpoint found — starting fresh")
        else:
            (
                script_status,
                processing_count,
                new_records,
                update_records,
                null_records,
                max_serial_number,
                max_interaction_id,
            ) = last_info
            start_records_processed = 0  # intentional: start fresh count each run
            if script_status == "FAILURE":
                script_status = "RE-PROCESSING"
                num_records = processing_count
                # Keep prior tallies on failure (per your current behavior)
                SCRIPT_LOGGER.info(
                    "Resuming failed run: max_serial=%d max_rec_id=%d keep(upd=%d null=%d)",
                    max_serial_number,
                    max_interaction_id,
                    update_records,
                    null_records,
                )
            else:
                script_status = "PROCESSING"
                new_records = 0
                update_records = 0
                null_records = 0
                SCRIPT_LOGGER.info(
                    "Starting new run from checkpoint: max_serial=%d max_rec_id=%d",
                    max_serial_number,
                    max_interaction_id,
                )

        checkpoint_management(
            sqlite_connection,
            SCRIPT_NAME,
            script_status,
            max_serial_number,
            max_interaction_id,
            num_records,
            new_records,
            update_records,
            null_records,
        )
        SCRIPT_LOGGER.info(
            "Checkpoint initialized: status=%s processing_count=%d new=%d upd=%d null=%d",
            script_status,
            num_records,
            new_records,
            update_records,
            null_records,
        )
    except Exception as e:
        SCRIPT_LOGGER.error("Failed to initialize checkpoint: %s\n%s", str(e),
                            traceback.format_exc())
        close_database_connection(sqlite_connection)
        sys.exit(1)

    # Generate + write
    total_new_records = 0
    total_update_records = 0
    total_null_records = 0
    total_chunks_processed = 0

    try:
        (
            new_records,
            update_records,
            null_records,
            chunks_processed,
            max_serial_number,
            max_interaction_id,
        ) = generate_and_update_records(
            support_categories=support_categories,
            agent_pseudo_names=agent_pseudo_names,
            customer_types=customer_types,
            start_serial_number=max_serial_number,
            start_interaction_id=max_interaction_id,
            num_records=num_records,
            collection=collection,
            chunk_size=chunk_size,
            sqlite_connection=sqlite_connection,
            script_name=SCRIPT_NAME,
            start_records_processed=start_records_processed,
        )

        total_new_records += new_records
        total_update_records += update_records
        total_null_records += null_records
        total_chunks_processed += chunks_processed

        checkpoint_management(
            sqlite_connection,
            SCRIPT_NAME,
            "SUCCESS",
            max_serial_number,
            max_interaction_id,
            num_records,
            0,
            0,
            0,
        )
        SCRIPT_LOGGER.info("Run completed — checkpoint set to SUCCESS")

    except Exception as e:
        checkpoint_management(
            sqlite_connection,
            SCRIPT_NAME,
            "FAILURE",
            max_serial_number,
            max_interaction_id,
            num_records,
            total_new_records,
            total_update_records,
            total_null_records,
        )
        SCRIPT_LOGGER.error(
            "Run failed — checkpoint set to FAILURE: %s\n%s",
            str(e),
            traceback.format_exc(),
        )
        sys.exit(1)
    finally:
        # Close DBs
        try:
            close_database_connection(sqlite_connection)
        finally:
            try:
                collection.database.client.close()
            except Exception:
                pass
        SCRIPT_LOGGER.info("Closed database connections")

    # Summary
    end_time = datetime.now(timezone.utc)
    runtime_seconds = (end_time - start_time).total_seconds()

    SCRIPT_LOGGER.info("----- Run Summary -----")
    SCRIPT_LOGGER.info("Total records requested: %d", num_records)
    SCRIPT_LOGGER.info("Records processed (new+updates): %d",
                       total_new_records + total_update_records)
    SCRIPT_LOGGER.info("Total new records: %d", total_new_records)
    SCRIPT_LOGGER.info("Total update records: %d", total_update_records)
    SCRIPT_LOGGER.info("Total null records: %d", total_null_records)
    SCRIPT_LOGGER.info("Chunks processed: %d", total_chunks_processed)
    SCRIPT_LOGGER.info("Runtime (seconds): %.2f", runtime_seconds)
    SCRIPT_LOGGER.info("-----------------------")
    SCRIPT_LOGGER.info("Client Alpha Data Generator completed successfully")


if __name__ == "__main__":
    main()
