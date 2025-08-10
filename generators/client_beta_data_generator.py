"""
Client Beta Data Generator (Kafka/XML)

Generates randomized support records for CLIENT_BETA:
- Reads allowed values from SQLite (lookup tables)
- Resumes/records progress via checkpoint table
- Produce inserts/updates as messages to Kafka
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
import xml.etree.ElementTree as ET
from datetime import datetime, timezone, timedelta
from typing import List, Tuple

from confluent_kafka import Producer, KafkaError
from db_operations import (
    connect_to_database,
    fetch_lookup_list,
    checkpoint_management,
    get_last_execution_info,
    close_database_connection,
)

# ========================== Config & Logging =========================== #

DEFAULT_CHUNK_SIZE = 1000
SCRIPT_NAME = "client_beta_data_generator"

SCRIPT_LOGGER = logging.getLogger(SCRIPT_NAME)
SCRIPT_LOGGER.setLevel(logging.INFO)

LOG_FILE = os.getenv(
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

# ============================== Kafka I/O ============================== #


def _delivery_report(err, msg) -> None:
    """Per-message delivery callback (debug level unless error)."""
    if err is not None:
        SCRIPT_LOGGER.error(
            "Kafka delivery failed: %s [topic=%s partition=%s offset=%s]",
            err,
            msg.topic(),
            msg.partition(),
            msg.offset(),
        )
    else:
        SCRIPT_LOGGER.debug("Kafka delivered to %s[%d]@%d", msg.topic(),
                            msg.partition(), msg.offset())


def produce_records_to_kafka(producer: Producer, topic: str,
                             records: List[ET.Element]) -> None:
    """
    Send a batch of XML records to a Kafka topic with backpressure handling.
    """
    try:
        for xml_data in records:
            xml_string = ET.tostring(xml_data, encoding="unicode")

            while True:
                try:
                    producer.produce(topic,
                                     xml_string.encode("utf-8"),
                                     callback=_delivery_report)
                    break
                except BufferError:
                    # Local queue is full; serve delivery callbacks and retry
                    SCRIPT_LOGGER.debug("Kafka queue full; polling to drain…")
                    producer.poll(0.1)

            # Serve callbacks opportunistically
            producer.poll(0)

        # Ensure everything is on the wire for this batch
        producer.flush()
        SCRIPT_LOGGER.info("Flushed %d records to Kafka topic %s",
                           len(records), topic)

    except KafkaError as e:
        SCRIPT_LOGGER.error(
            "Failed to produce batch to Kafka topic %s: %s\n%s",
            topic,
            str(e),
            traceback.format_exc(),
        )
        raise
    except Exception as e:
        SCRIPT_LOGGER.error("Unexpected Kafka produce error: %s\n%s", str(e),
                            traceback.format_exc())
        raise


# ============================ Record Gen ============================== #


def generate_random_record(
    record_id: int,
    support_categories: List[str],
    agent_pseudo_names: List[str],
    customer_types: List[str],
) -> ET.Element:
    """
    Generate a random XML record for Client Beta.
    """
    contact_duration_seconds = random.randint(20, 600)
    after_contact_work_time_seconds = random.randint(
        10, max(10, contact_duration_seconds - 10))
    time_stamp = datetime.now(timezone.utc).strftime("%Y/%m/%d %H:%M:%S")

    record = ET.Element("RECORD")
    ET.SubElement(record, "SUPPORT_IDENTIFIER").text = str(record_id)
    ET.SubElement(record,
                  "CONTACT_REGARDING").text = random.choice(support_categories)
    ET.SubElement(record,
                  "AGENT_CODE").text = random.choice(agent_pseudo_names)
    ET.SubElement(record, "DATE_OF_INTERACTION").text = (
        datetime.now(timezone.utc) -
        timedelta(days=random.randint(0, 1000))).strftime("%Y%m%d%H%M%S")
    ET.SubElement(record, "STATUS_OF_INTERACTION").text = random.choice(
        ["INTERACTION COMPLETED", "CUSTOMER DROPPED", "TRANSFERRED"])
    ET.SubElement(record,
                  "TYPE_OF_INTERACTION").text = random.choice(["CALL", "CHAT"])
    ET.SubElement(record, "CUSTOMER_TYPE").text = random.choice(customer_types)
    ET.SubElement(record, "CONTACT_DURATION").text = str(
        timedelta(seconds=contact_duration_seconds))
    ET.SubElement(record, "AFTER_CONTACT_WORK_TIME").text = str(
        timedelta(seconds=after_contact_work_time_seconds))
    ET.SubElement(record, "INCIDENT_STATUS").text = random.choice([
        "RESOLVED",
        "PENDING RESOLUTION",
        "PENDING CUSTOMER UPDATE",
        "WORK IN PROGRESS",
        "TRANSFERRED TO ANOTHER QUEUE",
    ])
    ET.SubElement(record, "FIRST_CONTACT_SOLVE").text = random.choice(
        ["TRUE", "FALSE"])
    ET.SubElement(record, "TYPE_OF_RESOLUTION").text = random.choice(
        ["SELF-HELP OPTION", "SUPPORT TEAM INTERVENTION"])
    ET.SubElement(record, "SUPPORT_RATING").text = str(random.randint(1, 5))
    ET.SubElement(record, "TIME_STAMP").text = time_stamp

    return record


# =========================== Core Processing =========================== #


def generate_and_update_records(
    support_categories: List[str],
    agent_pseudo_names: List[str],
    customer_types: List[str],
    start_serial_number: int,
    start_record_id: int,
    num_records: int,
    producer: Producer,
    topic: str,
    chunk_size: int,
    sqlite_connection: sqlite3.Connection,
    script_name: str,
    start_records_processed: int = 0,
) -> Tuple[int, int, int, int, int, int]:
    """
    Generate records in chunks, send them to Kafka, update checkpoints.

    Returns:
        (new_records, update_records, null_records, chunks_processed, serial_number, record_id)
    """
    serial_number = start_serial_number
    record_id = start_record_id
    new_records = 0
    update_records = 0
    null_records = 0
    records_processed = start_records_processed
    chunks_processed = 0

    if num_records <= 0:
        SCRIPT_LOGGER.warning(
            "Requested num_records <= 0 (%d); nothing to do.", num_records)
        return 0, 0, 0, 0, serial_number, record_id

    try:
        records_to_process = max(0, num_records - records_processed)
        if records_to_process <= 0:
            SCRIPT_LOGGER.warning(
                "No new records to process: processed=%d of requested=%d",
                records_processed,
                num_records,
            )
            return (
                new_records,
                update_records,
                null_records,
                chunks_processed,
                serial_number,
                record_id,
            )

        total_chunks = (records_to_process + chunk_size - 1) // chunk_size
        SCRIPT_LOGGER.info(
            "Starting generation: records_to_process=%d chunk_size=%d",
            records_to_process,
            chunk_size,
        )

        for chunk_start in range(0, records_to_process, chunk_size):
            chunk_end = min(chunk_start + chunk_size, records_to_process)
            chunk_records = chunk_end - chunk_start
            highest_record_id = record_id
            chunk_update_records = 0
            chunk_null_records = 0
            chunk_records_list: List[ET.Element] = []

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
                record_id += 1
                highest_record_id = record_id

                record = generate_random_record(record_id, support_categories,
                                                agent_pseudo_names,
                                                customer_types)

                # Randomly nullify a field
                if random.random() < 0.1:
                    key_to_nullify = random.choice([
                        "CONTACT_REGARDING",
                        "AGENT_CODE",
                        "DATE_OF_INTERACTION",
                        "STATUS_OF_INTERACTION",
                        "TYPE_OF_INTERACTION",
                        "CUSTOMER_TYPE",
                        "CONTACT_DURATION",
                        "AFTER_CONTACT_WORK_TIME",
                        "INCIDENT_STATUS",
                        "FIRST_CONTACT_SOLVE",
                        "SUPPORT_RATING",
                    ])
                    node = record.find(key_to_nullify)
                    if node is not None:
                        node.text = None
                        null_records += 1
                        chunk_null_records += 1
                        SCRIPT_LOGGER.debug(
                            "Nullified %s in record %d-%d",
                            key_to_nullify,
                            serial_number,
                            record_id,
                        )

                chunk_records_list.append(record)
                new_records += 1
                records_processed += 1

                # Pseudo-update: send another record for a random earlier id
                if random.random() < 0.25 and highest_record_id > 1:
                    serial_number += 1
                    update_record_id = random.randint(1, highest_record_id)
                    upd = generate_random_record(
                        update_record_id,
                        support_categories,
                        agent_pseudo_names,
                        customer_types,
                    )
                    chunk_records_list.append(upd)
                    update_records += 1
                    chunk_update_records += 1

            # Batch produce this chunk
            produce_records_to_kafka(producer, topic, chunk_records_list)

            SCRIPT_LOGGER.info(
                "Chunk %d: generated=%d pseudo_updates=%d nulls_in_chunk=%d",
                chunks_processed + 1,
                chunk_records,
                chunk_update_records,
                chunk_null_records,
            )

            # Free memory
            del chunk_records_list
            gc.collect()
            SCRIPT_LOGGER.debug("Freed chunk %d buffers", chunks_processed + 1)

            # Update checkpoint after each chunk
            try:
                checkpoint_management(
                    sqlite_connection,
                    script_name,
                    "PROCESSING",
                    serial_number,
                    record_id,
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
                    record_id,
                )
            except Exception as e:
                SCRIPT_LOGGER.error(
                    "Failed to update checkpoint (chunk %d): %s\n%s",
                    chunks_processed + 1,
                    str(e),
                    traceback.format_exc(),
                )
                raise

            chunks_processed += 1

        SCRIPT_LOGGER.info(
            "Completed generation: total_new=%d total_upd=%d total_null=%d",
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
            record_id,
        )

    except Exception as e:
        SCRIPT_LOGGER.error("Failed to generate or send records: %s\n%s",
                            str(e), traceback.format_exc())
        raise


# ============================== Main Flow ============================== #


def _parse_args() -> int:
    parser = argparse.ArgumentParser(
        description="Generate and send records to Kafka.")
    parser.add_argument(
        "input",
        nargs="?",
        default=None,
        type=int,
        help="Number of records to generate (default: random 1..1000)",
    )
    args = parser.parse_args()
    return args.input if args.input is not None else random.randint(1, 1000)


def _require_env() -> Tuple[str, str, int]:
    """
    Validate and return required env vars.
    Returns:
        (bootstrap_servers, topic_name, chunk_size)
    """
    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
    topic = os.getenv("KAFKA_CLIENT_BETA_STORAGE_TOPIC")
    chunk_size = int(os.getenv("CHUNK_SIZE", str(DEFAULT_CHUNK_SIZE)))

    missing = [
        k for k, v in {
            "KAFKA_BOOTSTRAP_SERVERS": bootstrap,
            "KAFKA_CLIENT_BETA_STORAGE_TOPIC": topic,
        }.items() if not v
    ]
    if missing:
        SCRIPT_LOGGER.error("Missing required environment variables: %s",
                            ", ".join(missing))
        sys.exit(1)

    if chunk_size <= 0:
        SCRIPT_LOGGER.warning(
            "CHUNK_SIZE<=0; falling back to DEFAULT_CHUNK_SIZE=%d",
            DEFAULT_CHUNK_SIZE)
        chunk_size = DEFAULT_CHUNK_SIZE

    return bootstrap, topic, chunk_size


def _validate_lookups(*lists: List[str]) -> None:
    names = ["support_categories", "agent_pseudo_names", "customer_types"]
    missing = [n for n, lst in zip(names, lists) if not lst]
    if missing:
        raise RuntimeError(f"Lookup lists are empty: {', '.join(missing)}")


def main() -> None:
    start_time = datetime.now(timezone.utc)

    # Args
    try:
        num_records = _parse_args()
        if num_records <= 0:
            raise ValueError(
                f"Number of records must be positive, got {num_records}")
        SCRIPT_LOGGER.info("Starting %s with input=%d log=%s", SCRIPT_NAME,
                           num_records, LOG_FILE)
    except Exception as e:
        SCRIPT_LOGGER.error("Failed to parse arguments: %s\n%s", str(e),
                            traceback.format_exc())
        sys.exit(1)

    # Env/config
    bootstrap, topic_name, chunk_size = _require_env()
    SCRIPT_LOGGER.info(
        "----- Configuration -----\nKAFKA_BOOTSTRAP_SERVERS=%s TOPIC=%s CHUNK_SIZE=%d\n-------------------------",
        bootstrap,
        topic_name,
        chunk_size,
    )

    # SQLite connection
    try:
        sqlite_connection = connect_to_database()
        SCRIPT_LOGGER.info("Connected to SQLite: %s",
                           os.getenv("SQLITE_DB_FILE"))
    except Exception as e:
        SCRIPT_LOGGER.error("Failed to connect to SQLite: %s\n%s", str(e),
                            traceback.format_exc())
        sys.exit(1)

    # Lookups
    try:
        SCRIPT_LOGGER.info("----- Fetching Lookup Lists from SQLite -----")
        support_categories = fetch_lookup_list(
            conn=sqlite_connection,
            table_name="support_areas",
            source_name="CLIENT_BETA",
            column_name="support_area_name",
        )
        agent_pseudo_names = fetch_lookup_list(
            conn=sqlite_connection,
            table_name="agents",
            source_name="CLIENT_BETA",
            column_name="pseudo_code",
        )
        customer_types = fetch_lookup_list(
            conn=sqlite_connection,
            table_name="customer_types",
            source_name="CLIENT_BETA",
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

    # Kafka producer
    producer = None
    try:
        # Keep minimal config; backpressure handled in produce_records_to_kafka
        producer = Producer({"bootstrap.servers": bootstrap})
        SCRIPT_LOGGER.info("Kafka producer initialized (bootstrap=%s)",
                           bootstrap)
    except Exception as e:
        SCRIPT_LOGGER.error(
            "Failed to initialize Kafka producer: %s\n%s",
            str(e),
            traceback.format_exc(),
        )
        close_database_connection(sqlite_connection)
        sys.exit(1)

    # Initialize from checkpoint
    try:
        SCRIPT_LOGGER.info("----- Initializing from Checkpoint -----")
        last = get_last_execution_info(sqlite_connection, SCRIPT_NAME)
        if last is None:
            script_status = "NEW"
            max_serial_number = 0
            max_record_id = 0
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
                max_record_id,
            ) = last
            start_records_processed = 0  # intentionally start fresh processing count
            if script_status == "FAILURE":
                script_status = "RE-PROCESSING"
                num_records = processing_count
                # keep prior update/null tallies on failure (same behavior as before)
                SCRIPT_LOGGER.info(
                    "Resuming failed run: max_serial=%d max_rec_id=%d keep(upd=%d null=%d)",
                    max_serial_number,
                    max_record_id,
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
                    max_record_id,
                )

        checkpoint_management(
            sqlite_connection,
            SCRIPT_NAME,
            script_status,
            max_serial_number,
            max_record_id,
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

    # Generate + send
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
            max_record_id,
        ) = generate_and_update_records(
            support_categories=support_categories,
            agent_pseudo_names=agent_pseudo_names,
            customer_types=customer_types,
            start_serial_number=max_serial_number,
            start_record_id=max_record_id,
            num_records=num_records,
            producer=producer,
            topic=topic_name,
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
            max_record_id,
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
            max_record_id,
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
        try:
            # Ensure any in-flight messages are sent
            producer.flush()
        except Exception:
            pass
        close_database_connection(sqlite_connection)
        SCRIPT_LOGGER.info(
            "Closed SQLite connection and flushed Kafka producer")

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
    SCRIPT_LOGGER.info("Client Beta Data Generator completed successfully")


if __name__ == "__main__":
    main()
