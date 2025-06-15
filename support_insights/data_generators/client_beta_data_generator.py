"""
This script generates random support records in XML format based on allowed values from a SQLite database,
sends them to a Kafka topic in batches per chunk, and tracks progress in a SQLite checkpoints table. It supports
configurable record counts via command-line arguments, chunk processing for efficient Kafka production, and is
designed to run under Data_Generator_Orchestrator.py with a log file specified via the SCRIPT_LOG_FILE environment
variable.
"""
import random
import os
import sys
import logging
import gc
import traceback
from datetime import datetime, timezone, timedelta
from typing import List, Tuple
import xml.etree.ElementTree as ET
import argparse
import sqlite3
from confluent_kafka import Producer, KafkaError
from db_operations import connect_to_database, fetch_lookup_list, checkpoint_management, get_last_execution_info, close_database_connection

DEFAULT_CHUNK_SIZE = 100
MAX_UPDATE_PERCENTAGE = 0.25
MAX_NULL_PERCENTAGE = 0.10

# Configure structured logging
SCRIPT_LOGGER = logging.getLogger("client_beta_data_generator")
SCRIPT_LOGGER.setLevel(logging.INFO)

# Use orchestrator-provided log file or fallback
script_name = "client_beta_data_generator"
log_file = os.getenv("SCRIPT_LOG_FILE", f"/app/logs/{script_name}_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.log")
formatter = logging.Formatter("timestamp=%(asctime)s level=%(levelname)s script=%(name)s message=%(message)s")
file_handler = logging.FileHandler(log_file)
file_handler.setFormatter(formatter)
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setFormatter(formatter)
SCRIPT_LOGGER.addHandler(file_handler)
SCRIPT_LOGGER.addHandler(console_handler)

def produce_records_to_kafka(
    producer: Producer,
    topic: str,
    records: List[ET.Element]
) -> None:
    """Send a batch of XML records to a Kafka topic."""
    try:
        for xml_data in records:
            xml_string = ET.tostring(xml_data, encoding="unicode")
            producer.produce(topic, xml_string.encode('utf-8'))
            SCRIPT_LOGGER.debug("Produced record to Kafka topic %s: %s", topic, xml_string[:50] + "...")
        producer.flush()
        SCRIPT_LOGGER.info("Flushed %d records to Kafka topic %s", len(records), topic)
    except KafkaError as e:
        SCRIPT_LOGGER.error("Failed to produce batch to Kafka topic %s: %s\n%s", topic, str(e), traceback.format_exc())
        raise

def generate_random_record(
    record_id: int,
    support_categories: List[str],
    agent_pseudo_names: List[str],
    customer_types: List[str]
) -> ET.Element:
    """Generate a random record with specified data fields in XML format."""
    contact_duration_seconds = random.randint(20, 600)
    after_contact_work_time_seconds = random.randint(10, contact_duration_seconds - 10)
    time_stamp = datetime.now(timezone.utc).strftime("%Y/%m/%d %H:%M:%S")

    record = ET.Element("RECORD")
    ET.SubElement(record, "SUPPORT_IDENTIFIER").text = str(record_id)
    ET.SubElement(record, "CONTACT_REGARDING").text = random.choice(support_categories)
    ET.SubElement(record, "AGENT_CODE").text = random.choice(agent_pseudo_names)
    ET.SubElement(record, "DATE_OF_INTERACTION").text = (
        datetime.now(timezone.utc) - timedelta(days=random.randint(0, 1000))
    ).strftime("%Y%m%d%H%M%S")
    ET.SubElement(record, "STATUS_OF_INTERACTION").text = random.choice(
        ["INTERACTION COMPLETED", "CUSTOMER DROPPED", "TRANSFERRED"]
    )
    ET.SubElement(record, "TYPE_OF_INTERACTION").text = random.choice(["CALL", "CHAT"])
    ET.SubElement(record, "CUSTOMER_TYPE").text = random.choice(customer_types)
    ET.SubElement(record, "CONTACT_DURATION").text = str(timedelta(seconds=contact_duration_seconds))
    ET.SubElement(record, "AFTER_CONTACT_WORK_TIME").text = str(
        timedelta(seconds=after_contact_work_time_seconds)
    )
    ET.SubElement(record, "INCIDENT_STATUS").text = random.choice(
        [
            "RESOLVED", "PENDING RESOLUTION", "PENDING CUSTOMER UPDATE",
            "WORK IN PROGRESS", "TRANSFERRED TO ANOTHER QUEUE"
        ]
    )
    ET.SubElement(record, "FIRST_CONTACT_SOLVE").text = random.choice(["TRUE", "FALSE"])
    ET.SubElement(record, "TYPE_OF_RESOLUTION").text = random.choice(
        ["SELF-HELP OPTION", "SUPPORT TEAM INTERVENTION"]
    )
    ET.SubElement(record, "SUPPORT_RATING").text = str(random.randint(1, 5))
    ET.SubElement(record, "TIME_STAMP").text = time_stamp

    SCRIPT_LOGGER.debug("Generated record with SUPPORT_IDENTIFIER: %d and TIME_STAMP: %s", record_id, time_stamp)
    return record

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
    start_records_processed: int = 0
) -> Tuple[int, int, int, int, int, int]:
    """Generate records in chunks, send them to Kafka in batches, and track progress."""
    serial_number = start_serial_number
    record_id = start_record_id
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
            return new_records, update_records, null_records, chunks_processed, serial_number, record_id

        SCRIPT_LOGGER.info("Starting record generation: records_to_process=%d, chunk_size=%d", records_to_process, chunk_size)

        for chunk_start in range(0, records_to_process, chunk_size):
            chunk_end = min(chunk_start + chunk_size, records_to_process)
            chunk_records = chunk_end - chunk_start
            highest_record_id = record_id
            chunk_update_records = 0
            chunk_null_records = 0
            chunk_records_list = []

            SCRIPT_LOGGER.info(
                "----- Processing chunk %d/%d: %d records (records %d to %d) -----",
                chunks_processed + 1, (records_to_process + chunk_size - 1) // chunk_size,
                chunk_records, records_processed + 1, records_processed + chunk_records
            )

            for _ in range(chunk_records):
                serial_number += 1
                record_id += 1
                new_record = generate_random_record(
                    record_id, support_categories, agent_pseudo_names, customer_types
                )

                if random.random() < 0.1 and null_records < max_nulls:
                    key_to_nullify = random.choice(
                        [
                            "CONTACT_REGARDING", "AGENT_CODE", "DATE_OF_INTERACTION",
                            "STATUS_OF_INTERACTION", "TYPE_OF_INTERACTION", "CUSTOMER_TYPE",
                            "CONTACT_DURATION", "AFTER_CONTACT_WORK_TIME", "INCIDENT_STATUS",
                            "FIRST_CONTACT_SOLVE", "SUPPORT_RATING"
                        ]
                    )
                    new_record.find(key_to_nullify).text = None
                    null_records += 1
                    chunk_null_records += 1
                    SCRIPT_LOGGER.debug("Set %s to None in record %d-%d", key_to_nullify, serial_number, record_id)

                chunk_records_list.append(new_record)
                new_records += 1

                if update_records < max_updates and random.random() < 0.25 and highest_record_id > 1:
                    serial_number += 1
                    update_record_id = random.randint(1, highest_record_id)
                    update_record = generate_random_record(
                        update_record_id, support_categories, agent_pseudo_names, customer_types
                    )
                    chunk_records_list.append(update_record)
                    update_records += 1
                    chunk_update_records += 1
                    SCRIPT_LOGGER.debug("Updated record with SUPPORT_IDENTIFIER: %d", update_record_id)

                records_processed += 1

            # Batch produce records to Kafka
            produce_records_to_kafka(producer, topic, chunk_records_list)
            SCRIPT_LOGGER.info(
                "Chunk %d: Generated %d records, including %d pseudo-updates, %d null records",
                chunks_processed + 1, chunk_records, chunk_update_records, chunk_null_records
            )

            # Clear memory after each chunk
            del chunk_records_list
            gc.collect()
            SCRIPT_LOGGER.debug("Cleared memory for chunk %d: chunk_records_list freed", chunks_processed + 1)

            # Update checkpoint
            try:
                SCRIPT_LOGGER.info(
                    "Before checkpoint update: new_records=%d, update_records=%d, null_records=%d, max_serial_number=%d, max_record_id=%d",
                    new_records, update_records, null_records, serial_number, record_id
                )
                checkpoint_management(
                    sqlite_connection, script_name, "PROCESSING", serial_number,
                    record_id, num_records, new_records, update_records, null_records
                )
                SCRIPT_LOGGER.info(
                    "Checkpoint updated: new_records=%d, update_records=%d, null_records=%d, max_serial_number=%d, max_record_id=%d",
                    new_records, update_records, null_records, serial_number, record_id
                )
            except Exception as e:
                SCRIPT_LOGGER.error("Failed to update checkpoint for chunk %d: %s\n%s", chunks_processed + 1, str(e), traceback.format_exc())
                raise

            chunks_processed += 1

        SCRIPT_LOGGER.info("Completed record generation: total_new_records=%d, total_update_records=%d, total_null_records=%d", new_records, update_records, null_records)
        return new_records, update_records, null_records, chunks_processed, serial_number, record_id
    except Exception as e:
        SCRIPT_LOGGER.error("Failed to generate or send records: %s\n%s", str(e), traceback.format_exc())
        raise

def main() -> None:
    """Main function to generate and send random client_beta support records to Kafka."""
    start_time = datetime.now(timezone.utc)

    # Parse command-line arguments
    try:
        parser = argparse.ArgumentParser(description="Generate and send records to Kafka.")
        parser.add_argument(
            "input",
            nargs="?",
            default=None,
            type=int,
            help="Number of records to generate (default: random between 1 and 1000)"
        )
        args = parser.parse_args()
        num_records = args.input if args.input is not None else random.randint(1, 1000)
        if num_records <= 0:
            raise ValueError(f"Number of records must be a positive integer, got {num_records}")
        SCRIPT_LOGGER.info("Starting %s with input %d, logging to %s", script_name, num_records, log_file)
    except Exception as e:
        SCRIPT_LOGGER.error("Failed to parse arguments: %s\n%s", str(e), traceback.format_exc())
        sys.exit(1)

    # Fetch configuration with error handling
    try:
        kafka_bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
        kafka_topic_name = os.getenv("KAFKA_CLIENT_BETA_STORAGE_TOPIC")
        chunk_size = int(os.getenv("CHUNK_SIZE", str(DEFAULT_CHUNK_SIZE)))
        SCRIPT_LOGGER.info("----- Configuration Details -----")
        SCRIPT_LOGGER.info(
            "Loaded environment variables: KAFKA_BOOTSTRAP_SERVERS=%s, KAFKA_CLIENT_BETA_STORAGE_TOPIC=%s, CHUNK_SIZE=%d",
            kafka_bootstrap_servers, kafka_topic_name, chunk_size
        )
        SCRIPT_LOGGER.info("--------------------------------")
    except KeyError as e:
        SCRIPT_LOGGER.error("Missing environment variable: %s\n%s", e, traceback.format_exc())
        sys.exit(1)
    except ValueError as e:
        SCRIPT_LOGGER.error("Invalid environment variable value: %s\n%s", e, traceback.format_exc())
        sys.exit(1)

    # Validate required environment variables
    required_vars = {
        "KAFKA_BOOTSTRAP_SERVERS": kafka_bootstrap_servers,
        "KAFKA_CLIENT_BETA_STORAGE_TOPIC": kafka_topic_name
    }
    missing_vars = [key for key, value in required_vars.items() if not value]
    if missing_vars:
        SCRIPT_LOGGER.error("Missing required environment variables: %s", ", ".join(missing_vars))
        sys.exit(1)

    # Connect to SQLite database
    try:
        sqlite_connection = connect_to_database()
        SCRIPT_LOGGER.info("Connected to SQLite database: %s", os.getenv("SQLITE_DB_FILE"))
    except Exception as e:
        SCRIPT_LOGGER.error("Failed to connect to SQLite database: %s\n%s", str(e), traceback.format_exc())
        sys.exit(1)

    # Fetch allowed values
    try:
        SCRIPT_LOGGER.info("----- Fetching Allowed Values from SQLite -----")
        support_categories = fetch_lookup_list(
            conn=sqlite_connection,
            table_name="support_areas",
            source_name="CLIENT_BETA",
            column_name="support_area_name"
        )
        agent_pseudo_names = fetch_lookup_list(
            conn=sqlite_connection,
            table_name="agents",
            source_name="CLIENT_BETA",
            column_name="pseudo_code"
        )
        customer_types = fetch_lookup_list(
            conn=sqlite_connection,
            table_name="customer_types",
            source_name="CLIENT_BETA",
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

    # Set up Kafka producer
    producer = None
    try:
        producer_conf = {'bootstrap.servers': kafka_bootstrap_servers}
        producer = Producer(producer_conf)
        SCRIPT_LOGGER.info("Initialized Kafka producer with bootstrap servers: %s", kafka_bootstrap_servers)
    except Exception as e:
        SCRIPT_LOGGER.error("Failed to initialize Kafka producer: %s\n%s", str(e), traceback.format_exc())
        close_database_connection(sqlite_connection)
        sys.exit(1)

    # Initialize from checkpoint
    try:
        SCRIPT_LOGGER.info("----- Initializing from Checkpoint -----")
        last_execution_info = get_last_execution_info(sqlite_connection, script_name)
        if last_execution_info is None:
            script_status = "NEW"
            max_serial_number = 0
            max_record_id = 0
            start_records_processed = 0
            new_records = 0
            update_records = 0
            null_records = 0
            SCRIPT_LOGGER.info("No previous checkpoint found, starting new run")
        else:
            script_status, processing_count, new_records, update_records, null_records, max_serial_number, max_record_id = last_execution_info
            start_records_processed = 0  # Reset to 0 to start fresh
            if script_status == "FAILURE":
                script_status = "RE-PROCESSING"
                num_records = processing_count
                new_records = 0  # Reset for fresh run
                update_records = update_records  # Retain on failure
                null_records = null_records  # Retain on failure
                SCRIPT_LOGGER.info(
                    "Resuming from failed run: max_serial_number=%d, max_record_id=%d, retained update_records=%d, null_records=%d",
                    max_serial_number, max_record_id, update_records, null_records
                )
            else:
                script_status = "PROCESSING"
                new_records = 0  # Reset for new run
                update_records = 0  # Reset for new run
                null_records = 0  # Reset for new run
                SCRIPT_LOGGER.info(
                    "Starting new run from checkpoint: max_serial_number=%d, max_record_id=%d",
                    max_serial_number, max_record_id
                )

        checkpoint_management(
            sqlite_connection, script_name, script_status, max_serial_number, max_record_id,
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

    # Generate and send records
    total_new_records = 0
    total_update_records = 0
    total_null_records = 0
    total_chunks_processed = 0
    try:
        new_records, update_records, null_records, chunks_processed, max_serial_number, max_record_id = generate_and_update_records(
            support_categories=support_categories,
            agent_pseudo_names=agent_pseudo_names,
            customer_types=customer_types,
            start_serial_number=max_serial_number,
            start_record_id=max_record_id,
            num_records=num_records,
            producer=producer,
            topic=kafka_topic_name,
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
            sqlite_connection, script_name, "SUCCESS", max_serial_number, max_record_id,
            num_records, 0, 0, 0
        )
        SCRIPT_LOGGER.info("Successfully completed run, checkpoint updated to SUCCESS")
    except Exception as e:
        checkpoint_management(
            sqlite_connection, script_name, "FAILURE", max_serial_number, max_record_id,
            num_records, total_new_records, total_update_records, total_null_records
        )
        SCRIPT_LOGGER.error("Run failed, checkpoint updated to FAILURE: %s\n%s", str(e), traceback.format_exc())
        sys.exit(1)
    finally:
        producer.flush()
        close_database_connection(sqlite_connection)
        SCRIPT_LOGGER.info("Closed database connections and flushed Kafka producer")

    # Log summary
    end_time = datetime.now(timezone.utc)
    runtime_seconds = (end_time - start_time).total_seconds()
    SCRIPT_LOGGER.info("----- Run Summary -----")
    SCRIPT_LOGGER.info("Total records requested: %d", num_records)
    SCRIPT_LOGGER.info("Records processed: %d (including null records present in new records)", total_new_records + total_update_records)
    SCRIPT_LOGGER.info("Total new records: %d", total_new_records)
    SCRIPT_LOGGER.info("Total update records: %d", total_update_records)
    SCRIPT_LOGGER.info("Total null records: %d", total_null_records)
    SCRIPT_LOGGER.info("Chunks processed: %d", total_chunks_processed)
    SCRIPT_LOGGER.info("Runtime (seconds): %.2f", runtime_seconds)
    SCRIPT_LOGGER.info("-----------------------")
    SCRIPT_LOGGER.info("Client Beta Data Generator completed successfully")

if __name__ == "__main__":
    main()