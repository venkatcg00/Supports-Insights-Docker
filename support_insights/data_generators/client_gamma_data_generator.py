"""
This script generates random support records based on allowed values from a SQLite database,
creates temporary compressed CSV files (gzip) for each chunk in a MinIO bucket, combines them into a final
compressed CSV file, and tracks progress in a SQLite checkpoints table. It supports configurable record counts via
command-line arguments, chunk processing, and is designed to run under Data_Generator_Orchestrator.py.

Product: Data Generator Suite
Version: 1.0.0
"""
import argparse
import logging
import traceback
import random
import csv
import gzip
from datetime import datetime, timezone, timedelta
from io import StringIO, BytesIO
from typing import List, Union, Tuple
import boto3
from botocore.exceptions import ClientError, BotoCoreError
from db_operations import connect_to_database, fetch_lookup_dictionary, checkpoint_management, get_last_execution_info, close_database_connection
import os
import sys
import gc
import sqlite3

DEFAULT_CHUNK_SIZE = 100
MAX_UPDATE_PERCENTAGE = 0.25
MAX_NULL_PERCENTAGE = 0.10

# Configure structured logging
SCRIPT_LOGGER = logging.getLogger("client_gamma_data_generator")
SCRIPT_LOGGER.setLevel(logging.INFO)

# Use orchestrator-provided log file or fallback
script_name = "client_gamma_data_generator"
log_file = os.getenv("SCRIPT_LOG_FILE", f"/app/logs/{script_name}_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.log")
formatter = logging.Formatter("timestamp=%(asctime)s level=%(levelname)s script=%(name)s message=%(message)s")
file_handler = logging.FileHandler(log_file)
file_handler.setFormatter(formatter)
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setFormatter(formatter)
SCRIPT_LOGGER.addHandler(file_handler)
SCRIPT_LOGGER.addHandler(console_handler)

def get_s3_client() -> boto3.client:
    """Initialize and return an S3-compatible storage client for MinIO."""
    minio_host = os.getenv("MINIO_HOST")
    minio_port = os.getenv("MINIO_PORT")
    minio_user = os.getenv("MINIO_USER")
    minio_password = os.getenv("MINIO_PASSWORD")

    required_vars = {
        "MINIO_HOST": minio_host,
        "MINIO_PORT": minio_port,
        "MINIO_USER": minio_user,
        "MINIO_PASSWORD": minio_password
    }
    missing_vars = [key for key, value in required_vars.items() if not value]
    if missing_vars:
        SCRIPT_LOGGER.error("Missing required MinIO environment variables: %s", ", ".join(missing_vars))
        raise ValueError(f"Missing required MinIO environment variables: {', '.join(missing_vars)}")

    endpoint_url = f"http://{minio_host}:{minio_port}"
    try:
        client = boto3.client(
            "s3",
            endpoint_url=endpoint_url,
            aws_access_key_id=minio_user,
            aws_secret_access_key=minio_password
        )
        SCRIPT_LOGGER.debug("Initialized S3 client for %s", endpoint_url)
        return client
    except Exception as e:
        SCRIPT_LOGGER.error("Failed to initialize S3 client: %s\n%s", str(e), traceback.format_exc())
        raise

def get_file_count(s3_client: boto3.client, bucket_name: str) -> int:
    """Count the number of files containing 'client_gamma_support_data' in their name in the MinIO bucket."""
    SCRIPT_LOGGER.debug("Counting files containing 'client_gamma_support_data' in bucket: %s", bucket_name)
    try:
        response = s3_client.list_objects_v2(Bucket=bucket_name)
        if "Contents" not in response or not response["Contents"]:
            SCRIPT_LOGGER.info("No files found in bucket: %s", bucket_name)
            return 0
        file_count = len([obj for obj in response["Contents"] if "client_gamma_support_data" in obj["Key"]])
        SCRIPT_LOGGER.info("Found %d files containing 'client_gamma_support_data' in bucket: %s", file_count, bucket_name)
        return file_count
    except ClientError as e:
        SCRIPT_LOGGER.error("MinIO ClientError while counting files: %s\n%s", e.response["Error"]["Message"], traceback.format_exc())
        return 0
    except BotoCoreError as e:
        SCRIPT_LOGGER.error("MinIO BotoCoreError while counting files: %s\n%s", str(e), traceback.format_exc())
        return 0

def write_temp_chunk_to_minio(
    s3_client: boto3.client,
    bucket_name: str,
    chunk_data: List[List[Union[str, None]]],
    chunk_number: int,
    timestamp: str
) -> str:
    """Write a chunk of records to a temporary compressed CSV file in MinIO."""
    headers = [
        "TICKET_IDENTIFIER", "SUPPORT_CATEGORY", "AGENT_NAME", "DATE_OF_CALL",
        "CALL_STATUS", "CALL_TYPE", "TYPE_OF_CUSTOMER", "DURATION", "WORK_TIME",
        "TICKET_STATUS", "RESOLVED_IN_FIRST_CONTACT", "RESOLUTION_CATEGORY", "RATING"
    ]
    temp_file_name = f"temp_{chunk_number}_client_gamma_support_data_{timestamp}.csv.gz"
    csv_buffer = StringIO()
    writer = csv.writer(csv_buffer, delimiter="|", quoting=csv.QUOTE_ALL)
    writer.writerow(headers)
    writer.writerows([[str(val) if val is not None else "" for val in row] for row in chunk_data])

    # Compress the CSV data using gzip
    compressed_buffer = BytesIO()
    with gzip.GzipFile(fileobj=compressed_buffer, mode="wb") as gz:
        gz.write(csv_buffer.getvalue().encode("utf-8"))
    compressed_data = compressed_buffer.getvalue()

    try:
        s3_client.put_object(
            Bucket=bucket_name,
            Key=temp_file_name,
            Body=compressed_data,
            ContentType="text/csv",
            ContentEncoding="gzip"
        )
        SCRIPT_LOGGER.info("Uploaded temporary compressed chunk file to MinIO: %s/%s", bucket_name, temp_file_name)
        return temp_file_name
    except ClientError as e:
        SCRIPT_LOGGER.error("Failed to upload temporary compressed chunk file to MinIO: %s\n%s", e.response["Error"]["Message"], traceback.format_exc())
        raise
    finally:
        csv_buffer.close()
        compressed_buffer.close()

def combine_temp_files(
    s3_client: boto3.client,
    bucket_name: str,
    temp_files: List[str],
    file_count: int,
    timestamp: str
) -> str:
    """Combine temporary compressed CSV files into a single compressed file and delete the temporary files."""
    headers = [
        "TICKET_IDENTIFIER", "SUPPORT_CATEGORY", "AGENT_NAME", "DATE_OF_CALL",
        "CALL_STATUS", "CALL_TYPE", "TYPE_OF_CUSTOMER", "DURATION", "WORK_TIME",
        "TICKET_STATUS", "RESOLVED_IN_FIRST_CONTACT", "RESOLUTION_CATEGORY", "RATING"
    ]
    final_file_name = f"{file_count}_client_gamma_support_data_{timestamp}.csv.gz"
    csv_buffer = StringIO()
    writer = csv.writer(csv_buffer, delimiter="|", quoting=csv.QUOTE_ALL)
    writer.writerow(headers)

    # Combine records from temporary compressed files
    for temp_file in temp_files:
        try:
            response = s3_client.get_object(Bucket=bucket_name, Key=temp_file)
            compressed_data = BytesIO(response["Body"].read())
            with gzip.GzipFile(fileobj=compressed_data, mode="rb") as gz:
                temp_csv = gz.read().decode("utf-8")
            temp_reader = csv.reader(StringIO(temp_csv), delimiter="|", quoting=csv.QUOTE_ALL)
            next(temp_reader)  # Skip header
            for row in temp_reader:
                writer.writerow(row)
            SCRIPT_LOGGER.debug("Combined records from temporary compressed file: %s", temp_file)
        except ClientError as e:
            SCRIPT_LOGGER.error("Failed to read temporary compressed file %s: %s\n%s", temp_file, e.response["Error"]["Message"], traceback.format_exc())
            raise
        except BotoCoreError as e:
            SCRIPT_LOGGER.error("BotoCoreError while reading temporary compressed file %s: %s\n%s", temp_file, str(e), traceback.format_exc())
            raise

    # Compress the final CSV data using gzip
    compressed_buffer = BytesIO()
    with gzip.GzipFile(fileobj=compressed_buffer, mode="wb") as gz:
        gz.write(csv_buffer.getvalue().encode("utf-8"))
    compressed_data = compressed_buffer.getvalue()

    # Upload the final compressed file
    try:
        s3_client.put_object(
            Bucket=bucket_name,
            Key=final_file_name,
            Body=compressed_data,
            ContentType="text/csv",
            ContentEncoding="gzip"
        )
        SCRIPT_LOGGER.info("Uploaded final compressed file to MinIO: %s/%s", bucket_name, final_file_name)
    except ClientError as e:
        SCRIPT_LOGGER.error("Failed to upload final compressed file to MinIO: %s\n%s", e.response["Error"]["Message"], traceback.format_exc())
        raise
    finally:
        csv_buffer.close()
        compressed_buffer.close()

    # Delete temporary files
    for temp_file in temp_files:
        try:
            s3_client.delete_object(Bucket=bucket_name, Key=temp_file)
            SCRIPT_LOGGER.info("Deleted temporary compressed file: %s/%s", bucket_name, temp_file)
        except ClientError as e:
            SCRIPT_LOGGER.warning("Failed to delete temporary compressed file %s: %s\n%s", temp_file, e.response["Error"]["Message"], traceback.format_exc())

    return final_file_name

def generate_random_record(
    record_id: int,
    support_categories: List[str],
    agent_pseudo_names: List[str],
    customer_types: List[str]
) -> List[Union[str, None]]:
    """Generate a random record for the dataset with all fields as strings or None."""
    duration = random.randint(20, 600)
    work_time = random.randint(10, duration - 10)
    record = [
        str(record_id),
        random.choice(support_categories),
        random.choice(agent_pseudo_names),
        (datetime.now(timezone.utc) - timedelta(minutes=duration)).strftime("%m%d%Y%H%M%S"),
        random.choice(["COMPLETED", "DROPPED", "TRANSFERRED"]),
        random.choice(["CALL", "CHAT", "EMAIL"]),
        random.choice(customer_types),
        str(duration),
        str(work_time),
        random.choice(["RESOLVED", "PENDING RESOLUTION", "WORK IN PROGRESS", "TRANSFERRED TO ANOTHER QUEUE"]),
        str(random.choice([1, 0])),
        random.choice(["SELF-HELP OPTION", "SUPPORT TEAM INTERVENTION"]),
        random.choice(["WORST", "BAD", "NEUTRAL", "GOOD", "BEST"])
    ]
    SCRIPT_LOGGER.debug("Generated record with ID: %d", record_id)
    return record

def generate_and_update_records(
    support_categories: List[str],
    agent_pseudo_names: List[str],
    customer_types: List[str],
    start_record_id: int,
    num_records: int,
    s3_client: boto3.client,
    bucket_name: str,
    chunk_size: int,
    sqlite_connection: sqlite3.Connection,
    script_name: str,
    timestamp: str,
    start_records_processed: int = 0
) -> Tuple[int, int, int, int, int, List[str]]:
    """Generate records in chunks, write to temporary compressed CSV files, and track progress."""
    record_id = start_record_id
    new_records = 0
    update_records = 0
    null_records = 0
    records_processed = start_records_processed
    chunks_processed = 0
    temp_files = []

    max_updates = int(num_records * MAX_UPDATE_PERCENTAGE)
    max_nulls = int(num_records * MAX_NULL_PERCENTAGE)

    try:
        records_to_process = max(0, num_records - records_processed)
        if records_to_process <= 0:
            SCRIPT_LOGGER.warning(
                "No new records to process: records_processed=%d exceeds or equals num_records=%d",
                records_processed, num_records
            )
            return new_records, update_records, null_records, chunks_processed, record_id, temp_files

        SCRIPT_LOGGER.info("Starting record generation: records_to_process=%d, chunk_size=%d", records_to_process, chunk_size)

        for chunk_start in range(0, records_to_process, chunk_size):
            chunk_end = min(chunk_start + chunk_size, records_to_process)
            chunk_records = chunk_end - chunk_start
            highest_record_id = record_id
            chunk_data = []
            chunk_update_records = 0
            chunk_null_records = 0

            SCRIPT_LOGGER.info(
                "----- Processing chunk %d/%d: %d records (records %d to %d) -----",
                chunks_processed + 1, (records_to_process + chunk_size - 1) // chunk_size,
                chunk_records, records_processed + 1, records_processed + chunk_records
            )

            for _ in range(chunk_records):
                record_id += 1
                new_record = generate_random_record(record_id, support_categories, agent_pseudo_names, customer_types)

                if random.random() < 0.1 and null_records < max_nulls:
                    idx_to_nullify = random.randint(1, 12)
                    new_record[idx_to_nullify] = None
                    null_records += 1
                    chunk_null_records += 1
                    SCRIPT_LOGGER.debug("Set index %d to None in record %d", idx_to_nullify, record_id)

                chunk_data.append(new_record)
                new_records += 1

                if update_records < max_updates and random.random() < 0.25 and highest_record_id > 1:
                    update_record_id = random.randint(1, highest_record_id)
                    updated_record = generate_random_record(update_record_id, support_categories, agent_pseudo_names, customer_types)
                    chunk_data.append(updated_record)
                    update_records += 1
                    chunk_update_records += 1
                    SCRIPT_LOGGER.debug("Updated record with ID: %d", update_record_id)

                records_processed += 1

            # Write chunk to temporary compressed CSV file
            try:
                temp_file = write_temp_chunk_to_minio(s3_client, bucket_name, chunk_data, chunks_processed + 1, timestamp)
                temp_files.append(temp_file)
            except Exception as e:
                SCRIPT_LOGGER.error("Failed to write chunk %d to temporary compressed file: %s\n%s", chunks_processed + 1, str(e), traceback.format_exc())
                raise

            SCRIPT_LOGGER.info(
                "Chunk %d: Generated %d records, including %d pseudo-updates, %d null records",
                chunks_processed + 1, chunk_records, chunk_update_records, chunk_null_records
            )

            # Clear memory after each chunk
            del chunk_data
            gc.collect()
            SCRIPT_LOGGER.debug("Cleared memory for chunk %d: chunk_data list freed", chunks_processed + 1)

            # Update checkpoint
            try:
                SCRIPT_LOGGER.info(
                    "Before checkpoint update: new_records=%d, update_records=%d, null_records=%d, max_record_id=%d",
                    new_records, update_records, null_records, record_id
                )
                checkpoint_management(
                    sqlite_connection, script_name, "PROCESSING", 0,  # No serial_number in CSV schema
                    record_id, num_records, new_records, update_records, null_records
                )
                SCRIPT_LOGGER.info(
                    "Checkpoint updated: new_records=%d, update_records=%d, null_records=%d, max_record_id=%d",
                    new_records, update_records, null_records, record_id
                )
            except Exception as e:
                SCRIPT_LOGGER.error("Failed to update checkpoint for chunk %d: %s\n%s", chunks_processed + 1, str(e), traceback.format_exc())
                raise

            chunks_processed += 1

        SCRIPT_LOGGER.info("Completed record generation: total_new_records=%d, total_update_records=%d, total_null_records=%d", new_records, update_records, null_records)
        return new_records, update_records, null_records, chunks_processed, record_id, temp_files
    except Exception as e:
        SCRIPT_LOGGER.error("Failed to generate or write records: %s\n%s", str(e), traceback.format_exc())
        raise

def main() -> None:
    """Main function to generate and upload random client_gamma support records with sequential filenames."""
    start_time = datetime.now(timezone.utc)
    timestamp = start_time.strftime("%Y%m%d_%H%M%S")

    # Parse command-line arguments
    try:
        parser = argparse.ArgumentParser(description="Generate and upload client_gamma support records.")
        parser.add_argument(
            "input",
            nargs="?",
            default=None,
            type=int,
            help="Number of records to generate (default: random between 1 and 10000)"
        )
        args = parser.parse_args()
        num_records = args.input if args.input is not None else random.randint(1, 10000)
        if num_records <= 0:
            raise ValueError(f"Number of records must be a positive integer, got {num_records}")
        SCRIPT_LOGGER.info("Starting %s with input %d, logging to %s", script_name, num_records, log_file)
    except Exception as e:
        SCRIPT_LOGGER.error("Failed to parse arguments: %s\n%s", str(e), traceback.format_exc())
        sys.exit(1)

    # Fetch configuration with error handling
    try:
        minio_bucket = os.getenv("MINIO_CLIENT_GAMMA_STORAGE_BUCKET")
        chunk_size = int(os.getenv("CHUNK_SIZE", str(DEFAULT_CHUNK_SIZE)))
        SCRIPT_LOGGER.info("----- Configuration Details -----")
        SCRIPT_LOGGER.info("Loaded environment variables: MINIO_CLIENT_GAMMA_STORAGE_BUCKET=%s, CHUNK_SIZE=%d", minio_bucket, chunk_size)
        SCRIPT_LOGGER.info("--------------------------------")
    except KeyError as e:
        SCRIPT_LOGGER.error("Missing environment variable: %s\n%s", e, traceback.format_exc())
        sys.exit(1)
    except ValueError as e:
        SCRIPT_LOGGER.error("Invalid environment variable value: %s\n%s", e, traceback.format_exc())
        sys.exit(1)

    # Validate required environment variables
    required_vars = {
        "MINIO_CLIENT_GAMMA_STORAGE_BUCKET": minio_bucket
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
        support_categories = fetch_lookup_dictionary(
            conn=sqlite_connection,
            table_name="support_areas",
            source_name="CLIENT_GAMMA",
            column_name="support_area_name"
        )
        agent_pseudo_names = fetch_lookup_dictionary(
            conn=sqlite_connection,
            table_name="agents",
            source_name="CLIENT_GAMMA",
            column_name="pseudo_code"
        )
        customer_types = fetch_lookup_dictionary(
            conn=sqlite_connection,
            table_name="customer_types",
            source_name="CLIENT_GAMMA",
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

    # Initialize S3 client and get file count
    try:
        s3_client = get_s3_client()
        file_count = get_file_count(s3_client, minio_bucket)
        next_file_number = file_count + 1
    except Exception as e:
        SCRIPT_LOGGER.error("Failed to initialize S3 client or count files: %s\n%s", str(e), traceback.format_exc())
        close_database_connection(sqlite_connection)
        sys.exit(1)

    # Initialize from checkpoint
    try:
        SCRIPT_LOGGER.info("----- Initializing from Checkpoint -----")
        last_execution_info = get_last_execution_info(sqlite_connection, script_name)
        if last_execution_info is None:
            script_status = "NEW"
            max_serial_number = 0  # Not used in CSV schema
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
                    "Resuming from failed run: max_record_id=%d, retained update_records=%d, null_records=%d",
                    max_record_id, update_records, null_records
                )
            else:
                script_status = "PROCESSING"
                new_records = 0  # Reset for new run
                update_records = 0  # Reset for new run
                null_records = 0  # Reset for new run
                SCRIPT_LOGGER.info(
                    "Starting new run from checkpoint: max_record_id=%d",
                    max_record_id
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

    # Generate and write records
    total_new_records = 0
    total_update_records = 0
    total_null_records = 0
    total_chunks_processed = 0
    temp_files = []
    try:
        new_records, update_records, null_records, chunks_processed, max_record_id, temp_files = generate_and_update_records(
            support_categories=support_categories,
            agent_pseudo_names=agent_pseudo_names,
            customer_types=customer_types,
            start_record_id=max_record_id,
            num_records=num_records,
            s3_client=s3_client,
            bucket_name=minio_bucket,
            chunk_size=chunk_size,
            sqlite_connection=sqlite_connection,
            script_name=script_name,
            timestamp=timestamp,
            start_records_processed=start_records_processed
        )
        total_new_records += new_records
        total_update_records += update_records
        total_null_records += null_records
        total_chunks_processed += chunks_processed

        # Combine temporary compressed files into final compressed file
        if temp_files:
            final_file = combine_temp_files(s3_client, minio_bucket, temp_files, next_file_number, timestamp)
            SCRIPT_LOGGER.info("Final compressed file created: %s", final_file)
        else:
            SCRIPT_LOGGER.warning("No temporary compressed files generated; no final file created")

        checkpoint_management(
            sqlite_connection, script_name, "SUCCESS", 0, max_record_id,
            num_records, 0, 0, 0
        )
        SCRIPT_LOGGER.info("Successfully completed run, checkpoint updated to SUCCESS")
    except Exception as e:
        checkpoint_management(
            sqlite_connection, script_name, "FAILURE", 0, max_record_id,
            num_records, total_new_records, total_update_records, total_null_records
        )
        SCRIPT_LOGGER.error("Run failed, checkpoint updated to FAILURE: %s\n%s", str(e), traceback.format_exc())
        sys.exit(1)
    finally:
        close_database_connection(sqlite_connection)
        SCRIPT_LOGGER.info("Closed database connections")

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
    SCRIPT_LOGGER.info("Client Gamma Data Generator completed successfully")

if __name__ == "__main__":
    main()