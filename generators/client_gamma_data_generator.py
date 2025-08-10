"""
Client Gamma Data Generator (MinIO/CSV.GZ, chunk-per-file)

Generates randomized support records for CLIENT_GAMMA:
- Reads allowed values from SQLite (lookup tables)
- Resumes/records progress via checkpoint table
- Writes EACH CHUNK as a final compressed CSV (.csv.gz) to MinIO
- Supports chunked writes and retrying DB connections

Designed to run under the Data Generator Orchestrator.
"""

from __future__ import annotations

import argparse
import csv
import gzip
import logging
import os
import random
import sqlite3
import sys
import traceback
from datetime import datetime, timezone, timedelta
from io import StringIO, BytesIO
from typing import List, Union, Tuple

import boto3
from botocore.exceptions import ClientError, BotoCoreError

from db_operations import (
    connect_to_database,
    fetch_lookup_list,
    checkpoint_management,
    get_last_execution_info,
    close_database_connection,
)

# ========================== Config & Logging =========================== #

DEFAULT_CHUNK_SIZE = 1000
SCRIPT_NAME = "client_gamma_data_generator"

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

# ============================== MinIO connect ============================== #


def _s3_client() -> boto3.client:
    need = {
        k: os.getenv(k)
        for k in ("MINIO_HOST", "MINIO_PORT", "MINIO_USER", "MINIO_PASSWORD")
    }
    miss = [k for k, v in need.items() if not v]
    if miss:
        raise ValueError(
            f"Missing required MinIO environment variables: {', '.join(miss)}")
    endpoint = f"http://{need['MINIO_HOST']}:{need['MINIO_PORT']}"
    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=need["MINIO_USER"],
        aws_secret_access_key=need["MINIO_PASSWORD"],
    )


def _s3_list_gamma_files(s3, bucket: str) -> int:
    try:
        resp = s3.list_objects_v2(Bucket=bucket)
        if "Contents" not in resp or not resp["Contents"]:
            return 0
        return sum(1 for o in resp["Contents"]
                   if "client_gamma_support_data" in o["Key"])
    except (ClientError, BotoCoreError):
        return 0


def _s3_put_object(s3, bucket: str, key: str, body: bytes) -> None:
    for attempt in range(1, 4):
        try:
            s3.put_object(
                Bucket=bucket,
                Key=key,
                Body=body,
                ContentType="text/csv",
                ContentEncoding="gzip",
            )
            return
        except (ClientError, BotoCoreError) as e:
            if attempt == 3:
                SCRIPT_LOGGER.error(
                    "Failed to upload %s/%s: %s\n%s",
                    bucket,
                    key,
                    str(e),
                    traceback.format_exc(),
                )
                raise
            SCRIPT_LOGGER.warning(
                "Upload failed (attempt %d/3) for %s. Retrying…", attempt, key)


# ============================ CSV ============================== #

_HEADERS = [
    "TICKET_IDENTIFIER",
    "SUPPORT_CATEGORY",
    "AGENT_NAME",
    "DATE_OF_CALL",
    "CALL_STATUS",
    "CALL_TYPE",
    "TYPE_OF_CUSTOMER",
    "DURATION",
    "WORK_TIME",
    "TICKET_STATUS",
    "RESOLVED_IN_FIRST_CONTACT",
    "RESOLUTION_CATEGORY",
    "RATING",
]


def write_chunk_file_to_minio(
    s3_client: boto3.client,
    bucket_name: str,
    chunk_data: List[List[Union[str, None]]],
    sequence_number: int,
    timestamp: str,
) -> str:
    """
    Write a final compressed CSV for this chunk (no temp, no merging).
    Key format: '{sequence_number:10d}_client_gamma_support_data_{timestamp}.csv.gz'
    """
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    key = f"{sequence_number:10d}_client_gamma_support_data_{timestamp}.csv.gz"
    csv_buffer = StringIO()
    writer = csv.writer(csv_buffer, delimiter="|", quoting=csv.QUOTE_ALL)
    writer.writerow(_HEADERS)
    writer.writerows([[str(v) if v is not None else "" for v in row]
                      for row in chunk_data])

    compressed = BytesIO()
    with gzip.GzipFile(fileobj=compressed, mode="wb") as gz:
        gz.write(csv_buffer.getvalue().encode("utf-8"))
    data = compressed.getvalue()

    _s3_put_object(s3_client, bucket_name, key, data)
    csv_buffer.close()
    compressed.close()
    SCRIPT_LOGGER.info("Uploaded chunk file: %s/%s", bucket_name, key)
    return key


# ============================ Record Gen ============================== #


def generate_random_record(
    record_id: int,
    support_categories: List[str],
    agent_pseudo_names: List[str],
    customer_types: List[str],
) -> List[Union[str, None]]:
    duration = random.randint(20, 600)
    work_time = random.randint(10, max(10, duration - 10))
    record = [
        str(record_id),
        random.choice(support_categories),
        random.choice(agent_pseudo_names),
        (datetime.now(timezone.utc) -
         timedelta(days=random.randint(0, 1000))).strftime("%m%d%Y%H%M%S"),
        random.choice(["COMPLETED", "DROPPED", "TRANSFERRED"]),
        random.choice(["CALL", "CHAT", "EMAIL"]),
        random.choice(customer_types),
        str(duration),
        str(work_time),
        random.choice([
            "RESOLVED",
            "PENDING RESOLUTION",
            "WORK IN PROGRESS",
            "TRANSFERRED TO ANOTHER QUEUE",
        ]),
        str(random.choice([1, 0])),
        random.choice(["SELF-HELP OPTION", "SUPPORT TEAM INTERVENTION"]),
        random.choice(["WORST", "BAD", "NEUTRAL", "GOOD", "BEST"]),
    ]

    return record


# =========================== Core Processing =========================== #


def generate_and_upload_chunks(
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
    sequence_number: int,
    start_records_processed: int = 0,
) -> Tuple[int, int, int, int, int, List[str]]:
    """
    Generate records in chunks and upload EACH CHUNK as a final .csv.gz file.
    Returns:
        (new_records, update_records, null_records, chunks_processed, final_record_id, keys_written)
    """
    record_id = start_record_id
    new_records = update_records = null_records = 0
    records_processed = start_records_processed
    chunks_processed = 0
    keys_written: List[str] = []

    if num_records <= 0:
        SCRIPT_LOGGER.warning(
            "Requested num_records <= 0 (%d); nothing to do.", num_records)
        return 0, 0, 0, 0, record_id, keys_written

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
            record_id,
            keys_written,
        )

    total_chunks = (records_to_process + chunk_size - 1) // chunk_size
    SCRIPT_LOGGER.info(
        "Starting generation: to_process=%d chunk_size=%d",
        records_to_process,
        chunk_size,
    )

    for chunk_start in range(0, records_to_process, chunk_size):
        chunk_end = min(chunk_start + chunk_size, records_to_process)
        chunk_records = chunk_end - chunk_start
        highest_record_id = record_id
        chunk_data: List[List[Union[str, None]]] = []
        chunk_update_records = 0
        chunk_null_records = 0

        SCRIPT_LOGGER.info(
            "----- Processing chunk %d/%d: %d records (records %d to %d) -----",
            chunks_processed + 1,
            total_chunks,
            chunk_records,
            records_processed + 1,
            records_processed + chunk_records,
        )

        for _ in range(chunk_records):
            record_id += 1
            highest_record_id = record_id

            rec = generate_random_record(record_id, support_categories,
                                         agent_pseudo_names, customer_types)

            if random.random() < 0.1:
                idx_to_nullify = random.randint(1,
                                                12)  # keep identifier intact
                rec[idx_to_nullify] = None
                null_records += 1
                chunk_null_records += 1

            chunk_data.append(rec)
            new_records += 1
            records_processed += 1

            if random.random() < 0.25 and highest_record_id > 1:
                upd_id = random.randint(1, highest_record_id)
                upd = generate_random_record(upd_id, support_categories,
                                             agent_pseudo_names,
                                             customer_types)
                chunk_data.append(upd)
                update_records += 1
                chunk_update_records += 1

        # Upload this chunk as final file (no merge step)
        key = write_chunk_file_to_minio(
            s3_client,
            bucket_name,
            chunk_data,
            sequence_number=sequence_number,
            timestamp=timestamp,
        )
        keys_written.append(key)
        sequence_number = sequence_number + 1

        SCRIPT_LOGGER.info(
            "Chunk %d: generated=%d pseudo_updates=%d nulls_in_chunk=%d",
            chunks_processed + 1,
            chunk_records,
            chunk_update_records,
            chunk_null_records,
        )

        # Checkpoint after each chunk
        checkpoint_management(
            sqlite_connection,
            script_name,
            "PROCESSING",
            0,  # serial not used for CSV flavor
            record_id,
            num_records,
            new_records,
            update_records,
            null_records,
        )
        SCRIPT_LOGGER.info(
            "Checkpoint updated (chunk %d): new=%d upd=%d null=%d max_record_id=%d",
            chunks_processed + 1,
            new_records,
            update_records,
            null_records,
            record_id,
        )

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
        record_id,
        keys_written,
    )


# ============================== Main Flow ============================== #


def _parse_args() -> int:
    p = argparse.ArgumentParser(
        description=
        "Generate and upload client_gamma support records per chunk.")
    p.add_argument(
        "input",
        nargs="?",
        default=None,
        type=int,
        help="Number of records (default: random 1..10000)",
    )
    a = p.parse_args()
    return a.input if a.input is not None else random.randint(1, 10_000)


def _require_env() -> Tuple[str, int]:
    bucket = os.getenv("MINIO_CLIENT_GAMMA_STORAGE_BUCKET")
    if not bucket:
        SCRIPT_LOGGER.error(
            "Missing required environment variables: MINIO_CLIENT_GAMMA_STORAGE_BUCKET"
        )
        sys.exit(1)
    try:
        chunk_size = int(os.getenv("CHUNK_SIZE", str(DEFAULT_CHUNK_SIZE)))
        if chunk_size <= 0:
            raise ValueError
    except ValueError:
        SCRIPT_LOGGER.warning("Invalid CHUNK_SIZE; using default %d",
                              DEFAULT_CHUNK_SIZE)
        chunk_size = DEFAULT_CHUNK_SIZE
    return bucket, chunk_size


def _validate_lookups(*lists: List[str]) -> None:
    names = ["support_categories", "agent_pseudo_names", "customer_types"]
    missing = [n for n, lst in zip(names, lists) if not lst]
    if missing:
        raise RuntimeError(f"Lookup lists are empty: {', '.join(missing)}")


def main() -> None:
    start_time = datetime.now(timezone.utc)
    timestamp = start_time.strftime("%Y%m%d_%H%M%S")

    # args
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

    # env
    bucket, chunk_size = _require_env()
    SCRIPT_LOGGER.info(
        "----- Configuration -----\nBUCKET=%s CHUNK_SIZE=%d\n-------------------------",
        bucket,
        chunk_size,
    )

    # SQLite
    try:
        sqlite_connection = connect_to_database()
        SCRIPT_LOGGER.info("Connected to SQLite: %s",
                           os.getenv("SQLITE_DB_FILE"))
    except Exception as e:
        SCRIPT_LOGGER.error("Failed to connect to SQLite: %s\n%s", str(e),
                            traceback.format_exc())
        sys.exit(1)

    # lookups
    try:
        support_categories = fetch_lookup_list(sqlite_connection,
                                               "support_areas", "CLIENT_GAMMA",
                                               "support_area_name")
        agent_pseudo_names = fetch_lookup_list(sqlite_connection, "agents",
                                               "CLIENT_GAMMA", "pseudo_code")
        customer_types = fetch_lookup_list(sqlite_connection, "customer_types",
                                           "CLIENT_GAMMA",
                                           "customer_type_name")
        _validate_lookups(support_categories, agent_pseudo_names,
                          customer_types)
        SCRIPT_LOGGER.info(
            "Lookups: support=%d agents=%d customers=%d",
            len(support_categories),
            len(agent_pseudo_names),
            len(customer_types),
        )
    except Exception as e:
        SCRIPT_LOGGER.error("Failed to fetch/validate lookups: %s\n%s", str(e),
                            traceback.format_exc())
        close_database_connection(sqlite_connection)
        sys.exit(1)

    # minio client + sequence number
    try:
        s3_client = _s3_client()
        file_count = _s3_list_gamma_files(s3_client, bucket)
        sequence_number = file_count + 1
    except Exception as e:
        SCRIPT_LOGGER.error(
            "Failed to init MinIO client or count files: %s\n%s",
            str(e),
            traceback.format_exc(),
        )
        close_database_connection(sqlite_connection)
        sys.exit(1)

    # checkpoint init
    try:
        last = get_last_execution_info(sqlite_connection, SCRIPT_NAME)
        if last is None:
            script_status = "NEW"
            max_serial_number = 0
            max_record_id = 0
            start_processed = 0
            new_records = update_records = null_records = 0
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
            start_processed = 0
            if script_status == "FAILURE":
                script_status = "RE-PROCESSING"
                num_records = processing_count
            else:
                script_status = "PROCESSING"
                new_records = update_records = null_records = 0

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
    except Exception as e:
        SCRIPT_LOGGER.error("Failed to initialize checkpoint: %s\n%s", str(e),
                            traceback.format_exc())
        close_database_connection(sqlite_connection)
        sys.exit(1)

    # generate & upload chunks (no merge)
    total_new = total_upd = total_null = total_chunks = 0
    try:
        new, upd, nnull, chunks, max_record_id, keys = generate_and_upload_chunks(
            support_categories=support_categories,
            agent_pseudo_names=agent_pseudo_names,
            customer_types=customer_types,
            start_record_id=max_record_id,
            num_records=num_records,
            s3_client=s3_client,
            bucket_name=bucket,
            chunk_size=chunk_size,
            sqlite_connection=sqlite_connection,
            script_name=SCRIPT_NAME,
            timestamp=timestamp,
            sequence_number=sequence_number,
            start_records_processed=start_processed,
        )
        total_new += new
        total_upd += upd
        total_null += nnull
        total_chunks += chunks

        checkpoint_management(
            sqlite_connection,
            SCRIPT_NAME,
            "SUCCESS",
            0,
            max_record_id,
            num_records,
            0,
            0,
            0,
        )
        SCRIPT_LOGGER.info("Run completed — checkpoint set to SUCCESS")
        SCRIPT_LOGGER.info("Chunk files written: %s",
                           ", ".join(keys) if keys else "(none)")
    except Exception as e:
        checkpoint_management(
            sqlite_connection,
            SCRIPT_NAME,
            "FAILURE",
            0,
            max_record_id,
            num_records,
            total_new,
            total_upd,
            total_null,
        )
        SCRIPT_LOGGER.error(
            "Run failed — checkpoint set to FAILURE: %s\n%s",
            str(e),
            traceback.format_exc(),
        )
        sys.exit(1)
    finally:
        close_database_connection(sqlite_connection)

    # summary
    runtime = (datetime.now(timezone.utc) - start_time).total_seconds()
    SCRIPT_LOGGER.info("----- Run Summary -----")
    SCRIPT_LOGGER.info("Total records requested: %d", num_records)
    SCRIPT_LOGGER.info("Processed (new+updates): %d", total_new + total_upd)
    SCRIPT_LOGGER.info("New: %d | Updates: %d | Nulls: %d", total_new,
                       total_upd, total_null)
    SCRIPT_LOGGER.info("Chunks written: %d", total_chunks)
    SCRIPT_LOGGER.info("Runtime (seconds): %.2f", runtime)
    SCRIPT_LOGGER.info("-----------------------")
    SCRIPT_LOGGER.info("Client Gamma Data Generator completed successfully")


if __name__ == "__main__":
    main()
