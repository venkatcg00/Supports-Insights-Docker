import os
import sys
import uuid
import time
import random
import logging
import gc
import traceback
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Any
import argparse
import sqlite3
import json
import avro.schema
import avro.io
import io
from faker import Faker
from confluent_kafka import Producer, KafkaError
from db_operations import (
    connect_to_database, checkpoint_management,
    get_last_execution_info, close_database_connection
)

DEFAULT_CHUNK_SIZE = 100
SCRIPT_LOGGER = logging.getLogger("web_session_duration_generator")
SCRIPT_LOGGER.setLevel(logging.INFO)
script_name = "web_session_duration_generator"
log_file = os.getenv("SCRIPT_LOG_FILE", f"/app/logs/{script_name}_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.log")
formatter = logging.Formatter("timestamp=%(asctime)s level=%(levelname)s script=%(name)s message=%(message)s")
file_handler = logging.FileHandler(log_file)
file_handler.setFormatter(formatter)
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setFormatter(formatter)
SCRIPT_LOGGER.addHandler(file_handler)
SCRIPT_LOGGER.addHandler(console_handler)
DB_PATH = os.getenv("SQLITE_DB_PATH", "platform_metadata.db")

# Initialize Faker
fake = Faker()

# Load Avro schema
SCHEMA_FILE = os.path.join(os.path.dirname(__file__), "avro_schemas", "web_session_schema.avsc")
with open(SCHEMA_FILE, 'r') as f:
    RAW_SCHEMA = json.load(f)
    AVRO_SCHEMA = avro.schema.parse(json.dumps(RAW_SCHEMA))

def get_db_connection() -> sqlite3.Connection:
    """Connect to the SQLite database."""
    return sqlite3.connect(DB_PATH)

def pick_random_landing_page(conn: sqlite3.Connection) -> tuple:
    """Select a random landing page weighted by weight column."""
    query = """
    SELECT page_type, url_path
    FROM landing_pages
    WHERE weight > 0
    ORDER BY weight * RANDOM() DESC LIMIT 1;
    """
    return conn.execute(query).fetchone()

def pick_random_channel(conn: sqlite3.Connection) -> tuple:
    """Select a random channel weighted by weight column."""
    query = """
    SELECT channel, default_medium, default_source
    FROM channels
    WHERE weight > 0
    ORDER BY weight * RANDOM() DESC LIMIT 1;
    """
    return conn.execute(query).fetchone()

def pick_random_device(conn: sqlite3.Connection) -> tuple:
    """Select a random device weighted by weight column."""
    query = """
    SELECT device_type, os, browser
    FROM devices
    WHERE weight > 0
    ORDER BY weight * RANDOM() DESC LIMIT 1;
    """
    return conn.execute(query).fetchone()

def pick_random_price_bucket(conn: sqlite3.Connection) -> tuple:
    """Select a random price bucket."""
    query = """
    SELECT price_bucket, min_price_eur, max_price_eur
    FROM price_buckets
    ORDER BY RANDOM() LIMIT 1;
    """
    return conn.execute(query).fetchone()

def pick_random_cars(conn: sqlite3.Connection, num_cars: int = 3) -> List[tuple]:
    """Select random cars from car_catalog."""
    query = f"""
    SELECT brand, model, body_type, powertrain, monthly_price_eur
    FROM car_catalog
    ORDER BY RANDOM() LIMIT {num_cars};
    """
    return conn.execute(query).fetchall()

def pick_random_funnel_timing(conn: sqlite3.Connection) -> tuple:
    """Select a random funnel stage and its timing."""
    query = """
    SELECT stage, min_delay_sec, max_delay_sec
    FROM funnel_timings
    ORDER BY RANDOM() LIMIT 1;
    """
    return conn.execute(query).fetchone()

def pick_random_ab_variant(conn: sqlite3.Connection) -> tuple:
    """Select a random A/B variant weighted by weight column."""
    query = """
    SELECT experiment, variant
    FROM ab_variants
    WHERE weight > 0
    ORDER BY weight * RANDOM() DESC LIMIT 1;
    """
    return conn.execute(query).fetchone()

def generate_record(conn: sqlite3.Connection, record_id: int) -> Dict[str, Any]:
    """Generate a single web session record using database."""
    landing_page = pick_random_landing_page(conn)
    channel = pick_random_channel(conn)
    device = pick_random_device(conn)
    price_bucket = pick_random_price_bucket(conn)
    funnel_timing = pick_random_funnel_timing(conn)
    ab_variant = pick_random_ab_variant(conn)

    page_type, entry_url = landing_page
    channel_name, utm_medium, utm_source = channel
    device_type, os, browser = device
    price_bucket_name, min_price_eur, max_price_eur = price_bucket
    stage, min_delay_sec, max_delay_sec = funnel_timing
    experiment, ab_test_variant = ab_variant if ab_variant else ("none", "none")

    # Generate session timing
    started_at = datetime.now(timezone.utc) - timedelta(minutes=random.randint(0, 1440))
    duration_sec = random.randint(min_delay_sec, max_delay_sec)
    ended_at = started_at + timedelta(seconds=duration_sec)
    event_date = started_at.strftime("%Y-%m-%d")
    event_hour = started_at.hour

    # Generate user and session data
    user_id = fake.uuid4() if random.choice([True, False]) else None
    num_events = random.randint(1, 50)
    num_page_views = random.randint(1, num_events)
    num_car_views = random.randint(0, min(10, num_page_views))
    num_filter_apply = random.randint(0, 5)
    num_compare_add = random.randint(0, 3)
    num_plan_select = random.randint(0, 2)
    num_checkout_events = random.randint(0, 4)

    # Generate behavioral metrics
    avg_dwell_sec = round(random.uniform(5.0, 120.0), 1)
    p95_dwell_sec = round(avg_dwell_sec * random.uniform(1.5, 2.5), 1)
    max_scroll_pct = round(random.uniform(10.0, 100.0), 1)

    # Generate viewed car attributes
    cars_viewed = pick_random_cars(conn, num_car_views) if num_car_views > 0 else []
    brands_viewed = list(set(car[0] for car in cars_viewed))
    body_types_viewed = list(set(car[2] for car in cars_viewed))
    powertrains_viewed = list(set(car[3] for car in cars_viewed))

    # Funnel progression flags and timings
    viewed_car = num_car_views > 0
    used_filters = num_filter_apply > 0
    started_checkout = num_checkout_events > 0
    created_account = started_checkout and random.choice([True, False])
    selected_plan = num_plan_select > 0
    uploaded_docs = created_account and random.choice([True, False])
    passed_credit_check = uploaded_docs and random.choice([True, False])
    subscribed = passed_credit_check and random.choice([True, False])

    t_to_first_car_view_sec = random.randint(5, 60) if viewed_car else -1
    t_to_plan_select_sec = random.randint(20, 120) if selected_plan else -1
    t_to_checkout_start_sec = random.randint(30, 150) if started_checkout else -1
    t_to_credit_submit_sec = random.randint(60, 180) if passed_credit_check else -1
    t_to_subscribe_sec = random.randint(90, 240) if subscribed else -1

    return {
        "session_id": str(uuid.uuid4()),
        "user_id": user_id,
        "started_at": int(started_at.timestamp() * 1000),
        "ended_at": int(ended_at.timestamp() * 1000),
        "duration_sec": duration_sec,
        "event_date": event_date,
        "event_hour": event_hour,
        "channel": channel_name,
        "utm_source": utm_source,
        "utm_medium": utm_medium,
        "utm_campaign": fake.word() if random.choice([True, False]) else None,
        "ab_test_variant": ab_test_variant,
        "device_type": device_type,
        "os": os,
        "browser": browser,
        "country": random.choice(["DE", "FR", "ES", "IT", "NL", "BE", "SE", "DK", "PT", "AT"]),
        "region": fake.state() if random.choice([True, False]) else None,
        "city": fake.city() if random.choice([True, False]) else None,
        "entry_url": entry_url,
        "exit_url": random.choice([entry_url, "/checkout", "/cars", "/pricing", "/faq"]),
        "landing_page_type": page_type,
        "num_events": num_events,
        "num_page_views": num_page_views,
        "num_car_views": num_car_views,
        "num_filter_apply": num_filter_apply,
        "num_compare_add": num_compare_add,
        "num_plan_select": num_plan_select,
        "num_checkout_events": num_checkout_events,
        "avg_dwell_sec": avg_dwell_sec,
        "p95_dwell_sec": p95_dwell_sec,
        "max_scroll_pct": max_scroll_pct,
        "brands_viewed": brands_viewed,
        "body_types_viewed": body_types_viewed,
        "powertrains_viewed": powertrains_viewed,
        "price_bucket_clicked": price_bucket_name,
        "min_price_clicked_eur": float(min_price_eur) if min_price_eur else None,
        "max_price_clicked_eur": float(max_price_eur) if max_price_eur else None,
        "delivery_time_pref_days": random.randint(7, 30) if random.choice([True, False]) else None,
        "mileage_cap_pref_km": random.randint(10000, 30000) if random.choice([True, False]) else None,
        "viewed_car": viewed_car,
        "used_filters": used_filters,
        "started_checkout": started_checkout,
        "created_account": created_account,
        "selected_plan": selected_plan,
        "uploaded_docs": uploaded_docs,
        "passed_credit_check": passed_credit_check,
        "subscribed": subscribed,
        "t_to_first_car_view_sec": t_to_first_car_view_sec,
        "t_to_plan_select_sec": t_to_plan_select_sec,
        "t_to_checkout_start_sec": t_to_checkout_start_sec,
        "t_to_credit_submit_sec": t_to_credit_submit_sec,
        "t_to_subscribe_sec": t_to_subscribe_sec,
        "last_event_type": random.choice(["page_view", "click", "form_submit", "checkout"]) if num_events > 0 else None,
        "last_page_url": random.choice([entry_url, "/checkout", "/cars", "/pricing", "/faq"]) if num_events > 0 else None,
        "customer_type": random.choice(["New", "Returning"]),
        "generator_version": "1.0.0",
        "source_system": "synthetic-clickstream",
        "ingested_at": int(datetime.now(timezone.utc).timestamp() * 1000),
        "schema_version": "wsw-1"
    }

def encode_record_avro(record: Dict[str, Any]) -> bytes:
    """Encode a record using Avro schema."""
    buffer = io.BytesIO()
    encoder = avro.io.BinaryEncoder(buffer)
    writer = avro.io.DatumWriter(AVRO_SCHEMA)
    writer.write(record, encoder)
    return buffer.getvalue()

def produce_records_to_kafka(producer: Producer, topic: str, records: List[Dict[str, Any]]) -> None:
    """Produce a batch of records to Kafka."""
    try:
        for record in records:
            encoded = encode_record_avro(record)
            producer.produce(topic, encoded)
        producer.flush()
        SCRIPT_LOGGER.info("Flushed %d records to Kafka topic %s", len(records), topic)
    except KafkaError as e:
        SCRIPT_LOGGER.error("Failed to produce batch to Kafka topic %s: %s\n%s", topic, str(e), traceback.format_exc())
        raise

def generate_records(
    start_serial_number: int,
    start_record_id: int,
    num_records: int,
    producer: Producer,
    topic: str,
    chunk_size: int,
    sqlite_connection: sqlite3.Connection,
    script_name: str
) -> tuple[int, int, int, int, int, int]:
    """Generate and send web session records to Kafka in chunks."""
    serial_number = start_serial_number
    record_id = start_record_id
    new_records = 0
    chunks_processed = 0
    records_processed = 0
    records_to_process = num_records

    SCRIPT_LOGGER.info("Starting record generation: %d records, chunk size = %d", records_to_process, chunk_size)

    for chunk_start in range(0, records_to_process, chunk_size):
        chunk_end = min(chunk_start + chunk_size, records_to_process)
        chunk_records = []
        for _ in range(chunk_start, chunk_end):
            serial_number += 1
            record_id += 1
            chunk_records.append(generate_record(sqlite_connection, record_id))
            new_records += 1
            records_processed += 1

        produce_records_to_kafka(producer, topic, chunk_records)
        checkpoint_management(
            sqlite_connection, script_name, "PROCESSING", serial_number,
            record_id, num_records, new_records, 0, 0
        )
        chunks_processed += 1
        del chunk_records
        gc.collect()

    return new_records, 0, 0, chunks_processed, serial_number, record_id

def main():
    """Main function to generate and send web session records."""
    try:
        parser = argparse.ArgumentParser(description="Generate and send web session records to Kafka")
        parser.add_argument("input", nargs="?", default=None, type=int)
        args = parser.parse_args()
        num_records = args.input if args.input is not None else random.randint(1, 1000)
        if num_records <= 0:
            raise ValueError("Number of records must be positive")
    except Exception as e:
        SCRIPT_LOGGER.error("Argument parsing failed: %s\n%s", str(e), traceback.format_exc())
        sys.exit(1)

    try:
        kafka_bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
        kafka_topic_name = os.getenv("KAFKA_ANALYTICS_SESSION_DURATION_TOPIC")
        chunk_size = int(os.getenv("CHUNK_SIZE", str(DEFAULT_CHUNK_SIZE)))
    except Exception as e:
        SCRIPT_LOGGER.error("Failed to read environment variables: %s\n%s", str(e), traceback.format_exc())
        sys.exit(1)

    required = {
        "KAFKA_BOOTSTRAP_SERVERS": kafka_bootstrap_servers,
        "KAFKA_ANALYTICS_SESSION_DURATION_TOPIC": kafka_topic_name
    }
    missing = [k for k, v in required.items() if not v]
    if missing:
        SCRIPT_LOGGER.error("Missing required environment variables: %s", ", ".join(missing))
        sys.exit(1)

    try:
        sqlite_connection = connect_to_database()
    except Exception as e:
        SCRIPT_LOGGER.error("Failed to connect to database: %s\n%s", str(e), traceback.format_exc())
        sys.exit(1)

    try:
        producer = Producer({"bootstrap.servers": kafka_bootstrap_servers})
    except Exception as e:
        SCRIPT_LOGGER.error("Failed to initialize Kafka producer: %s\n%s", str(e), traceback.format_exc())
        close_database_connection(sqlite_connection)
        sys.exit(1)

    last_execution_info = get_last_execution_info(sqlite_connection, script_name)
    if last_execution_info is None:
        script_status = "NEW"
        max_serial_number = 0
        max_record_id = 0
    else:
        script_status, _, _, _, _, max_serial_number, max_record_id = last_execution_info
        script_status = "PROCESSING"

    checkpoint_management(sqlite_connection, script_name, script_status, max_serial_number, max_record_id, num_records, 0, 0, 0)
    new_records = 0

    try:
        new_records, _, _, chunks_processed, max_serial_number, max_record_id = generate_records(
            max_serial_number, max_record_id, num_records, producer, kafka_topic_name, chunk_size,
            sqlite_connection, script_name
        )
        checkpoint_management(sqlite_connection, script_name, "SUCCESS", max_serial_number, max_record_id, num_records, new_records, 0, 0)
        SCRIPT_LOGGER.info("Successfully generated %d records across %d chunks", new_records, chunks_processed)
    except Exception as e:
        checkpoint_management(sqlite_connection, script_name, "FAILURE", max_serial_number, max_record_id, num_records, new_records, 0, 0)
        SCRIPT_LOGGER.error("Record generation failed: %s\n%s", str(e), traceback.format_exc())
        sys.exit(1)
    finally:
        producer.flush()
        close_database_connection(sqlite_connection)

if __name__ == "__main__":
    main()