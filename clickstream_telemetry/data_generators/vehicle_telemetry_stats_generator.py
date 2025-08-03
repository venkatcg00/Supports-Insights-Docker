import os
import sys
import uuid
import time
import random
import logging
import gc
import traceback
from datetime import datetime, timezone
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
SCRIPT_LOGGER = logging.getLogger("vehicle_telemetry_stats_generator")
SCRIPT_LOGGER.setLevel(logging.INFO)
script_name = "vehicle_telemetry_stats_generator"
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
SCHEMA_FILE = os.path.join(os.path.dirname(__file__), "avro_schemas", "vehicle_telemetry_stats_schema.avsc")
with open(SCHEMA_FILE, 'r') as f:
    RAW_SCHEMA = json.load(f)
    AVRO_SCHEMA = avro.schema.parse(json.dumps(RAW_SCHEMA))

# Vehicle specifications for realistic telemetry
VEHICLE_SPECS: List[Dict[str, Any]] = [
    {
        "make": "Volkswagen", "model": "ID.3 Pro", "year": 2023, "fuel_type": "Electric",
        "battery_capacity_kwh": 58.0, "range_km": 350.0, "energy_consumption_wh_per_km": 166.0,
        "power_kw": 150.0, "torque_nm": 310.0, "max_speed_kmph": 160.0, "co2_emission_gpkm": 0.0,
        "fuel_tank_capacity_l": None, "fuel_consumption_l_per_100km": None
    },
    {
        "make": "Peugeot", "model": "e-208", "year": 2024, "fuel_type": "Electric",
        "battery_capacity_kwh": 46.3, "range_km": 295.0, "energy_consumption_wh_per_km": 157.0,
        "power_kw": 100.0, "torque_nm": 260.0, "max_speed_kmph": 150.0, "co2_emission_gpkm": 0.0,
        "fuel_tank_capacity_l": None, "fuel_consumption_l_per_100km": None
    },
    {
        "make": "Peugeot", "model": "208", "year": 2013, "fuel_type": "Petrol",
        "battery_capacity_kwh": None, "fuel_tank_capacity_l": 45.0, "fuel_consumption_l_per_100km": 4.3,
        "range_km": 1046.5, "power_kw": 50.0, "torque_nm": 95.0, "max_speed_kmph": 163.0,
        "co2_emission_gpkm": 99.0, "energy_consumption_wh_per_km": None
    },
    {
        "make": "Toyota", "model": "Corolla Hybrid", "year": 2020, "fuel_type": "Hybrid",
        "battery_capacity_kwh": 1.3, "fuel_tank_capacity_l": 50.0, "fuel_consumption_l_per_100km": 4.5,
        "range_km": 1111.1, "power_kw": 90.0, "torque_nm": 142.0, "max_speed_kmph": 180.0,
        "co2_emission_gpkm": 100.0, "energy_consumption_wh_per_km": None
    }
]

def get_db_connection() -> sqlite3.Connection:
    """Connect to the SQLite database."""
    return sqlite3.connect(DB_PATH)

def pick_random_vehicle(conn: sqlite3.Connection) -> tuple:
    """Select a random vehicle from the vehicles table."""
    query = "SELECT * FROM vehicles ORDER BY RANDOM() LIMIT 1;"
    return conn.execute(query).fetchone()

def pick_random_city(conn: sqlite3.Connection) -> tuple:
    """Select a random city from the cities table."""
    query = "SELECT * FROM cities ORDER BY RANDOM() LIMIT 1;"
    return conn.execute(query).fetchone()

def generate_record(conn: sqlite3.Connection, record_id: int) -> Dict[str, Any]:
    """Generate a single telemetry record using database and vehicle specs."""
    vehicle = pick_random_vehicle(conn)
    city = pick_random_city(conn)
    
    vehicle_id, vin, make, model, variant, fuel_type, vehicle_type, license_plate, country, odometer_km, _ = vehicle
    city_name, country_name, lat, lon, weather = city
    weather_condition = random.choice(json.loads(weather.replace("'", '"')))
    
    # Match vehicle to a spec from VEHICLE_SPECS
    matching_specs = [spec for spec in VEHICLE_SPECS if spec["make"] == make and spec["model"].startswith(model.split()[0])]
    car = random.choice(matching_specs) if matching_specs else random.choice(VEHICLE_SPECS)
    
    now = datetime.now(timezone.utc)
    # Use database coordinates with small random offset (~5km)
    latitude = lat + random.uniform(-0.05, 0.05)
    longitude = lon + random.uniform(-0.05, 0.05)
    
    # Constrain speed to 80% of max speed
    max_speed = car.get("max_speed_kmph", 180.0) or 180.0
    speed_kmph = round(random.uniform(0, max_speed * 0.8), 1)
    
    # Calculate engine parameters based on fuel type
    if fuel_type.lower() == "electric":
        engine_rpm = int(speed_kmph * 25)  # Simulate electric motor RPM
        engine_temp_c = round(random.uniform(40, 90), 1)
        oil_pressure_psi = None
        coolant_temp_c = round(random.uniform(30, 80), 1)
        battery_charge_pct = round(random.uniform(20, 100), 1)
        fuel_level_pct = None
        fuel_range_km = round(car["range_km"] * (battery_charge_pct / 100.0), 1) if car["range_km"] else None
    else:
        idle_rpm = 700
        max_rpm = 6000
        engine_rpm = int(idle_rpm + (speed_kmph / max_speed) * (max_rpm - idle_rpm))
        engine_temp_c = round(random.uniform(70, 110), 1)
        oil_pressure_psi = round(random.uniform(20, 80), 1)
        coolant_temp_c = round(random.uniform(60, 120), 1)
        fuel_level_pct = round(random.uniform(20, 100), 1)
        battery_charge_pct = round(random.uniform(20, 100), 1) if fuel_type.lower() == "hybrid" else None
        fuel_range_km = round(car["range_km"] * (fuel_level_pct / 100.0), 1) if car.get("range_km") else None
    
    # CO2 emissions based on spec with slight variation
    base_emission = car.get("co2_emission_gpkm", 100.0)
    co2_emission_gpkm = 0.0 if fuel_type.lower() == "electric" else round(random.uniform(base_emission * 0.9, base_emission * 1.1), 1)
    
    return {
        "vehicle_id": vehicle_id,
        "vin": vin,
        "timestamp_utc": now.isoformat(),
        "make": make,
        "model": model,
        "variant": variant,
        "fuel_type": fuel_type,
        "vehicle_type": vehicle_type,
        "license_plate": license_plate,
        "latitude": float(latitude),
        "longitude": float(longitude),
        "city_name": city_name,
        "country_name": country_name,
        "region": country,
        "altitude": round(random.uniform(100.0, 1000.0), 2),
        "speed_kmph": speed_kmph,
        "acceleration_mps2": round(random.uniform(-3, 3), 2),
        "yaw_deg": round(random.uniform(-180, 180), 2),
        "pitch_deg": round(random.uniform(-30, 30), 2),
        "roll_deg": round(random.uniform(-45, 45), 2),
        "engine_rpm": engine_rpm,
        "engine_temp_c": engine_temp_c,
        "oil_pressure_psi": oil_pressure_psi,
        "coolant_temp_c": coolant_temp_c,
        "fuel_level_pct": fuel_level_pct,
        "fuel_range_km": fuel_range_km,
        "co2_emission_gpkm": co2_emission_gpkm,
        "battery_voltage_v": round(random.uniform(11.5, 14.8), 2) if fuel_type.lower() == "electric" else None,
        "battery_current_a": round(random.uniform(-50, 50), 1) if fuel_type.lower() == "electric" else None,
        "battery_charge_pct": battery_charge_pct,
        "battery_health_pct": round(random.uniform(70, 100), 1) if fuel_type.lower() == "electric" else None,
        "battery_temp_c": round(random.uniform(20, 60), 1) if fuel_type.lower() == "electric" else None,
        "cabin_temp_c": round(random.uniform(18, 30), 1),
        "humidity_pct": round(random.uniform(30, 70), 1),
        "ac_on": random.choice([True, False]),
        "fan_speed_level": random.randint(1, 5),
        "outside_temp_c": round(random.uniform(-10, 45), 1),
        "tyre_pressure_fl_psi": round(random.uniform(30, 36), 1),
        "tyre_pressure_fr_psi": round(random.uniform(30, 36), 1),
        "tyre_pressure_rl_psi": round(random.uniform(30, 36), 1),
        "tyre_pressure_rr_psi": round(random.uniform(30, 36), 1),
        "brake_pad_wear_pct": round(random.uniform(0, 100), 1),
        "abs_active": random.choice([True, False]),
        "airbag_deployed": random.choice([True, False]),
        "check_engine_light": random.choice([True, False]),
        "seatbelt_fastened": random.choice([True, False]),
        "stability_control_on": random.choice([True, False]),
        "media_playing": fake.word(),
        "volume_level": random.randint(0, 100),
        "bluetooth_connected": random.choice([True, False]),
        "navigation_active": random.choice([True, False]),
        "lane_assist_on": random.choice([True, False]),
        "radar_distance_m": round(random.uniform(0.5, 100.0), 2),
        "collision_warning": random.choice([True, False]),
        "auto_braking_enabled": random.choice([True, False]),
        "blind_spot_monitor_on": random.choice([True, False]),
        "error_code_1": fake.lexify(text='ERR????'),
        "error_code_2": fake.lexify(text='ERR????'),
        "ecu_status": random.choice(["OK", "WARNING", "FAILURE"]),
        "firmware_version": fake.numerify(text="v#.##.###"),
        "odometer_km": round(odometer_km + random.uniform(0, 2), 1),
        "trip_duration_sec": random.randint(60, 7200),
        "gear_position": random.choice(["P", "R", "N", "D", "S"]),
        "parking_brake_on": random.choice([True, False]),
        "driver_fatigue_detected": random.choice([True, False]),
        "weather_condition": weather_condition
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
    """Generate and send telemetry records to Kafka in chunks."""
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
    """Main function to generate and send vehicle telemetry records."""
    try:
        parser = argparse.ArgumentParser(description="Generate and send telemetry records to Kafka")
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
        kafka_topic_name = os.getenv("KAFKA_TELEMETRY_VEHICLE_STATS_TOPIC")
        chunk_size = int(os.getenv("CHUNK_SIZE", str(DEFAULT_CHUNK_SIZE)))
    except Exception as e:
        SCRIPT_LOGGER.error("Failed to read environment variables: %s\n%s", str(e), traceback.format_exc())
        sys.exit(1)

    required = {"KAFKA_BOOTSTRAP_SERVERS": kafka_bootstrap_servers, "KAFKA_TELEMETRY_VEHICLE_STATS_TOPIC": kafka_topic_name}
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