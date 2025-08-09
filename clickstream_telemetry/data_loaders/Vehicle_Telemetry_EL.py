from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.hooks.base import BaseHook

from datetime import datetime, timedelta
from confluent_kafka import Consumer, KafkaException, TopicPartition
import avro.schema, avro.io
import polars as pl
import io, json, logging
from collections import defaultdict

DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(seconds=30),
    "execution_timeout": timedelta(minutes=10),
}

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

with DAG(
    dag_id="vehicle_telemetry_kafka_to_minio",
    default_args=DEFAULT_ARGS,
    schedule="*/1 * * * *",
    catchup=False,
    max_active_runs=1,
    start_date=datetime(2024, 8, 3),
    tags=["kafka", "polars", "minio", "telemetry"],
) as dag:

    @task(task_id="fetch_kafka_avro_to_minio")
    def fetch_kafka_avro_to_minio():
        # --- Connections & config ---
        kafka_conn = BaseHook.get_connection("kafka_project_connection")
        kafka_bootstrap = f"{kafka_conn.host}:{kafka_conn.port}"

        # Use the variable names that actually exist
        topic = Variable.get("KAFKA_TELEMETRY_VEHICLE_STATS_TOPIC")
        bucket = Variable.get("MINIO_CLICKSTREAM_TELEMETRY_BUCKET")
        schema_path = Variable.get(
            "VEHICLE_TELEMETRY_SCHEMA_PATH",
            default_var="/opt/airflow/dags/vehicle_telemetry_stats_schema.avsc",
        )

        kafka_conf = {
            "bootstrap.servers": kafka_bootstrap,
            "group.id": "vehicle_telemetry_group",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,  # commit only after successful MinIO write
        }
        logger.info(f"Kafka bootstrap={kafka_bootstrap}, topic={topic}, bucket={bucket}")

        # --- Consumer & poll ---
        consumer = Consumer(kafka_conf)
        consumer.subscribe([topic])

        messages = []
        last_offsets = defaultdict(lambda: -1)  # partition -> last offset
        end_time = datetime.now() + timedelta(seconds=8)

        try:
            while datetime.now() < end_time:
                msg = consumer.poll(1.0)
                if msg is None:
                    continue
                if msg.error():
                    raise KafkaException(msg.error())
                messages.append(msg)
                last_offsets[msg.partition()] = max(last_offsets[msg.partition()], msg.offset())
        except KafkaException as e:
            consumer.close()
            logger.error(f"Kafka poll failed: {e}")
            raise

        if not messages:
            consumer.close()
            logger.info("No messages fetched in this batch.")
            return

        # --- Avro schema & decode ---
        try:
            with open(schema_path, "r") as f:
                schema_json = json.load(f)
            schema = avro.schema.parse(json.dumps(schema_json))
            datum_reader = avro.io.DatumReader(schema)
        except Exception as e:
            consumer.close()
            logger.error(f"Failed to load AVRO schema from {schema_path}: {e}")
            raise

        records = []
        for msg in messages:
            try:
                decoder = avro.io.BinaryDecoder(io.BytesIO(msg.value()))
                record = datum_reader.read(decoder)
                records.append(record)
            except Exception as e:
                logger.warning(f"Failed to decode message at offset {msg.offset()}: {e}")

        if not records:
            consumer.close()
            logger.info("No decodable records in this batch.")
            return

        # --- Polars DF & partition cols ---
        df = pl.DataFrame(records)
        now_utc = datetime.utcnow()
        df = df.with_columns(
            [
                pl.lit(now_utc.year).alias("year"),
                pl.lit(now_utc.month).alias("month"),
                pl.lit(now_utc.day).alias("day"),
                pl.lit(now_utc.hour).alias("hour"),
            ]
        )

        # --- Write to MinIO ---
        partition_path = (
            f"vehicle_telemetry/"
            f"year={now_utc.year}/month={now_utc.month:02d}/day={now_utc.day:02d}/hour={now_utc.hour:02d}"
        )
        filename = f"{partition_path}/telemetry_{now_utc.strftime('%Y%m%d%H%M%S')}.parquet"

        buf = io.BytesIO()
        df.write_parquet(buf)
        buf.seek(0)

        s3 = S3Hook(aws_conn_id="minio_project_connection")
        if not s3.check_for_bucket(bucket):
            s3.create_bucket(bucket_name=bucket)

        try:
            s3.load_file_obj(file_obj=buf, key=filename, bucket_name=bucket, replace=True)
        except Exception as e:
            consumer.close()
            logger.error(f"Failed to upload Parquet to s3://{bucket}/{filename}: {e}")
            raise

        # --- Commit offsets only after successful upload ---
        try:
            commit_tps = [TopicPartition(topic, p, o + 1) for p, o in last_offsets.items() if o >= 0]
            consumer.commit(offsets=commit_tps, asynchronous=False)
            logger.info(f"Committed offsets: {commit_tps}. Wrote {len(records)} rows to s3://{bucket}/{filename}")
        finally:
            consumer.close()

    fetch_kafka_avro_to_minio()
