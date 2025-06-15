from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.hooks.base import BaseHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import Variable
from airflow.exceptions import AirflowException, AirflowSkipException

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, md5, concat_ws, when, ceil, row_number, split
from pyspark.sql.window import Window
from datetime import timedelta, datetime
from time import time
from confluent_kafka.admin import AdminClient
from confluent_kafka import TopicPartition, Consumer
import xml.etree.ElementTree as ET
import logging
import os
import sys

sys.path.append(os.path.dirname(__file__))
from db_dag_operations import (
    get_current_checkpoint, update_dag_run_status, end_dag_run, make_dag_run_entry,
    fetch_lookup_dictionary, get_source_id, get_postgres_jdbc_url,
    get_table_count, get_data_counts, create_temp_table, drop_temp_table, get_valid_count
)

# Constants
SOURCE = "CLIENT_BETA"
logger = logging.getLogger(__name__)
pg_hook = PostgresHook(postgres_conn_id="postgres_project_connection")
DEFAULT_ARGS = {
    "retries": 1,
    "retry_delay": timedelta(seconds=30),
    "execution_timeout": timedelta(hours=2)
}

def safe_text(root, tag):
    val = root.findtext(tag)
    return val if val and val.strip() != "" else None

def get_spark_session(app_name: str) -> SparkSession:
    """Create a Spark session optimized for local container with memory constraints."""
    try:
        conn = BaseHook.get_connection("spark_project_connection")
        spark = (SparkSession.builder \
            .appName(app_name) \
            .master(f"spark://{conn.host}:{conn.port}") \
            .config("spark.jars", "/opt/spark/jars/postgresql-jdbc.jar") \
            .config("spark.driver.extraJavaOptions", "-Dlog4j.configurationFile=file:/opt/spark/conf/log4j2.properties")
            .config("spark.executor.extraJavaOptions", "-Dlog4j.configurationFile=file:/opt/spark/conf/log4j2.properties")
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel("INFO")
        return spark
    except Exception as e:
        logger.error(f"Spark session creation failed: {str(e)}")
        raise AirflowException(f"Spark session creation failed: {str(e)}")

def handle_task_error(task_name: str, error: Exception, dag_run_id: str = None):
    """Centralized error handling for tasks."""
    logger.error(f"{task_name} failed: {str(error)}")
    if dag_run_id:
        update_dag_run_status(dag_run_id, f"FAILED - {task_name}")
    raise AirflowException(f"{task_name} failed: {str(error)}")

def client_beta_etl_dag():
    """Client Beta ETL DAG with optimized Spark actions and memory usage."""
    with DAG(
        dag_id="Client_Beta_ETL_Task_Flow",
        start_date=datetime.now() - timedelta(days=1),
        schedule="*/5 * * * *",
        catchup=False,
        default_args=DEFAULT_ARGS,
        tags=["client_beta", "etl", "taskflow"],
        max_active_runs=1
    ) as dag:

        client_beta_start = EmptyOperator(task_id="client_beta_start")
        client_beta_branch_b_end = EmptyOperator(task_id="client_beta_branch_b_end")
        client_beta_branch_a_start = EmptyOperator(task_id="client_beta_branch_a_start")
        client_beta_end = EmptyOperator(task_id="client_beta_end", trigger_rule=TriggerRule.NONE_FAILED)

        @task(task_id="client_beta_check_new_data")
        def client_beta_check_new_data(ti=None):
            try:
                checkpoint = get_current_checkpoint(SOURCE)
                ti.xcom_push(key="checkpoint", value=checkpoint)

                topic = Variable.get("KAFKA_CLIENT_BETA_STORAGE_TOPIC")
                conn = BaseHook.get_connection("kafka_project_connection")
                bootstrap = conn.host + (f":{conn.port}" if conn.port else "")
                kafka_conf = {"bootstrap.servers": bootstrap, "group.id": "offset_check", "enable.auto.commit": False}
                consumer = Consumer(kafka_conf)
                tp = TopicPartition(topic, 0)
                high_offset = consumer.get_watermark_offsets(tp)[1]
                consumer.close()

                signal = "Y" if high_offset > (checkpoint + 10) else "N"
                ti.xcom_push(key="signal", value=signal)
                logger.info(f"Checkpoint={checkpoint}, Latest={high_offset}, New={signal}")
                return True
            except Exception as e:
                handle_task_error("NEW DATA CHECK", e)

        def client_beta_branch_func(ti):
            signal = ti.xcom_pull(task_ids="client_beta_check_new_data", key="signal")
            if signal not in ["Y", "N"]:
                raise AirflowException(f"Invalid signal value: {signal}")
            logger.info(f"Branching decision: {'client_beta_branch_a_start' if signal == 'Y' else 'client_beta_branch_b_end'}")
            return "client_beta_branch_a_start" if signal == "Y" else "client_beta_branch_b_end"

        client_beta_branch = BranchPythonOperator(task_id="client_beta_branch", python_callable=client_beta_branch_func)

        @task(task_id="client_beta_source_to_lnd")
        def client_beta_source_to_lnd(ti=None):
            airflow_dag_run_id = f"{ti.dag_id} - {ti.run_id}"
            checkpoint = ti.xcom_pull(task_ids="client_beta_check_new_data", key="checkpoint")
            dag_run_id = make_dag_run_entry(airflow_dag_run_id, SOURCE, "STARTED")
            ti.xcom_push(key="dag_run_id", value=dag_run_id)

            topic = Variable.get("KAFKA_CLIENT_BETA_STORAGE_TOPIC")
            conn = BaseHook.get_connection("kafka_project_connection")
            bootstrap = conn.host + (f":{conn.port}" if conn.port else "")
            kafka_conf = {
                "bootstrap.servers": bootstrap,
                "group.id": f"{SOURCE}_loader",
                "enable.auto.commit": False,
                "auto.offset.reset": "earliest"
            }

            consumer = Consumer(kafka_conf)
            consumer.subscribe([topic])
            new_records = []

            try:
                logger.info(f"Fetching Kafka messages from offset > {checkpoint} for topic '{topic}'")
                start_time = time()
                max_wait = 10  # seconds

                while True:
                    msg = consumer.poll(1.0)
                    if msg is None:
                        if time() - start_time > max_wait:
                            break
                        continue
                    if msg.error():
                        logger.warning(f"Kafka error: {msg.error()}")
                        continue
                    if msg.offset() <= checkpoint:
                        continue

                    try:
                        xml_str = msg.value().decode("utf-8")
                        root = ET.fromstring(xml_str)
                        new_records.append({
                            "support_identifier": safe_text(root, "SUPPORT_IDENTIFIER"),
                            "contact_regarding": safe_text(root, "CONTACT_REGARDING"),
                            "agent_code": safe_text(root, "AGENT_CODE"),
                            "date_of_interaction": safe_text(root, "DATE_OF_INTERACTION"),
                            "status_of_interaction": safe_text(root, "STATUS_OF_INTERACTION"),
                            "type_of_interaction": safe_text(root, "TYPE_OF_INTERACTION"),
                            "customer_type": safe_text(root, "CUSTOMER_TYPE"),
                            "contact_duration": safe_text(root, "CONTACT_DURATION"),
                            "after_contact_work_time": safe_text(root, "AFTER_CONTACT_WORK_TIME"),
                            "incident_status": safe_text(root, "INCIDENT_STATUS"),
                            "first_contact_solve": safe_text(root, "FIRST_CONTACT_SOLVE"),
                            "type_of_resolution": safe_text(root, "TYPE_OF_RESOLUTION"),
                            "support_rating": safe_text(root, "SUPPORT_RATING"),
                            "time_stamp": safe_text(root, "TIME_STAMP"),
                            "kafka_offset": msg.offset()
                        })
                    except Exception as e:
                        handle_task_error('SOURCE TO LND', e, dag_run_id)

            finally:
                consumer.close()

            if not new_records:
                ti.xcom_push(key="batch_count", value=0)
                logger.info("No new data found in Kafka")
                raise AirflowSkipException("No new data to process")

            spark = get_spark_session("SOURCE TO LND")
            try:
                df = spark.createDataFrame(new_records)

                new_checkpoint = df.agg({"kafka_offset": "max"}).collect()[0][0]
                ti.xcom_push(key="new_checkpoint", value=new_checkpoint)

                hash_columns = [
                    "support_identifier", "contact_regarding", "agent_code", "date_of_interaction",
                    "status_of_interaction", "type_of_interaction", "customer_type",
                    "contact_duration", "after_contact_work_time", "incident_status",
                    "first_contact_solve", "type_of_resolution", "support_rating", "time_stamp"
                ]

                df = (
                    df.withColumn("hash_key", md5(concat_ws("||", *[col(c).cast("string") for c in hash_columns])))
                    .withColumn("row_num", row_number().over(Window.partitionBy("support_identifier").orderBy(col("time_stamp").cast("timestamp").desc())))
                    .filter(col("row_num") == 1)
                    .drop("row_num")
                    .select(col("support_identifier").cast("integer"), *hash_columns[1:], "hash_key", "kafka_offset")
                )

                jdbc_url, user, password = get_postgres_jdbc_url()

                df.drop("kafka_offset").write.option("truncate", "true").format("jdbc") \
                    .option("url", jdbc_url) \
                    .option("dbtable", "lnd.client_beta_cs_data") \
                    .option("user", user) \
                    .option("password", password) \
                    .option("driver", "org.postgresql.Driver") \
                    .mode("overwrite") \
                    .save()

                batch_count = df.count()
                ti.xcom_push(key="batch_count", value=batch_count)
                
                update_dag_run_status(dag_run_id, "PROCESSING - LND TO PRS")

                return True
            except Exception as e:
                handle_task_error("SOURCE TO LND", e, dag_run_id)
            finally:
                spark.stop()

        @task(task_id="client_beta_lnd_to_prs")
        def client_beta_lnd_to_prs(ti=None):
            dag_run_id = ti.xcom_pull(task_ids="client_beta_source_to_lnd", key="dag_run_id")
            source_id = get_source_id(SOURCE)
            ti.xcom_push(key="source_id", value=source_id)

            spark = get_spark_session("lnd To PRS")
            try:
                jdbc_url, user, password = get_postgres_jdbc_url()
                query = """
                    SELECT
                        LND.support_identifier, LND.contact_regarding, LND.agent_code,
                        TO_TIMESTAMP(LND.date_of_interaction, 'YYYYMMDDHH24MISS') AS date_of_interaction, LND.status_of_interaction,
                        LND.type_of_interaction, LND.customer_type, LND.contact_duration::INTERVAL AS contact_duration,
                        LND.after_contact_work_time::INTERVAL AS after_contact_work_time, LND.incident_status,
                        CASE WHEN LND.first_contact_solve ILIKE 'TRUE' THEN TRUE
                            WHEN LND.first_contact_solve ILIKE 'FALSE' THEN FALSE
                            ELSE NULL END AS first_contact_solve,
                        LND.type_of_resolution, LND.support_rating::INT AS support_rating, LND.hash_key AS lnd_hash_key,
                        PRS.prs_record_id, PRS.hash_key AS prs_hash_key,
                        CASE WHEN PRS.prs_record_id IS NULL THEN 'INSERT'
                            WHEN PRS.hash_key <> LND.hash_key AND PRS.prs_record_id IS NOT NULL THEN 'UPDATE'
                            WHEN PRS.hash_key = LND.hash_key AND PRS.prs_record_id IS NOT NULL THEN 'DUPLICATE'
                        END AS merge_type
                    FROM lnd.client_beta_cs_data LND
                    LEFT JOIN prs.client_beta_cs_data PRS
                        ON LND.support_identifier = PRS.support_identifier AND PRS.is_active = TRUE
                """
                df = spark.read.format("jdbc") \
                    .option("url", jdbc_url) \
                    .option("dbtable", f"({query}) as src") \
                    .option("user", user) \
                    .option("password", password) \
                    .option("driver", "org.postgresql.Driver") \
                    .load()

                insert_count, update_count, duplicate_count = get_data_counts(
                    'lnd.client_beta_cs_data', 'prs.client_beta_cs_data', 'support_identifier', 'support_identifier'
                )
                ti.xcom_push(key="insert_count", value=insert_count)
                ti.xcom_push(key="update_count", value=update_count)
                ti.xcom_push(key="duplicate_count", value=duplicate_count)

                df_filtered = df.filter(col("merge_type").isin("INSERT", "UPDATE"))
                create_temp_table('prs', 'client_beta_temp', 'prs_record_id', 'INT')
                df_filtered.filter(col("merge_type") == "UPDATE").select("prs_record_id").write \
                    .format("jdbc") \
                    .option("url", jdbc_url) \
                    .option("dbtable", "prs.client_beta_temp") \
                    .option("user", user) \
                    .option("password", password) \
                    .option("driver", "org.postgresql.Driver") \
                    .mode("append") \
                    .save()

                pg_hook.run("""
                    UPDATE prs.client_beta_cs_data AS main
                    SET is_active = FALSE, end_date = CURRENT_TIMESTAMP
                    FROM prs.client_beta_temp AS tmp
                    WHERE main.is_active = TRUE AND main.prs_record_id = tmp.prs_record_id
                """)

                df_filtered.select(
                    lit(source_id).alias("source_id"), lit(dag_run_id).alias("dag_run_id"),
                    concat_ws("-", lit(SOURCE), col("support_identifier")).alias("source_system_identifier"),
                    col("support_identifier"), col("contact_regarding"), col("agent_code"), col("date_of_interaction"),
                    col("status_of_interaction"), col("type_of_interaction"), col("customer_type"), col("contact_duration"),
                    col("after_contact_work_time"), col("incident_status"), col("first_contact_solve"),
                    col("type_of_resolution"), col("support_rating"), col("lnd_hash_key").alias("hash_key")
                ).write.format("jdbc") \
                    .option("url", jdbc_url) \
                    .option("dbtable", "prs.client_beta_cs_data") \
                    .option("user", user) \
                    .option("password", password) \
                    .option("driver", "org.postgresql.Driver") \
                    .mode("append") \
                    .save()

                drop_temp_table('prs', 'client_beta_temp')
                update_dag_run_status(dag_run_id, "PROCESSING - PRS TO CDC")
                return True
            except Exception as e:
                handle_task_error("LND TO PRS", e, dag_run_id)
            finally:
                spark.stop()

        @task(task_id="client_beta_prs_to_cdc")
        def client_beta_prs_to_cdc(ti=None):
            dag_run_id = ti.xcom_pull(task_ids="client_beta_source_to_lnd", key="dag_run_id")
            spark = get_spark_session("PRS To CDC")
            try:
                jdbc_url, user, password = get_postgres_jdbc_url()
                query = f"""
                    SELECT
                        PRS.prs_record_id AS cdc_record_id, PRS.source_id, PRS.dag_run_id, PRS.source_system_identifier,
                        PRS.support_identifier, PRS.contact_regarding, PRS.agent_code, PRS.date_of_interaction,
                        PRS.status_of_interaction, PRS.type_of_interaction, PRS.customer_type, PRS.contact_duration,
                        PRS.after_contact_work_time, PRS.incident_status, PRS.first_contact_solve,
                        PRS.type_of_resolution, PRS.support_rating, PRS.start_date
                    FROM prs.client_beta_cs_data PRS
                    WHERE PRS.is_active = TRUE AND PRS.dag_run_id = {dag_run_id}
                """
                df = spark.read.format("jdbc") \
                    .option("url", jdbc_url) \
                    .option("dbtable", f"({query}) as src") \
                    .option("user", user) \
                    .option("password", password) \
                    .option("driver", "org.postgresql.Driver") \
                    .load()

                df.write.option("truncate", "true").format("jdbc") \
                    .option("url", jdbc_url) \
                    .option("dbtable", "cdc.client_beta_cs_data") \
                    .option("user", user) \
                    .option("password", password) \
                    .option("driver", "org.postgresql.Driver") \
                    .mode("append") \
                    .save()

                update_dag_run_status(dag_run_id, "PROCESSING - CDC TO PRE-DM")
                return True
            except Exception as e:
                handle_task_error("PRS TO CDC", e, dag_run_id)
            finally:
                spark.stop()

        @task(task_id="client_beta_cdc_to_predm")
        def client_beta_cdc_to_predm(ti=None):
            dag_run_id = ti.xcom_pull(task_ids="client_beta_source_to_lnd", key="dag_run_id")
            spark = get_spark_session("CDC to PRE-DM")
            try:
                jdbc_url, user, password = get_postgres_jdbc_url()
                query = f"""
                    SELECT
                        cdc_record_id AS source_record_id, source_id, dag_run_id, source_system_identifier,
                        support_identifier, contact_regarding, agent_code, date_of_interaction, status_of_interaction,
                        type_of_interaction, customer_type, contact_duration, after_contact_work_time, incident_status,
                        first_contact_solve, type_of_resolution, support_rating, start_date
                    FROM cdc.client_beta_cs_data
                    WHERE dag_run_id = {dag_run_id}
                """
                df = spark.read.format("jdbc") \
                    .option("url", jdbc_url) \
                    .option("dbtable", f"({query}) as src") \
                    .option("user", user) \
                    .option("password", password) \
                    .option("driver", "org.postgresql.Driver") \
                    .load()
                
                df = df.withColumn("handle_time_parts", split(col("contact_duration"), ":")) \
                        .withColumn("handle_time_int",
                            col("handle_time_parts")[0].cast("int") * 3600 +
                            col("handle_time_parts")[1].cast("int") * 60 +
                            col("handle_time_parts")[2].cast("int")) \
                        .drop("handle_time_parts") \
                        .withColumnRenamed("handle_time_int", "handle_time") \
                        .withColumn("work_time_parts", split(col("after_contact_work_time"), ":")) \
                        .withColumn("work_time_int",
                                    col("work_time_parts")[0].cast("int") * 3600 +
                                    col("work_time_parts")[1].cast("int") * 60 +
                                    col("work_time_parts")[2].cast("int")) \
                        .drop("work_time_parts") \
                        .withColumnRenamed("work_time_int", "work_time")

                # Load lookup dictionaries and join in one transformation
                agent_dict = fetch_lookup_dictionary('AGENT_DICT', SOURCE)
                support_dict = fetch_lookup_dictionary('SUPPORT_DICT', SOURCE)
                cust_dict = fetch_lookup_dictionary('CUST_DICT', SOURCE)

                transformed_df = (df.join(spark.createDataFrame([(k, v) for k, v in agent_dict.items()], ["agent_id", "pseudo_code"]),
                                         df["agent_code"] == col("pseudo_code"), "left").drop("pseudo_code")
                                 .join(spark.createDataFrame([(k, v) for k, v in support_dict.items()], ["support_area_id", "support_area_name"]),
                                       df["contact_regarding"] == col("support_area_name"), "left").drop("support_area_name")
                                 .join(spark.createDataFrame([(k, v) for k, v in cust_dict.items()], ["customer_type_id", "customer_type_name"]),
                                       df["customer_type"] == col("customer_type_name"), "left").drop("customer_type_name")
                                 .select(
                                     col("source_id"), col("source_record_id"), col("source_system_identifier"),
                                     col("agent_id"), col("date_of_interaction").alias("interaction_date"), col("support_area_id"),
                                     col("status_of_interaction").alias("interaction_status"), col("type_of_interaction").alias("interaction_type"), 
                                     col("customer_type_id"), col("handle_time"),
                                     col("work_time"),
                                     col("first_contact_solve").alias("first_contact_resolution"),
                                     col("incident_status").alias("query_status"),
                                     col("type_of_resolution").alias("solution_type"), col("support_rating").alias("customer_rating"),
                                     col("dag_run_id")
                                 ))

                valid_check_df = transformed_df.withColumn("is_valid",
                    when(
                        col("agent_id").isNull() | col("interaction_date").isNull() | col("support_area_id").isNull() |
                        col("interaction_status").isNull() | col("interaction_type").isNull() | col("customer_type_id").isNull() |
                        col("handle_time").isNull() | col("work_time").isNull() | col("first_contact_resolution").isNull() |
                        col("query_status").isNull() | col("solution_type").isNull() | col("customer_rating").isNull(),
                        False
                    ).otherwise(True))

                valid_check_df.write.option("truncate", "true").format("jdbc") \
                    .option("url", jdbc_url) \
                    .option("dbtable", "pre_dm.customer_support_stage_beta") \
                    .option("user", user) \
                    .option("password", password) \
                    .option("driver", "org.postgresql.Driver") \
                    .mode("append") \
                    .save()

                invalid_records = valid_check_df.filter(col("is_valid") == False).select(
                    col("dag_run_id"), col("source_id"), lit("INVALID").alias("error_type"),
                    lit("A REQUIRED COLUMN IS NULL").alias("error_description"),
                    col("source_system_identifier"), col("source_record_id")
                )

                create_temp_table('aud', 'data_error_temp_beta', 'source_system_identifier', 'TEXT')
                invalid_records.select("source_system_identifier").write \
                    .format("jdbc") \
                    .option("url", jdbc_url) \
                    .option("dbtable", "aud.data_error_temp_beta") \
                    .option("user", user) \
                    .option("password", password) \
                    .option("driver", "org.postgresql.Driver") \
                    .mode("append") \
                    .save()

                pg_hook.run("""
                    UPDATE aud.data_error_history AS main
                    SET is_active = FALSE, end_date = CURRENT_TIMESTAMP
                    FROM aud.data_error_temp_beta AS tmp
                    WHERE main.is_active = TRUE AND main.source_system_identifier = tmp.source_system_identifier
                """)
                drop_temp_table('aud', 'data_error_temp_beta')

                invalid_records.write.format("jdbc") \
                    .option("url", jdbc_url) \
                    .option("dbtable", "aud.data_error_history") \
                    .option("user", user) \
                    .option("password", password) \
                    .option("driver", "org.postgresql.Driver") \
                    .mode("append") \
                    .save()

                update_dag_run_status(dag_run_id, "PROCESSING - PRE-DM TO DM")
                return True
            except Exception as e:
                handle_task_error("CDC TO PRE-DM", e, dag_run_id)
            finally:
                spark.stop()

        @task(task_id="client_beta_predm_to_dm")
        def client_beta_predm_to_dm(ti=None):
            dag_run_id = ti.xcom_pull(task_ids="client_beta_source_to_lnd", key="dag_run_id")
            source_id = ti.xcom_pull(task_ids="client_beta_lnd_to_prs", key="source_id")
            spark = get_spark_session("PRE-DM to DM")
            try:
                jdbc_url, user, password = get_postgres_jdbc_url()
                query = f"""
                    SELECT
                        PRE_DM.source_id, PRE_DM.source_record_id, PRE_DM.source_system_identifier,
                        PRE_DM.agent_id, PRE_DM.interaction_date, PRE_DM.support_area_id,
                        PRE_DM.interaction_status, PRE_DM.interaction_type, PRE_DM.customer_type_id,
                        PRE_DM.handle_time, PRE_DM.work_time, PRE_DM.first_contact_resolution,
                        PRE_DM.query_status, PRE_DM.solution_type, PRE_DM.customer_rating,
                        PRE_DM.dag_run_id, PRE_DM.is_valid,
                        CASE WHEN DM.source_system_identifier IS NOT NULL THEN 'UPDATE' ELSE 'INSERT' END AS merge_type
                    FROM pre_dm.customer_support_stage_beta PRE_DM
                    LEFT JOIN dm.customer_support_fact DM
                        ON PRE_DM.source_system_identifier = DM.source_system_identifier AND DM.is_active = TRUE
                    WHERE PRE_DM.dag_run_id = {dag_run_id} AND PRE_DM.source_id = {source_id}
                """
                df = spark.read.format("jdbc") \
                    .option("url", jdbc_url) \
                    .option("dbtable", f"({query}) as src") \
                    .option("user", user) \
                    .option("password", password) \
                    .option("driver", "org.postgresql.Driver") \
                    .load()

                create_temp_table('dm', 'temp_table', 'source_system_identifier', 'TEXT')
                df.filter(col("merge_type") == "UPDATE").select("source_system_identifier").write \
                    .format("jdbc") \
                    .option("url", jdbc_url) \
                    .option("dbtable", "dm.temp_table") \
                    .option("user", user) \
                    .option("password", password) \
                    .option("driver", "org.postgresql.Driver") \
                    .mode("append") \
                    .save()

                pg_hook.run("""
                    UPDATE dm.customer_support_fact AS main
                    SET is_active = FALSE, end_date = CURRENT_TIMESTAMP
                    FROM dm.temp_table AS tmp
                    WHERE main.is_active = TRUE AND main.source_system_identifier = tmp.source_system_identifier
                """)
                drop_temp_table('dm', 'temp_table')

                df.select(
                    "source_id", "source_record_id", "source_system_identifier",
                    "agent_id", "interaction_date", "support_area_id",
                    "interaction_status", "interaction_type", "customer_type_id",
                    "handle_time", "work_time", "first_contact_resolution",
                    "query_status", "solution_type", "customer_rating",
                    "dag_run_id", "is_valid"
                ).write.format("jdbc") \
                    .option("url", jdbc_url) \
                    .option("dbtable", "dm.customer_support_fact") \
                    .option("user", user) \
                    .option("password", password) \
                    .option("driver", "org.postgresql.Driver") \
                    .mode("append") \
                    .save()

                update_dag_run_status(dag_run_id, "PROCESSING - ALL LOADING STAGES COMPLETED")
                return True
            except Exception as e:
                handle_task_error("PRE-DM TO DM", e, dag_run_id)
            finally:
                spark.stop()

        @task(task_id="client_beta_finalize_dag_run")
        def client_beta_finalize_dag_run(ti=None):
            dag_run_id = ti.xcom_pull(task_ids="client_beta_source_to_lnd", key="dag_run_id")
            try:
                batch_count = ti.xcom_pull(task_ids="client_beta_source_to_lnd", key="batch_count") or 0
                insert_count = ti.xcom_pull(task_ids="client_beta_lnd_to_prs", key="insert_count") or 0
                update_count = ti.xcom_pull(task_ids="client_beta_lnd_to_prs", key="update_count") or 0
                duplicate_count = ti.xcom_pull(task_ids="client_beta_lnd_to_prs", key="duplicate_count") or 0
                source_id = ti.xcom_pull(task_ids="client_beta_lnd_to_prs", key="source_id")
                valid_count = get_valid_count(source_id, dag_run_id) or 0
                new_checkpoint = ti.xcom_pull(task_ids="client_beta_source_to_lnd", key="new_checkpoint") or 0

                end_dag_run(
                    dag_run_id=dag_run_id, batch_count=batch_count, insert_count=insert_count,
                    update_count=update_count, duplicate_count=duplicate_count,
                    valid_count=valid_count, source_checkpoint=new_checkpoint
                )
                logger.info(f"DAG run finalized with ID: {dag_run_id}")
                return True
            except Exception as e:
                handle_task_error("FINALIZE DAG RUN", e, dag_run_id)

        client_beta_check = client_beta_check_new_data()
        client_beta_lnd = client_beta_source_to_lnd()
        client_beta_prs = client_beta_lnd_to_prs()
        client_beta_cdc = client_beta_prs_to_cdc()
        client_beta_predm = client_beta_cdc_to_predm()
        client_beta_dm = client_beta_predm_to_dm()
        client_beta_finalize = client_beta_finalize_dag_run()

        client_beta_start >> client_beta_check >> client_beta_branch
        client_beta_branch >> client_beta_branch_a_start >> client_beta_lnd >> client_beta_prs >> client_beta_cdc >> client_beta_predm >> client_beta_dm >> client_beta_finalize >> client_beta_end
        client_beta_branch >> client_beta_branch_b_end >> client_beta_end

    return dag

client_beta_etl_dag = client_beta_etl_dag()