from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.hooks.base import BaseHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.exceptions import AirflowException, AirflowSkipException

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, md5, concat_ws, when, ceil, row_number
from pyspark.sql.window import Window
from datetime import timedelta, datetime
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
SOURCE = "CLIENT_ALPHA"
logger = logging.getLogger(__name__)
pg_hook = PostgresHook(postgres_conn_id="postgres_project_connection")
DEFAULT_ARGS = {
    "retries": 1,
    "retry_delay": timedelta(seconds=30),
    "execution_timeout": timedelta(hours=2)
}

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

def client_alpha_etl_dag():
    """Client Alpha ETL DAG with optimized Spark actions and memory usage."""
    with DAG(
        dag_id="Client_Alpha_ETL_Task_Flow",
        start_date=datetime.now() - timedelta(days=1),
        schedule="*/5 * * * *",
        catchup=False,
        default_args=DEFAULT_ARGS,
        tags=["client_alpha", "etl", "taskflow"],
        max_active_runs=1
    ) as dag:

        client_alpha_start = EmptyOperator(task_id="client_alpha_start")
        client_alpha_branch_b_end = EmptyOperator(task_id="client_alpha_branch_b_end")
        client_alpha_branch_a_start = EmptyOperator(task_id="client_alpha_branch_a_start")
        client_alpha_end = EmptyOperator(task_id="client_alpha_end", trigger_rule=TriggerRule.NONE_FAILED)

        @task(task_id="client_alpha_check_new_data")
        def client_alpha_check_new_data(ti=None):
            try:
                checkpoint = get_current_checkpoint(SOURCE)
                ti.xcom_push(key="checkpoint", value=checkpoint)
                logger.info(f"Retrieved checkpoint: {checkpoint}")

                with MongoHook(mongo_conn_id="mongo_project_connection") as mongo:
                    collection = mongo.get_collection("client_alpha_storage")
                    new_data = collection.find_one({"serial_number": {"$gt": checkpoint}})
                    signal = "Y" if new_data else "N"
                    ti.xcom_push(key="signal", value=signal)
                    logger.info(f"New data check result: {'Found' if new_data else 'Not found'}")
                return True
            except Exception as e:
                handle_task_error('NEW DATA CHECK', e)

        def client_alpha_branch_func(ti):
            signal = ti.xcom_pull(task_ids="client_alpha_check_new_data", key="signal")
            if signal not in ["Y", "N"]:
                raise AirflowException(f"Invalid signal value: {signal}")
            logger.info(f"Branching decision: {'client_alpha_branch_a_start' if signal == 'Y' else 'client_alpha_branch_b_end'}")
            return "client_alpha_branch_a_start" if signal == "Y" else "client_alpha_branch_b_end"

        client_alpha_branch = BranchPythonOperator(task_id="client_alpha_branch", python_callable=client_alpha_branch_func)

        @task(task_id="client_alpha_source_to_lnd")
        def client_alpha_source_to_lnd(ti=None):
            airflow_dag_run_id = f"{ti.dag_id} - {ti.run_id}"
            checkpoint = ti.xcom_pull(task_ids="client_alpha_check_new_data", key="checkpoint")
            dag_run_id = make_dag_run_entry(airflow_dag_run_id, SOURCE, "STARTED")
            ti.xcom_push(key="dag_run_id", value=dag_run_id)

            try:
                with MongoHook(mongo_conn_id="mongo_project_connection") as mongo:
                    collection = mongo.get_collection("client_alpha_storage")
                    new_data = list(collection.find({"serial_number": {"$gt": checkpoint}}))
            except Exception as e:
                    handle_task_error('SOURCE TO LND', e, dag_run_id)
            
            if not new_data:
                ti.xcom_push(key="batch_count", value=0)
                logger.info("No new data found, skipping to next task")
                raise AirflowSkipException("No new data to process")

            spark = get_spark_session("SOURCE TO LND")
            try:
                df = spark.createDataFrame(new_data)
                # Define columns for hash key
                hash_columns = [
                    "interaction_id", "support_category", "agent_pseudo_name", "contact_date",
                    "interaction_status", "interaction_type", "type_of_customer", "interaction_duration",
                    "total_time", "status_of_customer_incident", "resolved_in_first_contact", "solution_type", "rating"
                ]
                # Transform and deduplicate in one go
                df = (
                        df.withColumnRenamed("serial_number", "serial_number")
                        .withColumnRenamed("INTERACTION_ID", "interaction_id")
                        .withColumnRenamed("SUPPORT_CATEGORY", "support_category")
                        .withColumnRenamed("AGENT_PSEUDO_NAME", "agent_pseudo_name")
                        .withColumnRenamed("CONTACT_DATE", "contact_date")
                        .withColumnRenamed("INTERACTION_STATUS", "interaction_status")
                        .withColumnRenamed("INTERACTION_TYPE", "interaction_type")
                        .withColumnRenamed("TYPE_OF_CUSTOMER", "type_of_customer")
                        .withColumnRenamed("INTERACTION_DURATION", "interaction_duration")
                        .withColumnRenamed("TOTAL_TIME", "total_time")
                        .withColumnRenamed("STATUS_OF_CUSTOMER_INCIDENT", "status_of_customer_incident")
                        .withColumnRenamed("RESOLVED_IN_FIRST_CONTACT", "resolved_in_first_contact")
                        .withColumnRenamed("SOLUTION_TYPE", "solution_type")
                        .withColumnRenamed("RATING", "rating")
                        .withColumn("row_num", row_number().over(Window.partitionBy("interaction_id").orderBy(col("serial_number").desc())))
                        .filter(col("row_num") == 1)
                        .drop("row_num")
                        .withColumn("hash_key", md5(concat_ws("||", *[col(c).cast("string") for c in hash_columns])))
                        .select(
                            "serial_number", "interaction_id", "support_category", "agent_pseudo_name", "contact_date",
                            "interaction_status", "interaction_type", "type_of_customer", "interaction_duration",
                            "total_time", "status_of_customer_incident", "resolved_in_first_contact", "solution_type",
                            "rating", "hash_key"
                        )
                    )

                jdbc_url, user, password = get_postgres_jdbc_url()
                df.write.option("truncate", "true").format("jdbc") \
                    .option("url", jdbc_url) \
                    .option("dbtable", "lnd.client_alpha_cs_data") \
                    .option("user", user) \
                    .option("password", password) \
                    .option("driver", "org.postgresql.Driver") \
                    .mode("overwrite") \
                    .save()

                batch_count = get_table_count('lnd', 'client_alpha_cs_data')
                ti.xcom_push(key="batch_count", value=batch_count)
                new_checkpoint = pg_hook.get_first("SELECT COALESCE(MAX(serial_number), 0) FROM lnd.client_alpha_cs_data")[0]
                ti.xcom_push(key="new_checkpoint", value=new_checkpoint)
                update_dag_run_status(dag_run_id, "PROCESSING - LND TO PRS")
                return True
            except Exception as e:
                handle_task_error("SOURCE TO LND", e, dag_run_id)
            finally:
                spark.stop()

        @task(task_id="client_alpha_lnd_to_prs")
        def client_alpha_lnd_to_prs(ti=None):
            dag_run_id = ti.xcom_pull(task_ids="client_alpha_source_to_lnd", key="dag_run_id")
            source_id = get_source_id(SOURCE)
            ti.xcom_push(key="source_id", value=source_id)

            spark = get_spark_session("LND TO PRS")
            try:
                jdbc_url, user, password = get_postgres_jdbc_url()
                query = """
                    SELECT
                        LND.interaction_id, LND.support_category, LND.agent_pseudo_name,
                        TO_TIMESTAMP(LND.contact_date, 'DD/MM/YYYY HH24:MI:SS') AS contact_date, LND.interaction_status,
                        LND.interaction_type, LND.type_of_customer, LND.interaction_duration::INT AS interaction_duration,
                        LND.total_time::INT AS total_time, LND.status_of_customer_incident,
                        CASE WHEN LND.resolved_in_first_contact ILIKE 'YES' THEN TRUE
                             WHEN LND.resolved_in_first_contact ILIKE 'NO' THEN FALSE
                             ELSE NULL END AS resolved_in_first_contact,
                        LND.solution_type, LND.rating::INT AS rating, LND.hash_key AS lnd_hash_key,
                        PRS.prs_record_id, PRS.hash_key AS prs_hash_key,
                        CASE WHEN PRS.prs_record_id IS NULL THEN 'INSERT'
                             WHEN PRS.hash_key <> LND.hash_key AND PRS.prs_record_id IS NOT NULL THEN 'UPDATE'
                             WHEN PRS.hash_key = LND.hash_key AND PRS.prs_record_id IS NOT NULL THEN 'DUPLICATE'
                        END AS merge_type
                    FROM lnd.client_alpha_cs_data LND
                    LEFT JOIN prs.client_alpha_cs_data PRS
                        ON LND.interaction_id = PRS.interaction_id AND PRS.is_active = TRUE
                """
                df = spark.read.format("jdbc") \
                    .option("url", jdbc_url) \
                    .option("dbtable", f"({query}) as src") \
                    .option("user", user) \
                    .option("password", password) \
                    .option("driver", "org.postgresql.Driver") \
                    .load()

                insert_count, update_count, duplicate_count = get_data_counts(
                    'lnd.client_alpha_cs_data', 'prs.client_alpha_cs_data', 'interaction_id', 'interaction_id'
                )
                ti.xcom_push(key="insert_count", value=insert_count)
                ti.xcom_push(key="update_count", value=update_count)
                ti.xcom_push(key="duplicate_count", value=duplicate_count)

                df_filtered = df.filter(col("merge_type").isin("INSERT", "UPDATE"))
                create_temp_table('prs', 'client_alpha_temp', 'prs_record_id', 'INT')
                df_filtered.filter(col("merge_type") == "UPDATE").select("prs_record_id").write \
                    .format("jdbc") \
                    .option("url", jdbc_url) \
                    .option("dbtable", "prs.client_alpha_temp") \
                    .option("user", user) \
                    .option("password", password) \
                    .option("driver", "org.postgresql.Driver") \
                    .mode("overwrite") \
                    .save()

                pg_hook.run("""
                    UPDATE prs.client_alpha_cs_data AS main
                    SET is_active = FALSE, end_date = CURRENT_TIMESTAMP
                    FROM prs.client_alpha_temp AS tmp
                    WHERE main.is_active = TRUE AND main.prs_record_id = tmp.prs_record_id
                """)

                df_filtered.select(
                    lit(source_id).alias("source_id"), lit(dag_run_id).alias("dag_run_id"),
                    concat_ws("-", lit(SOURCE), col("interaction_id")).alias("source_system_identifier"),
                    col("interaction_id"), col("support_category"), col("agent_pseudo_name"), col("contact_date"),
                    col("interaction_status"), col("interaction_type"), col("type_of_customer"), col("interaction_duration"),
                    col("total_time"), col("status_of_customer_incident"), col("resolved_in_first_contact"),
                    col("solution_type"), col("rating"), col("lnd_hash_key").alias("hash_key")
                ).write.format("jdbc") \
                    .option("url", jdbc_url) \
                    .option("dbtable", "prs.client_alpha_cs_data") \
                    .option("user", user) \
                    .option("password", password) \
                    .option("driver", "org.postgresql.Driver") \
                    .mode("append") \
                    .save()

                drop_temp_table('prs', 'client_alpha_temp')
                update_dag_run_status(dag_run_id, "PROCESSING - PRS TO CDC")
                return True
            except Exception as e:
                handle_task_error("LND TO PRS", e, dag_run_id)
            finally:
                spark.stop()

        @task(task_id="client_alpha_prs_to_cdc")
        def client_alpha_prs_to_cdc(ti=None):
            dag_run_id = ti.xcom_pull(task_ids="client_alpha_source_to_lnd", key="dag_run_id")
            spark = get_spark_session("PRS TO CDC")
            try:
                jdbc_url, user, password = get_postgres_jdbc_url()
                query = f"""
                    SELECT
                        PRS.prs_record_id AS cdc_record_id, PRS.source_id, PRS.dag_run_id, PRS.source_system_identifier,
                        PRS.interaction_id, PRS.support_category, PRS.agent_pseudo_name, PRS.contact_date,
                        PRS.interaction_status, PRS.interaction_type, PRS.type_of_customer, PRS.interaction_duration,
                        PRS.total_time, PRS.status_of_customer_incident, PRS.resolved_in_first_contact,
                        PRS.solution_type, PRS.rating, PRS.start_date
                    FROM prs.client_alpha_cs_data PRS
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
                    .option("dbtable", "cdc.client_alpha_cs_data") \
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

        @task(task_id="client_alpha_cdc_to_predm")
        def client_alpha_cdc_to_predm(ti=None):
            dag_run_id = ti.xcom_pull(task_ids="client_alpha_source_to_lnd", key="dag_run_id")
            spark = get_spark_session("CDC TO PRE-DM")
            try:
                jdbc_url, user, password = get_postgres_jdbc_url()
                query = f"""
                    SELECT
                        cdc_record_id AS source_record_id, source_id, dag_run_id, source_system_identifier,
                        interaction_id, support_category, agent_pseudo_name, contact_date, interaction_status,
                        interaction_type, type_of_customer, interaction_duration, total_time, status_of_customer_incident,
                        resolved_in_first_contact, solution_type, rating, start_date
                    FROM cdc.client_alpha_cs_data
                    WHERE dag_run_id = {dag_run_id}
                """
                df = spark.read.format("jdbc") \
                    .option("url", jdbc_url) \
                    .option("dbtable", f"({query}) as src") \
                    .option("user", user) \
                    .option("password", password) \
                    .option("driver", "org.postgresql.Driver") \
                    .load()

                # Load lookup dictionaries and join in one transformation
                agent_dict = fetch_lookup_dictionary('AGENT_DICT', SOURCE)
                support_dict = fetch_lookup_dictionary('SUPPORT_DICT', SOURCE)
                cust_dict = fetch_lookup_dictionary('CUST_DICT', SOURCE)

                transformed_df = (df.join(spark.createDataFrame([(k, v) for k, v in agent_dict.items()], ["agent_id", "pseudo_code"]),
                                         df["agent_pseudo_name"] == col("pseudo_code"), "left").drop("pseudo_code")
                                 .join(spark.createDataFrame([(k, v) for k, v in support_dict.items()], ["support_area_id", "support_area_name"]),
                                       df["support_category"] == col("support_area_name"), "left").drop("support_area_name")
                                 .join(spark.createDataFrame([(k, v) for k, v in cust_dict.items()], ["customer_type_id", "customer_type_name"]),
                                       df["type_of_customer"] == col("customer_type_name"), "left").drop("customer_type_name")
                                 .select(
                                     col("source_id"), col("source_record_id"), col("source_system_identifier"),
                                     col("agent_id"), col("contact_date").alias("interaction_date"), col("support_area_id"),
                                     col("interaction_status"), col("interaction_type"), col("customer_type_id"),
                                     col("interaction_duration").alias("handle_time"),
                                     (col("total_time") - col("interaction_duration")).alias("work_time"),
                                     col("resolved_in_first_contact").alias("first_contact_resolution"),
                                     col("status_of_customer_incident").alias("query_status"),
                                     col("solution_type"), ceil(col("rating").cast("int") / 2).alias("customer_rating"),
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
                    .option("dbtable", "pre_dm.customer_support_stage_alpha") \
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

                create_temp_table('aud', 'data_error_temp_alpha', 'source_system_identifier', 'TEXT')
                invalid_records.select("source_system_identifier").write \
                    .format("jdbc") \
                    .option("url", jdbc_url) \
                    .option("dbtable", "aud.data_error_temp_alpha") \
                    .option("user", user) \
                    .option("password", password) \
                    .option("driver", "org.postgresql.Driver") \
                    .mode("overwrite") \
                    .save()

                pg_hook.run("""
                    UPDATE aud.data_error_history AS main
                    SET is_active = FALSE, end_date = CURRENT_TIMESTAMP
                    FROM aud.data_error_temp_alpha AS tmp
                    WHERE main.is_active = TRUE AND main.source_system_identifier = tmp.source_system_identifier
                """)
                drop_temp_table('aud', 'data_error_temp_alpha')

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

        @task(task_id="client_alpha_predm_to_dm")
        def client_alpha_predm_to_dm(ti=None):
            dag_run_id = ti.xcom_pull(task_ids="client_alpha_source_to_lnd", key="dag_run_id")
            source_id = ti.xcom_pull(task_ids="client_alpha_lnd_to_prs", key="source_id")
            spark = get_spark_session("PRE-DM TO DM")
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
                    FROM pre_dm.customer_support_stage_alpha PRE_DM
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

                create_temp_table('dm', 'temp_table_ca', 'source_system_identifier', 'TEXT')
                df.filter(col("merge_type") == "UPDATE").select("source_system_identifier").write \
                    .format("jdbc") \
                    .option("url", jdbc_url) \
                    .option("dbtable", "dm.temp_table_ca") \
                    .option("user", user) \
                    .option("password", password) \
                    .option("driver", "org.postgresql.Driver") \
                    .mode("overwrite") \
                    .save()

                pg_hook.run("""
                    UPDATE dm.customer_support_fact AS main
                    SET is_active = FALSE, end_date = CURRENT_TIMESTAMP
                    FROM dm.temp_table_ca AS tmp
                    WHERE main.is_active = TRUE AND main.source_system_identifier = tmp.source_system_identifier
                """)
                drop_temp_table('dm', 'temp_table_ca')

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

        @task(task_id="client_alpha_finalize_dag_run")
        def client_alpha_finalize_dag_run(ti=None):
            dag_run_id = ti.xcom_pull(task_ids="client_alpha_source_to_lnd", key="dag_run_id")
            try:
                batch_count = ti.xcom_pull(task_ids="client_alpha_source_to_lnd", key="batch_count") or 0
                insert_count = ti.xcom_pull(task_ids="client_alpha_lnd_to_prs", key="insert_count") or 0
                update_count = ti.xcom_pull(task_ids="client_alpha_lnd_to_prs", key="update_count") or 0
                duplicate_count = ti.xcom_pull(task_ids="client_alpha_lnd_to_prs", key="duplicate_count") or 0
                source_id = ti.xcom_pull(task_ids="client_alpha_lnd_to_prs", key="source_id")
                valid_count = get_valid_count(source_id, dag_run_id) or 0
                new_checkpoint = ti.xcom_pull(task_ids="client_alpha_source_to_lnd", key="new_checkpoint") or 0

                end_dag_run(
                    dag_run_id=dag_run_id, batch_count=batch_count, insert_count=insert_count,
                    update_count=update_count, duplicate_count=duplicate_count,
                    valid_count=valid_count, source_checkpoint=new_checkpoint
                )
                logger.info(f"DAG run finalized with ID: {dag_run_id}")
                return True
            except Exception as e:
                handle_task_error("FINALIZE DAG RUN", e, dag_run_id)

        client_alpha_check = client_alpha_check_new_data()
        client_alpha_lnd = client_alpha_source_to_lnd()
        client_alpha_prs = client_alpha_lnd_to_prs()
        client_alpha_cdc = client_alpha_prs_to_cdc()
        client_alpha_predm = client_alpha_cdc_to_predm()
        client_alpha_dm = client_alpha_predm_to_dm()
        client_alpha_finalize = client_alpha_finalize_dag_run()

        client_alpha_start >> client_alpha_check >> client_alpha_branch
        client_alpha_branch >> client_alpha_branch_a_start >> client_alpha_lnd >> client_alpha_prs >> client_alpha_cdc >> client_alpha_predm >> client_alpha_dm >> client_alpha_finalize >> client_alpha_end
        client_alpha_branch >> client_alpha_branch_b_end >> client_alpha_end

    return dag

client_alpha_etl_dag = client_alpha_etl_dag()