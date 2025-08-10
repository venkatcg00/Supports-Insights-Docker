from typing import Optional
from datetime import datetime
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.hooks.base import BaseHook

pg_hook = PostgresHook(postgres_conn_id="postgres_project_connection")


def get_current_checkpoint(source: str) -> int:
    with pg_hook.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT COALESCE(MAX(source_checkpoint), 0)
                FROM aud.dag_runs A
                WHERE A.source_id = (
                    SELECT B.source_id FROM ds.sources B WHERE B.source_name = %s
                )
            """,
                (source, ),
            )
            result = cur.fetchone()
    return result[0] if result else 0


def make_dag_run_entry(airflow_dag_run_id: str, source: str,
                       dag_run_status: str) -> int:
    with pg_hook.get_conn() as conn:
        try:
            with conn.cursor() as cur:
                # Check if entry already exists
                cur.execute(
                    """
                    SELECT dag_run_id FROM aud.dag_runs
                    WHERE airflow_dag_run_id = %s
                """,
                    (airflow_dag_run_id, ),
                )
                existing = cur.fetchone()

                if existing:
                    return existing[0]

                # Insert if not exists
                cur.execute(
                    """
                    INSERT INTO aud.dag_runs (airflow_dag_run_id, source_id, dag_run_status)
                    VALUES (%s, (SELECT source_id FROM ds.sources WHERE source_name = %s), %s)
                    RETURNING dag_run_id
                """,
                    (airflow_dag_run_id, source, dag_run_status),
                )
                dag_run_id = cur.fetchone()[0]
                conn.commit()
                return dag_run_id

        except Exception:
            conn.rollback()
            raise


def get_source_id(source: str) -> int:
    with pg_hook.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT DISTINCT COALESCE(source_id, 0)
                FROM ds.sources
                WHERE source_name = %s
            """,
                (source, ),
            )
            result = cur.fetchone()
            return int(result[0]) if result else 0


def update_dag_run_status(dag_run_id: int, dag_run_status: str) -> bool:
    with pg_hook.get_conn() as conn:
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE aud.dag_runs
                    SET dag_run_status = %s
                    WHERE dag_run_id = %s
                """,
                    (dag_run_status, dag_run_id),
                )
                conn.commit()
                return True
        except Exception:
            conn.rollback()
            raise


def end_dag_run(
    dag_run_id: int,
    batch_count: int,
    insert_count: int,
    update_count: int,
    duplicate_count: int,
    valid_count: int,
    source_checkpoint: int,
) -> bool:
    with pg_hook.get_conn() as conn:
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT run_start_date FROM aud.dag_runs WHERE dag_run_id = %s",
                    (dag_run_id, ),
                )
                start_result = cur.fetchone()
                if not start_result or not start_result[0]:
                    return False

                run_start_time = start_result[0]
                run_end_time = datetime.now()
                run_duration = run_end_time - run_start_time
                validity_percentage = (round(
                    (valid_count / batch_count) *
                    100, 2) if batch_count > 0 else 0.0)
                dag_run_status = "SUCCESS"

                cur.execute(
                    """
                    UPDATE aud.dag_runs
                    SET run_end_date = %s,
                        dag_run_status = %s,
                        batch_count = %s,
                        insert_count = %s,
                        update_count = %s,
                        duplicate_count = %s,
                        valid_count = %s,
                        validity_percentage = %s,
                        run_duration = %s,
                        source_checkpoint = %s
                    WHERE dag_run_id = %s
                """,
                    (
                        run_end_time,
                        dag_run_status,
                        batch_count,
                        insert_count,
                        update_count,
                        duplicate_count,
                        valid_count,
                        validity_percentage,
                        run_duration,
                        source_checkpoint,
                        dag_run_id,
                    ),
                )
                conn.commit()
                return True
        except Exception:
            conn.rollback()
            raise


def fetch_lookup_dictionary(dict_type: str,
                            source: str) -> Optional[dict[int, str]]:
    queries = {
        "AGENT_DICT":
        """
            SELECT agent_id, pseudo_code
            FROM ds.agents
            WHERE source_id = (SELECT source_id FROM ds.sources WHERE source_name = %s)
        """,
        "CUST_DICT":
        """
            SELECT customer_type_id, customer_type_name
            FROM ds.customer_types
            WHERE source_id = (SELECT source_id FROM ds.sources WHERE source_name = %s)
        """,
        "SUPPORT_DICT":
        """
            SELECT support_area_id, support_area_name
            FROM ds.support_areas
            WHERE source_id = (SELECT source_id FROM ds.sources WHERE source_name = %s)
        """,
    }

    query = queries.get(dict_type)
    if not query:
        return {}

    with pg_hook.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(query, (source, ))
            result = cur.fetchall()
            return {key: value for key, value in result}


def get_postgres_jdbc_url():
    conn = BaseHook.get_connection("postgres_project_connection")
    return (
        f"jdbc:postgresql://{conn.host}:{conn.port}/{conn.schema}",
        conn.login,
        conn.password,
    )


def get_table_count(schema_name: str, table_name: str, condition: str = None):
    sql = f'SELECT COUNT(*) FROM "{schema_name}"."{table_name}"'
    if condition:
        sql += f" WHERE {condition}"
    return pg_hook.get_first(sql)[0]


def create_temp_table(schema_name: str, table_name: str, col_name: str,
                      data_type: str):
    pg_hook.run(f"""
        CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} (
            {col_name} {data_type}
        )
    """)


def get_data_counts(src_table: str, prs_table: str, src_id_column: str,
                    prs_id_column: str):
    counts_query = f"""
        SELECT
            COUNT(*) FILTER (WHERE PRS.{prs_id_column} IS NULL) AS insert_count,
            COUNT(*) FILTER (WHERE PRS.hash_key <> LND.hash_key AND PRS.{prs_id_column} IS NOT NULL) AS update_count,
            COUNT(*) FILTER (WHERE PRS.hash_key = LND.hash_key AND PRS.{prs_id_column} IS NOT NULL) AS duplicate_count
        FROM
            {src_table} LND
        LEFT JOIN
            {prs_table} PRS
            ON LND.{src_id_column} = PRS.{prs_id_column}
            AND PRS.is_active = TRUE
    """
    return pg_hook.get_first(counts_query)


def drop_temp_table(schema_name: str, table_name: str):
    pg_hook.run(f"DROP TABLE IF EXISTS {schema_name}.{table_name}")


def get_valid_count(source_id: int, dag_run_id: int):
    query = f"""
        SELECT COUNT(*) 
        FROM pre_dm.customer_support_stage 
        WHERE source_id = {source_id} AND dag_run_id = {dag_run_id} AND is_valid = TRUE
    """
    return pg_hook.get_first(query)[0]
