\c support_insights

CREATE OR REPLACE VIEW vw.data_metrics_view AS
SELECT 
    DR.dag_run_id,
    S.source_name,
    DR.run_start_date,
    COALESCE(DR.batch_count, 0) AS batch_count,
    COALESCE(DR.insert_count, 0) AS insert_count,
    COALESCE(DR.update_count, 0) AS update_count,
    COALESCE(DR.duplicate_count, 0) AS duplicate_count,
    COALESCE(DR.valid_count, 0) AS valid_count,
    COALESCE(DR.validity_percentage, 0.0) AS data_quality_score,
    COALESCE(EXTRACT(EPOCH FROM DR.run_duration), 0) AS run_duration_sec
FROM 
    AUD.dag_runs DR
    JOIN DS.sources S 
        ON DR.source_id = S.source_id
WHERE 
    DR.dag_run_status = 'SUCCESS'
ORDER BY 
    DR.run_start_date DESC;

CREATE OR REPLACE VIEW vw.customer_support_fact_view AS
SELECT
    s.source_name,
    csf.source_record_id,
    csf.source_system_identifier,
    a.pseudo_code,
    csf.interaction_date,
    sa.support_area_name,
    csf.interaction_status,
    csf.interaction_type,
    ct.customer_type_name,
    csf.hANDle_time,
    csf.work_time,
    csf.first_contact_resolution,
    csf.query_status,
    csf.solution_type,
    csf.customer_rating
FROM
    dm.customer_support_fact csf 
    JOIN ds.sources s 
        ON csf.source_id = s.source_id
    LEFT JOIN ds.agents a 
        ON csf.agent_id = a.agent_id
    LEFT JOIN ds.support_areas sa 
        ON csf.support_area_id = sa.support_area_id
    LEFT JOIN ds.customer_types ct 
        ON csf.customer_type_id  = ct.customer_type_id
WHERE 
        csf.is_active = true 
    AND csf.is_valid = true
    AND NOW() BETWEEN csf.start_date  AND csf.end_date;