\c support_insights

-- Create partitioned table in pre_dm schema for truncate and load
CREATE TABLE IF NOT EXISTS pre_dm.customer_support_stage (
    source_id              INT,
    source_record_id       INT,
    source_system_identifier TEXT,
    agent_id               INT,
    interaction_date       TIMESTAMP,
    support_area_id        INT,
    interaction_status     TEXT,
    interaction_type       TEXT,
    customer_type_id       INT,
    handle_time            INT,
    work_time              INT,
    first_contact_resolution BOOLEAN,
    query_status           TEXT,
    solution_type          TEXT,
    customer_rating        INTEGER,
    dag_run_id             INT,
    is_valid               BOOLEAN,
    start_date              TIMESTAMP
) PARTITION BY LIST (source_id);

-- Create partitions for specific source_id values
CREATE TABLE IF NOT EXISTS pre_dm.customer_support_stage_alpha
    PARTITION OF pre_dm.customer_support_stage
    FOR VALUES IN (1);

CREATE TABLE IF NOT EXISTS pre_dm.customer_support_stage_beta
    PARTITION OF pre_dm.customer_support_stage
    FOR VALUES IN (2);

CREATE TABLE IF NOT EXISTS pre_dm.customer_support_stage_gamma
    PARTITION OF pre_dm.customer_support_stage
    FOR VALUES IN (3);

-- Create default partition for other unexpected values
CREATE TABLE IF NOT EXISTS pre_dm.customer_support_stage_default
    PARTITION OF pre_dm.customer_support_stage
    DEFAULT; -- Handles source_id = 0, 4, NULL, etc.