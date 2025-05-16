\c support_insights

-- Create tables in cdc schema for truncate and load
CREATE TABLE IF NOT EXISTS cdc.client_alpha_cs_data (
    cdc_record_id          INT,
    source_id              INT,
    dag_run_id             INT,
    source_system_identifier TEXT,
    source_record_id       INT,
    interaction_id         INT,
    support_category       TEXT,
    agent_pseudo_name      TEXT,
    contact_date           TIMESTAMP,
    interaction_status     TEXT,
    interaction_type       TEXT,
    type_of_customer       TEXT,
    interaction_duration   INT,
    total_time             INT,
    status_of_customer_incident TEXT,
    resolved_in_first_contact BOOLEAN,
    solution_type          TEXT,
    rating                 INTEGER,
    start_date             TIMESTAMP
);

CREATE TABLE IF NOT EXISTS cdc.client_beta_cs_data (
    cdc_record_id          INT,
    source_id              INT,
    dag_run_id             INT,
    source_system_identifier TEXT,
    support_identifier     INT,
    contact_regarding      TEXT,
    agent_code             TEXT,
    date_of_interaction    TIMESTAMP,
    status_of_interaction  TEXT,
    type_of_interaction    TEXT,
    customer_type          TEXT,
    contact_duration       INTERVAL,
    after_contact_work_time INTERVAL,
    incident_status        TEXT,
    first_contact_solve    BOOLEAN,
    type_of_resolution     TEXT,
    support_rating         INTEGER,
    time_stamp             TIMESTAMP,
    start_date             TIMESTAMP
);

CREATE TABLE IF NOT EXISTS cdc.client_gamma_cs_data (
    cdc_record_id          INT,
    source_id              INT,
    dag_run_id             INT,
    source_system_identifier TEXT,
    ticket_identifier      INT,
    support_category       TEXT,
    agent_name             TEXT,
    date_of_call           TIMESTAMP,
    call_status            TEXT,
    call_type              TEXT,
    type_of_customer       TEXT,
    duration               INT,
    work_time              INT,
    ticket_status          TEXT,
    resolved_in_first_contact BOOLEAN, 
    resolution_category    TEXT,
    rating                 TEXT,
    start_date             TIMESTAMP
);