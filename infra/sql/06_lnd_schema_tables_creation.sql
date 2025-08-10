\c support_insights

-- Create tables in lnd schema for truncate and load
CREATE TABLE IF NOT EXISTS lnd.client_alpha_cs_data (
    serial_number          INT,
    interaction_id         INT,
    support_category       TEXT,
    agent_pseudo_name      TEXT,
    contact_date           TEXT,
    interaction_status     TEXT,
    interaction_type       TEXT,
    type_of_customer       TEXT,
    interaction_duration   INT,
    total_time             TEXT,
    status_of_customer_incident TEXT,
    resolved_in_first_contact TEXT,
    solution_type          TEXT,
    rating                 INT,
    hash_key               TEXT
);

CREATE TABLE IF NOT EXISTS lnd.client_beta_cs_data (
    support_identifier     INT,
    contact_regarding      TEXT,
    agent_code             TEXT,
    date_of_interaction    TEXT,
    status_of_interaction  TEXT,
    type_of_interaction    TEXT,
    customer_type          TEXT,
    contact_duration       TEXT,
    after_contact_work_time TEXT,
    incident_status        TEXT,
    first_contact_solve    TEXT,
    type_of_resolution     TEXT,
    support_rating         TEXT,
    time_stamp             TEXT,
    hash_key               TEXT
);

CREATE TABLE IF NOT EXISTS lnd.client_gamma_cs_data (
    ticket_identifier      INT,
    support_category       TEXT,
    agent_name             TEXT,
    date_of_call           TEXT,
    call_status            TEXT,
    call_type              TEXT,
    type_of_customer       TEXT,
    duration               TEXT,
    work_time              TEXT,
    ticket_status          TEXT,
    resolved_in_first_contact TEXT,
    resolution_category    TEXT,
    rating                 TEXT,
    hash_key               TEXT
);